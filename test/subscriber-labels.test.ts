/**
 * Subscriber labels (EB-213): operator-defined `key=value` tags on
 * subscriptions — `SubscribeOptions.labels` at registration,
 * `setSubscriberLabels` / `getSubscriberLabels` hot updates, per-label
 * aggregation in `getStats().subscriberLabels`, Prometheus exposition,
 * subscription-audit coverage, and namespace-handle inheritance.
 *
 * Run: `node --test test/subscriber-labels.test.ts` (Node >= 22, no build step).
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage } from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const noop = () => {};

test('subscribe stores labels and getSubscriberLabels returns a detached copy', () => {
  const bus = new EventBus();
  const input = { team: 'search', tier: 'gold' };
  const sub = bus.subscribe('t.*', noop, { labels: input });
  assert.deepEqual(bus.getSubscriberLabels(sub.id), { team: 'search', tier: 'gold' });
  // The subscription owns its copy: mutating the caller's object or the
  // returned copy never affects the stored labels.
  input.team = 'infra';
  const copy = bus.getSubscriberLabels(sub.id);
  assert.deepEqual(copy, { team: 'search', tier: 'gold' });
  assert.ok(copy !== undefined);
  copy.tier = 'silver';
  assert.deepEqual(bus.getSubscriberLabels(sub.id), { team: 'search', tier: 'gold' });
  sub.unsubscribe();
  bus.close();
});

test('subscribe without labels yields an empty label set', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', noop);
  assert.deepEqual(bus.getSubscriberLabels(sub.id), {});
  sub.unsubscribe();
  bus.close();
});

test('subscribe rejects malformed labels before anything registers', () => {
  const bus = new EventBus();
  // Bad keys.
  for (const labels of [
    { '': 'x' },
    { 'has space': 'x' },
    { ['a'.repeat(65)]: 'x' },
    { 'dot.key': 'x' },
  ]) {
    assert.throws(() => bus.subscribe('t', noop, { labels }), RangeError);
  }
  // Bad values.
  for (const labels of [{ k: '' }, { k: 'v'.repeat(129) }, { k: 42 as unknown as string }]) {
    assert.throws(() => bus.subscribe('t', noop, { labels }), RangeError);
  }
  // Not an object.
  for (const labels of [['k'], 'k=v', 42] as unknown as Array<Record<string, string>>) {
    assert.throws(() => bus.subscribe('t', noop, { labels }), TypeError);
  }
  // A rejected subscribe registers nothing.
  assert.equal(bus.getStats().totalSubscribers, 0);
  bus.close();
});

test('setSubscriberLabels replaces labels and reports unknown ids', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', noop, { labels: { team: 'search' } });
  assert.equal(bus.setSubscriberLabels(sub.id, { team: 'infra', tier: 'gold' }), true);
  assert.deepEqual(bus.getSubscriberLabels(sub.id), { team: 'infra', tier: 'gold' });
  // Clearing.
  assert.equal(bus.setSubscriberLabels(sub.id, undefined), true);
  assert.deepEqual(bus.getSubscriberLabels(sub.id), {});
  // Unknown id: not an error, just nothing to update.
  assert.equal(bus.setSubscriberLabels('sub-9999', { a: 'b' }), false);
  assert.equal(bus.getSubscriberLabels('sub-9999'), undefined);
  // Validation runs before the lookup: malformed labels throw even for an
  // unknown id, and a failed update leaves the subscription untouched.
  assert.throws(() => bus.setSubscriberLabels(sub.id, { 'bad key': 'x' }), RangeError);
  assert.throws(() => bus.setSubscriberLabels('sub-9999', { 'bad key': 'x' }), RangeError);
  assert.deepEqual(bus.getSubscriberLabels(sub.id), {});
  sub.unsubscribe();
  bus.close();
});

test('label changes are recorded in the subscription audit log', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', noop, { labels: { team: 'search' } });
  const subscribed = bus.getSubscriptionAudit({ action: 'subscribe' });
  assert.equal(subscribed.length, 1);
  assert.deepEqual(subscribed[0].detail?.labels, { team: 'search' });
  bus.setSubscriberLabels(sub.id, { team: 'infra' });
  const updated = bus.getSubscriptionAudit({ action: 'subscriber_labels_updated' });
  assert.equal(updated.length, 1);
  assert.deepEqual(updated[0].detail?.previous, { team: 'search' });
  assert.deepEqual(updated[0].detail?.labels, { team: 'infra' });
  assert.equal(updated[0].subId, sub.id);
  // A label-less subscribe carries no labels detail.
  const sub2 = bus.subscribe('u', noop);
  const all = bus.getSubscriptionAudit({ action: 'subscribe' });
  assert.equal(all.length, 2);
  assert.equal(all[1].detail, undefined);
  sub.unsubscribe();
  sub2.unsubscribe();
  bus.close();
});

test('getStats aggregates live subscriptions per label pair', async () => {
  const bus = new EventBus();
  const a = bus.subscribe('orders.*', noop, { labels: { team: 'search', tier: 'gold' } });
  const b = bus.subscribe('orders.eu', noop, { labels: { team: 'search' } });
  const c = bus.subscribe('orders.*', noop); // unlabeled: invisible to the aggregation
  bus.publish('orders.eu', { n: 1 });
  bus.publish('orders.us', { n: 2 });
  await flush();
  const rows = bus.getStats().subscriberLabels ?? [];
  // Sorted by (key, value): team=search, tier=gold.
  assert.deepEqual(rows, [
    { key: 'team', value: 'search', subscribers: 2, deliveredMessages: 3 },
    { key: 'tier', value: 'gold', subscribers: 1, deliveredMessages: 2 },
  ]);
  // Unsubscribing drops the subscription from the aggregation.
  b.unsubscribe();
  const rows2 = bus.getStats().subscriberLabels ?? [];
  assert.deepEqual(rows2, [
    { key: 'team', value: 'search', subscribers: 1, deliveredMessages: 2 },
    { key: 'tier', value: 'gold', subscribers: 1, deliveredMessages: 2 },
  ]);
  // Relabeling moves the subscription between rows.
  bus.setSubscriberLabels(a.id, { team: 'infra' });
  const rows3 = bus.getStats().subscriberLabels ?? [];
  assert.deepEqual(rows3, [
    { key: 'team', value: 'infra', subscribers: 1, deliveredMessages: 2 },
  ]);
  a.unsubscribe();
  c.unsubscribe();
  // No labeled subscriptions left: the aggregation is empty.
  assert.deepEqual(bus.getStats().subscriberLabels ?? [], []);
  bus.close();
});

test('renderPrometheus carries the label dimension only for labeled subscriptions', async () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', noop, { labels: { team: 'search' } });
  bus.publish('t', { n: 1 });
  await flush();
  const text = renderPrometheus(bus.getStats());
  assert.ok(text.includes('eventbus_subscriber_label_info{label_key="team",label_value="search"} 1'));
  assert.ok(
    text.includes('eventbus_subscriber_label_delivered_total{label_key="team",label_value="search"} 1'),
  );
  sub.unsubscribe();
  const text2 = renderPrometheus(bus.getStats());
  assert.ok(!text2.includes('eventbus_subscriber_label_info{'));
  assert.ok(!text2.includes('eventbus_subscriber_label_delivered_total{'));
  bus.close();
});

test('labels are metadata: they never affect matching or delivery', async () => {
  const bus = new EventBus();
  const seen: BusMessage[] = [];
  const sub = bus.subscribe('t.*', (msg) => void seen.push(msg), { labels: { team: 'search' } });
  bus.publish('t.a', { n: 1 });
  bus.publish('other', { n: 2 });
  await flush();
  assert.equal(seen.length, 1);
  assert.equal(seen[0].topic, 't.a');
  sub.unsubscribe();
  bus.close();
});

test('namespace scoped handles inherit labels', async () => {
  const bus = new EventBus();
  const ns = bus.createNamespace('t1');
  const sub = ns.subscribe('orders.*', noop, { labels: { tenant: 'acme' } });
  assert.deepEqual(bus.getSubscriberLabels(sub.id), { tenant: 'acme' });
  ns.publish('orders.eu', { n: 1 });
  await flush();
  const rows = bus.getStats().subscriberLabels ?? [];
  assert.deepEqual(rows, [
    { key: 'tenant', value: 'acme', subscribers: 1, deliveredMessages: 1 },
  ]);
  sub.unsubscribe();
  bus.deleteNamespace('t1');
  bus.close();
});

test('label values with special characters are escaped in exposition', async () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', noop, { labels: { team: 'a"b\\c' } });
  await flush();
  const text = renderPrometheus(bus.getStats());
  assert.ok(text.includes('label_value="a\\"b\\\\c"'));
  sub.unsubscribe();
  bus.close();
});
