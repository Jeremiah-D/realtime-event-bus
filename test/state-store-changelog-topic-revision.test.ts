/**
 * Materialized state-store changelog-topic revision (EB-463).
 *
 * `setStateStoreChangelogTopic(topic, actor?)` retargets where the next
 * state update's changelog record is fanned out — without rebuilding
 * the bus. Only the destination moves: `observeAdmittedPublish` reads
 * the live topic on every state update, so the new topic governs from
 * the next update. A store with no changelog topic can opt in live;
 * passing `null` clears the topic so updates still land in the table
 * but no changelog record is fanned out. Records already fanned out
 * stay on the old topic; the store rows, `version`, `seq`, and
 * `maxKeys` are untouched. A bus whose state store is not enabled
 * refuses fail-closed; illegal values fail closed with the live topic
 * untouched; an identical revision is a no-op with no audit.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage } from '../src/bus.ts';

/** Yields until the bus's async flush has delivered. */
const flush = () => new Promise<void>((resolve) => setTimeout(resolve, 20));

const audit = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'state_store_changelog_topic_revised' });

test('a store without a changelog topic can opt in live; the next update fans out to the new topic', async () => {
  const bus = new EventBus();
  const seen: BusMessage[] = [];
  bus.subscribe('changelog-new', (m) => seen.push(m));
  bus.enableStateStore();
  assert.equal(bus.getStateStoreChangelogTopic(), undefined);

  // Before the opt-in, updates land in the table but fan out nowhere.
  bus.publish('orders', { status: 'new' }, { key: 'o1' });
  await flush();
  assert.equal(seen.length, 0);
  assert.equal(bus.getState('orders', 'o1')!.version, 1);

  bus.setStateStoreChangelogTopic('changelog-new', 'ops');
  assert.equal(bus.getStateStoreChangelogTopic(), 'changelog-new');
  bus.publish('orders', { status: 'paid' }, { key: 'o1' });
  await flush();
  assert.equal(seen.length, 1);
  const record = seen[0].payload as Record<string, unknown>;
  assert.equal(record.topic, 'orders');
  assert.equal(record.key, 'o1');
  assert.deepEqual(record.value, { status: 'paid' });
  assert.deepEqual(record.oldValue, { status: 'new' });
  assert.equal(record.version, 2);
  assert.equal(audit(bus).length, 1);
  assert.deepEqual(audit(bus)[0].detail, {
    changelogTopic: 'changelog-new',
    oldChangelogTopic: null,
  });
  assert.equal(audit(bus)[0].actor, 'ops');
});

test('redirecting fans the next update only to the new topic; the old topic gains nothing', async () => {
  const bus = new EventBus();
  const oldSeen: BusMessage[] = [];
  const newSeen: BusMessage[] = [];
  bus.subscribe('changelog-old', (m) => oldSeen.push(m));
  bus.subscribe('changelog-new', (m) => newSeen.push(m));
  bus.enableStateStore({ changelogTopic: 'changelog-old' });
  assert.equal(bus.getStateStoreChangelogTopic(), 'changelog-old');

  bus.publish('orders', { status: 'new' }, { key: 'o1' });
  await flush();
  assert.equal(oldSeen.length, 1);
  assert.equal(newSeen.length, 0);

  bus.setStateStoreChangelogTopic('changelog-new');
  assert.equal(bus.getStateStoreChangelogTopic(), 'changelog-new');
  bus.publish('orders', { status: 'paid' }, { key: 'o1' });
  await flush();
  assert.equal(oldSeen.length, 1);
  assert.equal(newSeen.length, 1);
  const record = newSeen[0].payload as Record<string, unknown>;
  assert.equal(record.key, 'o1');
  assert.equal(record.version, 2);
  assert.deepEqual(audit(bus)[0].detail, {
    changelogTopic: 'changelog-new',
    oldChangelogTopic: 'changelog-old',
  });
});

test('records already fanned out stay on the old topic with their original content', async () => {
  const bus = new EventBus();
  const oldSeen: BusMessage[] = [];
  const newSeen: BusMessage[] = [];
  bus.subscribe('changelog-old', (m) => oldSeen.push(m));
  bus.subscribe('changelog-new', (m) => newSeen.push(m));
  bus.enableStateStore({ changelogTopic: 'changelog-old' });
  bus.publish('orders', { status: 'new' }, { key: 'o1' });
  bus.publish('orders', { status: 'shipped' }, { key: 'o2' });
  await flush();
  assert.equal(oldSeen.length, 2);

  bus.setStateStoreChangelogTopic('changelog-new');
  bus.publish('orders', { status: 'paid' }, { key: 'o1' });
  await flush();
  // The two pre-redirect records are still only on the old topic, with
  // the payloads they were fanned out with — never re-emitted.
  assert.equal(oldSeen.length, 2);
  assert.deepEqual(
    oldSeen.map((m) => (m.payload as Record<string, unknown>).value),
    [{ status: 'new' }, { status: 'shipped' }],
  );
  assert.equal(newSeen.length, 1);
  assert.deepEqual(
    (newSeen[0].payload as Record<string, unknown>).value,
    { status: 'paid' },
  );
});

test('clearing with null keeps updates landing in the table but stops changelog fan-out', async () => {
  const bus = new EventBus();
  const seen: BusMessage[] = [];
  bus.subscribe('changelog', (m) => seen.push(m));
  bus.enableStateStore({ changelogTopic: 'changelog' });
  bus.publish('orders', { status: 'new' }, { key: 'o1' });
  await flush();
  assert.equal(seen.length, 1);

  bus.setStateStoreChangelogTopic(null, 'ops');
  assert.equal(bus.getStateStoreChangelogTopic(), undefined);
  bus.publish('orders', { status: 'paid' }, { key: 'o1' });
  await flush();
  // The update still landed; only the fan-out stopped.
  assert.equal(seen.length, 1);
  assert.equal(bus.getState('orders', 'o1')!.version, 2);
  assert.deepEqual(bus.getState('orders', 'o1')!.value, { status: 'paid' });
  assert.deepEqual(audit(bus)[0].detail, {
    changelogTopic: null,
    oldChangelogTopic: 'changelog',
  });
  assert.equal(audit(bus)[0].actor, 'ops');
});

test('setStateStoreChangelogTopic requires an enabled store and a valid actor', () => {
  const bus = new EventBus();
  assert.throws(
    () => bus.setStateStoreChangelogTopic('changelog'),
    /state store is not enabled/,
  );
  assert.throws(() => bus.setStateStoreChangelogTopic('changelog', ''), /actor must be a non-empty string/);
  assert.throws(() => bus.setStateStoreChangelogTopic('changelog', 42 as never), RangeError);
  assert.equal(audit(bus).length, 0);
  assert.equal(bus.getStateStoreChangelogTopic(), undefined);

  bus.enableStateStore({ changelogTopic: 'changelog' });
  assert.throws(() => bus.setStateStoreChangelogTopic('other', '' as never), RangeError);
  assert.equal(audit(bus).length, 0);
  assert.equal(bus.getStateStoreChangelogTopic(), 'changelog');
});

test('revision rejects illegal values and leaves the live topic and audit untouched', () => {
  const bus = new EventBus();
  bus.enableStateStore({ changelogTopic: 'changelog' });
  for (const bad of ['', 42, true, {}, []]) {
    assert.throws(() => bus.setStateStoreChangelogTopic(bad as never), RangeError);
  }
  assert.equal(audit(bus).length, 0);
  assert.equal(bus.getStateStoreChangelogTopic(), 'changelog');
  // A valid revision afterwards still works on the untouched topic.
  bus.setStateStoreChangelogTopic('changelog-new', 'ops');
  assert.equal(audit(bus).length, 1);
  assert.deepEqual(audit(bus)[0].detail, {
    changelogTopic: 'changelog-new',
    oldChangelogTopic: 'changelog',
  });
  assert.equal(bus.getStateStoreChangelogTopic(), 'changelog-new');
});

test('an identical revision is a no-op with no audit', () => {
  const bus = new EventBus();
  bus.enableStateStore({ changelogTopic: 'changelog' });
  bus.setStateStoreChangelogTopic('changelog');
  assert.equal(audit(bus).length, 0);

  // Clearing a store that has no topic is a no-op too.
  const topicless = new EventBus();
  topicless.enableStateStore();
  topicless.setStateStoreChangelogTopic(null);
  assert.equal(audit(topicless).length, 0);
  assert.equal(topicless.getStateStoreChangelogTopic(), undefined);
});

test('redirecting leaves maxKeys, rows, and versions untouched', async () => {
  const bus = new EventBus();
  const seen: BusMessage[] = [];
  bus.subscribe('changelog-new', (m) => seen.push(m));
  bus.enableStateStore({ maxKeys: 2, changelogTopic: 'changelog-old' });
  bus.publish('t', 'v1', { key: 'k1' });
  bus.publish('t', 'v2', { key: 'k2' });
  bus.setStateStoreChangelogTopic('changelog-new');
  bus.publish('t', 'v1b', { key: 'k1' });
  await flush();
  assert.deepEqual(bus.getStats().stateStore, { keys: 2, evictedKeys: 0, maxKeys: 2 });
  assert.equal(bus.getState('t', 'k1')!.version, 2);
  assert.equal(bus.getState('t', 'k1')!.seq, 3);
  assert.equal(bus.getState('t', 'k2')!.version, 1);
  assert.equal(bus.getState('t', 'k2')!.seq, 2);
  // The post-redirect update fanned out exactly once, to the new topic.
  assert.equal(seen.length, 1);
  assert.equal((seen[0].payload as Record<string, unknown>).key, 'k1');
  // The redirect itself audited once and nothing else.
  assert.equal(audit(bus).length, 1);
});

test('the getter follows the live topic through opt-in, redirect, and clear', () => {
  const bus = new EventBus();
  bus.enableStateStore();
  assert.equal(bus.getStateStoreChangelogTopic(), undefined);
  bus.setStateStoreChangelogTopic('a');
  assert.equal(bus.getStateStoreChangelogTopic(), 'a');
  bus.setStateStoreChangelogTopic('b');
  assert.equal(bus.getStateStoreChangelogTopic(), 'b');
  bus.setStateStoreChangelogTopic(null);
  assert.equal(bus.getStateStoreChangelogTopic(), undefined);
  // No getStats row carries the topic.
  assert.equal('changelogTopic' in (bus.getStats().stateStore ?? {}), false);
});
