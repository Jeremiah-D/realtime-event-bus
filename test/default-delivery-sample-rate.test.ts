import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/**
 * Bus-level default delivery-sample rate (EB-499):
 * `setDefaultDeliverySampleRate` installs the `deliverySampleRate`
 * divisor new subscribers fall back to when they pass no explicit
 * value — the priority chain is subscriber-explicit > bus default >
 * 1 — and, unlike the EB-491/493/498 defaults, there is no opt-in
 * form to fill in: the default applies to ALL new subscribers that
 * pass no explicit value, so one call is a fleet-wide
 * delivery-narrowing policy. The rate is frozen into the subscriber
 * at subscribe time; a later revision never moves it.
 *
 * Sampling assertions follow `delivery-sample.test.ts`: seqs are
 * per-topic from 1, `seq % N === 0` is admitted, the rest are
 * deliberate skips (never enqueued, no sequence gap).
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

test('EB-499: default 10 admits exactly 1 in 10 with skip-count and gap-baseline semantics unchanged', async () => {
  const bus = new EventBus();
  bus.setDefaultDeliverySampleRate(10);
  assert.equal(bus.getDefaultDeliverySampleRate(), 10);
  const received: number[] = [];
  bus.subscribe('t', (msg) => received.push(msg.seq));
  for (let i = 1; i <= 20; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [10, 20]);
  const topic = bus.getStats().topics.find((t) => t.topic === 't')!;
  assert.equal(topic.sampledDeliveries, 2);
  assert.equal(topic.sampleDropped, 18);
  assert.equal(bus.getStats().sampledDeliveries, 2);
  assert.equal(bus.getStats().sampleDropped, 18);
  // Deliberate skips are not losses: no sequence gaps.
  assert.equal(topic.sequenceGaps, 0);
  assert.equal(bus.getStats().sequenceGaps, 0);
});

test('EB-499: an explicit subscribe-time value always beats the bus default', async () => {
  const bus = new EventBus();
  bus.setDefaultDeliverySampleRate(10);
  const explicit: number[] = [];
  const fallback: number[] = [];
  bus.subscribe('a', (msg) => explicit.push(msg.seq), { deliverySampleRate: 2 });
  bus.subscribe('b', (msg) => fallback.push(msg.seq));
  for (let i = 1; i <= 10; i++) {
    bus.publish('a', i);
    bus.publish('b', i);
  }
  await flush();
  assert.deepEqual(explicit, [2, 4, 6, 8, 10], 'the explicit 2 beats the default 10');
  assert.deepEqual(fallback, [10], 'no explicit value takes the default 10');
  // An explicit 1 opts back out of sampling entirely, even with a default installed.
  const full: number[] = [];
  bus.subscribe('c', (msg) => full.push(msg.seq), { deliverySampleRate: 1 });
  for (let i = 1; i <= 3; i++) bus.publish('c', i);
  await flush();
  assert.deepEqual(full, [1, 2, 3]);
});

test('EB-499: clearing the default with null returns new subscribers to full delivery', async () => {
  const bus = new EventBus();
  bus.setDefaultDeliverySampleRate(10);
  bus.setDefaultDeliverySampleRate(null);
  assert.equal(bus.getDefaultDeliverySampleRate(), undefined);
  const received: number[] = [];
  bus.subscribe('t', (msg) => received.push(msg.seq));
  for (let i = 1; i <= 5; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [1, 2, 3, 4, 5]);
  assert.equal(bus.getStats().sampleDropped, 0);
  const audit = bus.getSubscriptionAudit({ action: 'default_delivery_sample_rate_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, { previous: 10, deliverySampleRate: null });
});

test('EB-499: an invalid value throws RangeError and leaves the live default untouched', () => {
  const bus = new EventBus();
  bus.setDefaultDeliverySampleRate(10);
  const bad = [0, -1, 1.5, Number.NaN, '10', true, {}, [], undefined] as unknown[];
  for (const value of bad) {
    assert.throws(
      () => bus.setDefaultDeliverySampleRate(value as never),
      RangeError,
      `rate=${String(value)}`,
    );
    assert.equal(bus.getDefaultDeliverySampleRate(), 10);
  }
  assert.throws(() => bus.setDefaultDeliverySampleRate(5, ''), RangeError);
  assert.equal(bus.getDefaultDeliverySampleRate(), 10);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_delivery_sample_rate_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});

test('EB-499: a same-value revision is a silent no-op with no audit', () => {
  const bus = new EventBus();
  // Clearing an already-unset default audits nothing.
  bus.setDefaultDeliverySampleRate(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_delivery_sample_rate_revised' }).length,
    0,
  );
  bus.setDefaultDeliverySampleRate(10);
  bus.setDefaultDeliverySampleRate(10);
  bus.setDefaultDeliverySampleRate(10);
  const audit = bus.getSubscriptionAudit({ action: 'default_delivery_sample_rate_revised' });
  assert.equal(audit.length, 1, 're-affirming the default audits nothing');
  assert.deepEqual(audit[0].detail, { previous: null, deliverySampleRate: 10 });
  // Clearing twice: only the first clear audits.
  bus.setDefaultDeliverySampleRate(null);
  bus.setDefaultDeliverySampleRate(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_delivery_sample_rate_revised' }).length,
    2,
  );
});

test('EB-499: existing subscribers are unaffected by a later default revision', async () => {
  const bus = new EventBus();
  const early: number[] = [];
  bus.subscribe('t', (msg) => early.push(msg.seq));
  for (let i = 1; i <= 5; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(early, [1, 2, 3, 4, 5]);
  bus.setDefaultDeliverySampleRate(10);
  for (let i = 6; i <= 15; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(
    early,
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
    'the early subscriber keeps the rate 1 it was baked with',
  );
  // A subscriber joining after the revision takes the new default.
  const late: number[] = [];
  bus.subscribe('t', (msg) => late.push(msg.seq));
  for (let i = 16; i <= 25; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(late, [20], 'the new default rate is 10');
  assert.deepEqual(early.slice(-10), [16, 17, 18, 19, 20, 21, 22, 23, 24, 25]);
});

test('EB-499: the default never enters a config snapshot', () => {
  const bus = new EventBus();
  bus.setDefaultDeliverySampleRate(10);
  const snapshot = bus.exportBusConfig();
  assert.ok(!('deliverySampleRate' in snapshot), 'no top-level deliverySampleRate key');
  assert.ok(
    !JSON.stringify(snapshot).includes('deliverySampleRate'),
    'the default appears nowhere in the exported snapshot',
  );
  assert.ok(
    !JSON.stringify(snapshot).includes('defaultDeliverySampleRate'),
    'the field name appears nowhere in the exported snapshot',
  );
});

test('EB-499: the getter returns the exact live default, undefined out of the box', () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultDeliverySampleRate(), undefined);
  bus.setDefaultDeliverySampleRate(10);
  assert.equal(bus.getDefaultDeliverySampleRate(), 10);
  bus.setDefaultDeliverySampleRate(3);
  assert.equal(bus.getDefaultDeliverySampleRate(), 3);
  bus.setDefaultDeliverySampleRate(null);
  assert.equal(bus.getDefaultDeliverySampleRate(), undefined);
});

test('EB-499: audit previous/current record the exact numbers, with the actor', () => {
  const bus = new EventBus();
  bus.setDefaultDeliverySampleRate(10, 'ops');
  bus.setDefaultDeliverySampleRate(5, 'ops');
  bus.setDefaultDeliverySampleRate(null, 'ops');
  const audit = bus.getSubscriptionAudit({ action: 'default_delivery_sample_rate_revised' });
  assert.equal(audit.length, 3);
  assert.deepEqual(audit[0].detail, { previous: null, deliverySampleRate: 10 });
  assert.equal(audit[0].actor, 'ops');
  assert.deepEqual(audit[1].detail, { previous: 10, deliverySampleRate: 5 });
  assert.deepEqual(audit[2].detail, { previous: 5, deliverySampleRate: null });
});
