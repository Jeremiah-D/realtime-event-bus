import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** Reads the one delivery-concurrency stats entry for a pattern. */
function concurrencyEntry(bus: EventBus, pattern: string) {
  const entries = bus.getStats().deliveryConcurrency.filter((e) => e.pattern === pattern);
  assert.equal(entries.length, 1, `expected one deliveryConcurrency entry for ${pattern}`);
  return entries[0];
}

/** Audit entries for the EB-277 revision action. */
function revisionAudit(bus: EventBus) {
  return bus.getSubscriptionAudit({ action: 'delivery_concurrency_revised' });
}

test('setDeliveryConcurrency happy path: revises the live max, including a never-explicitly-set cap', async () => {
  const bus = new EventBus();
  // No explicit maxConcurrentDeliveries: always armed at the default 1,
  // and — unlike the EB-263–EB-276 precedents — revisable without any
  // prior opt-in.
  const sub = bus.subscribe('t', () => {});
  assert.equal(concurrencyEntry(bus, 't').maxConcurrentDeliveries, 1);

  bus.setDeliveryConcurrency(sub.id, 3);
  assert.equal(concurrencyEntry(bus, 't').maxConcurrentDeliveries, 3);

  const audit = revisionAudit(bus);
  assert.equal(audit.length, 1);
  assert.equal(audit[0].subId, sub.id);
  assert.deepEqual(audit[0].detail, { deliveryConcurrency: 3, oldDeliveryConcurrency: 1 });
  sub.unsubscribe();
});

test('setDeliveryConcurrency lowering the limit does not revoke in-flight calls', async () => {
  const bus = new EventBus();
  const events: string[] = [];
  let releaseFirst!: () => void;
  let releaseSecond!: () => void;
  const firstGate = new Promise<void>((resolve) => {
    releaseFirst = resolve;
  });
  const secondGate = new Promise<void>((resolve) => {
    releaseSecond = resolve;
  });
  bus.subscribe(
    't',
    async (msg) => {
      const n = (msg.payload as { n: number }).n;
      events.push(`start-${n}`);
      if (n === 1) await firstGate;
      if (n === 2) await secondGate;
      events.push(`end-${n}`);
    },
    { maxConcurrentDeliveries: 2 },
  );
  for (let n = 1; n <= 3; n += 1) bus.publish('t', { n });
  await flush();
  assert.deepEqual(events, ['start-1', 'start-2']);
  assert.equal(concurrencyEntry(bus, 't').inFlight, 2);

  // Lower below the live in-flight count: both calls keep running.
  const subId = concurrencyEntry(bus, 't').subscriberId;
  bus.setDeliveryConcurrency(subId, 1);
  const entry = concurrencyEntry(bus, 't');
  assert.equal(entry.maxConcurrentDeliveries, 1);
  assert.equal(entry.inFlight, 2, 'lowering must not revoke in-flight calls');
  assert.deepEqual(entry.keysInFlight, {});
  await flush();
  assert.deepEqual(events, ['start-1', 'start-2'], 'no new call admitted while over the lowered cap');

  releaseFirst();
  releaseSecond();
  await flush();
  await flush();
  assert.deepEqual(events, ['start-1', 'start-2', 'end-1', 'end-2', 'start-3', 'end-3']);
  assert.equal(concurrencyEntry(bus, 't').inFlight, 0);
});

test('setDeliveryConcurrency raising the limit releases queued callers via the live max', async () => {
  const bus = new EventBus();
  const events: string[] = [];
  let releaseFirst!: () => void;
  const firstGate = new Promise<void>((resolve) => {
    releaseFirst = resolve;
  });
  const sub = bus.subscribe('t', async (msg) => {
    const n = (msg.payload as { n: number }).n;
    events.push(`start-${n}`);
    if (n === 1) await firstGate;
    events.push(`end-${n}`);
  });
  for (let n = 1; n <= 3; n += 1) bus.publish('t', { n });
  await flush();
  assert.deepEqual(events, ['start-1']);

  // Raising schedules a flush, and tryAcquireDeliverySlot reads the
  // live max, so the queued callers are admitted without any new publish.
  bus.setDeliveryConcurrency(sub.id, 3);
  await flush();
  await flush();
  assert.deepEqual(events, ['start-1', 'start-2', 'end-2', 'start-3', 'end-3']);
  assert.equal(concurrencyEntry(bus, 't').inFlight, 1, 'only the gated first call is still in flight');

  releaseFirst();
  await flush();
  assert.deepEqual(events, ['start-1', 'start-2', 'end-2', 'start-3', 'end-3', 'end-1']);
  sub.unsubscribe();
});

test('setDeliveryConcurrency keeps same-key ordering after a raise', async () => {
  const bus = new EventBus();
  const order: string[] = [];
  const activeByKey = new Map<string, number>();
  let keyOverlap = false;
  const sub = bus.subscribe(
    't',
    async (msg) => {
      const key = msg.key ?? '<none>';
      activeByKey.set(key, (activeByKey.get(key) ?? 0) + 1);
      if ((activeByKey.get(key) ?? 0) > 1) keyOverlap = true;
      order.push(`start:${key}`);
      await sleep(15);
      activeByKey.set(key, (activeByKey.get(key) ?? 0) - 1);
      order.push(`end:${key}`);
    },
    { maxConcurrentDeliveries: 1 },
  );
  bus.publish('t', 'a', { key: 'k1' });
  bus.publish('t', 'b', { key: 'k1' });
  bus.publish('t', 'c', { key: 'k2' });
  await flush();
  assert.deepEqual(order, ['start:k1']);

  // Raise to 3: k2 may now overlap k1, but b (same key as a) still
  // waits for a to finish — keysInFlight is untouched by the revision.
  bus.setDeliveryConcurrency(sub.id, 3);
  await sleep(100);
  assert.equal(keyOverlap, false);
  assert.ok(order.indexOf('start:k2') >= 0, 'a different key is admitted under the raised cap');
  const firstK1Start = order.indexOf('start:k1');
  const firstK1End = order.indexOf('end:k1');
  const secondK1Start = order.indexOf('start:k1', firstK1Start + 1);
  assert.ok(
    firstK1End >= 0 && secondK1Start >= 0 && firstK1End < secondK1Start,
    `same-key order preserved: ${JSON.stringify(order)}`,
  );
  const k1Starts = order.filter((e) => e === 'start:k1').length;
  assert.equal(k1Starts, 2, 'both k1 messages were delivered exactly once');
  assert.equal(order.filter((e) => e.startsWith('start:')).length, 3);
  sub.unsubscribe();
});

test('setDeliveryConcurrency unknown subscription / undefined fail closed with the cap unchanged', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {}, { maxConcurrentDeliveries: 2 });
  assert.throws(() => bus.setDeliveryConcurrency('no-such-sub', 3), RangeError);
  assert.throws(() => bus.setDeliveryConcurrency('', 3), RangeError);
  assert.throws(
    () => bus.setDeliveryConcurrency(sub.id, undefined as never),
    RangeError,
  );
  assert.equal(concurrencyEntry(bus, 't').maxConcurrentDeliveries, 2);
  assert.equal(revisionAudit(bus).length, 0, 'failed revisions leave the audit log untouched');
  sub.unsubscribe();
});

test('setDeliveryConcurrency invalid max fails with the original cap unchanged', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {}, { maxConcurrentDeliveries: 2 });
  for (const bad of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, '2' as unknown as number]) {
    assert.throws(() => bus.setDeliveryConcurrency(sub.id, bad), RangeError, `max=${String(bad)}`);
  }
  assert.equal(concurrencyEntry(bus, 't').maxConcurrentDeliveries, 2);
  assert.equal(revisionAudit(bus).length, 0);
  sub.unsubscribe();
});

test('setDeliveryConcurrency null restores the parse-chain default (current maxInflight, otherwise 1)', () => {
  const bus = new EventBus();
  // No maxInflight: the default is 1.
  const plain = bus.subscribe('plain', () => {});
  bus.setDeliveryConcurrency(plain.id, 3);
  assert.equal(concurrencyEntry(bus, 'plain').maxConcurrentDeliveries, 3);
  bus.setDeliveryConcurrency(plain.id, null);
  assert.equal(concurrencyEntry(bus, 'plain').maxConcurrentDeliveries, 1);
  plain.unsubscribe();

  // maxInflight doubles as the subscribe-time default, and null
  // restores the *current* maxInflight, not the subscribe-time cap.
  const capped = bus.subscribe('capped', () => {}, { maxInflight: 3 });
  assert.equal(concurrencyEntry(bus, 'capped').maxConcurrentDeliveries, 3);
  bus.setDeliveryConcurrency(capped.id, 2);
  assert.equal(concurrencyEntry(bus, 'capped').maxConcurrentDeliveries, 2);
  bus.setMaxInflight(capped.id, 5);
  bus.setDeliveryConcurrency(capped.id, null);
  assert.equal(concurrencyEntry(bus, 'capped').maxConcurrentDeliveries, 5);
  capped.unsubscribe();

  const audit = revisionAudit(bus);
  assert.deepEqual(
    audit.map((e) => e.detail),
    [
      { deliveryConcurrency: 3, oldDeliveryConcurrency: 1 },
      { deliveryConcurrency: 1, oldDeliveryConcurrency: 3 },
      { deliveryConcurrency: 2, oldDeliveryConcurrency: 3 },
      { deliveryConcurrency: 5, oldDeliveryConcurrency: 2 },
    ],
  );
});

test('setDeliveryConcurrency same-value revision is a no-op with no audit entry', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {}, { maxConcurrentDeliveries: 2 });
  bus.setDeliveryConcurrency(sub.id, 2);
  assert.equal(revisionAudit(bus).length, 0);
  assert.equal(concurrencyEntry(bus, 't').maxConcurrentDeliveries, 2);

  // Null restoring the value already in force is the same no-op.
  const plain = bus.subscribe('plain', () => {});
  bus.setDeliveryConcurrency(plain.id, null);
  assert.equal(revisionAudit(bus).length, 0);
  assert.equal(concurrencyEntry(bus, 'plain').maxConcurrentDeliveries, 1);
  sub.unsubscribe();
  plain.unsubscribe();
});

test('setDeliveryConcurrency audit entry is recorded on a real revision', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {}, { maxConcurrentDeliveries: 1 });
  bus.setDeliveryConcurrency(sub.id, 4);
  bus.setDeliveryConcurrency(sub.id, 2);
  const audit = revisionAudit(bus);
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[0].detail, { deliveryConcurrency: 4, oldDeliveryConcurrency: 1 });
  assert.deepEqual(audit[1].detail, { deliveryConcurrency: 2, oldDeliveryConcurrency: 4 });
  assert.ok(audit.every((e) => e.action === 'delivery_concurrency_revised' && e.subId === sub.id));
  sub.unsubscribe();
});
