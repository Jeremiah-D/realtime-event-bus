import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/**
 * Bus-level terminal handler-concurrency default (EB-482):
 * `setDefaultMaxConcurrentDeliveries` installs the
 * `maxConcurrentDeliveries` new subscribers fall back to when they omit
 * their own (EB-159) — the priority chain is subscriber-explicit > bus
 * default > maxInflight (explicit or default-derived) > 1 — and
 * `setDeliveryConcurrency(subId, null)` now restores to the bus default
 * in effect at revision time.
 *
 * Overlap tests follow `delivery-concurrency.test.ts`: async handlers
 * sleep, and the peak number of simultaneously executing handlers is
 * counted directly.
 */

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** The per-subscriber concurrency cap a subscriber was baked with. */
function concurrencyMax(bus: EventBus, subId: string): number {
  const row = bus.getStats().deliveryConcurrency.find((r) => r.subscriberId === subId);
  assert.ok(row, `subscriber ${subId} must have a deliveryConcurrency row`);
  return row.maxConcurrentDeliveries;
}

test('EB-482: the factory bus default is unset (omitting maxConcurrentDeliveries follows the EB-159 chain)', () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultMaxConcurrentDeliveries(), undefined);
  const plain = bus.subscribe('t', () => {});
  assert.equal(concurrencyMax(bus, plain.id), 1, 'no default, no maxInflight: serial');
  const windowed = bus.subscribe('t', () => {}, { maxInflight: 3 });
  assert.equal(concurrencyMax(bus, windowed.id), 3, 'no default: the maxInflight opt-in still doubles as the concurrency default');
});

test('EB-482: a new subscriber takes the bus default, and two handlers may overlap', async () => {
  const bus = new EventBus();
  bus.setDefaultMaxConcurrentDeliveries(2);
  let current = 0;
  let maxSeen = 0;
  const sub = bus.subscribe('t', async () => {
    current += 1;
    maxSeen = Math.max(maxSeen, current);
    await sleep(20);
    current -= 1;
  });
  assert.equal(concurrencyMax(bus, sub.id), 2);
  for (let n = 1; n <= 4; n += 1) bus.publish('t', { n });
  await sleep(150);
  assert.equal(maxSeen, 2, 'the default cap admits two overlapping handlers');
  assert.equal(bus.getStats().deliveryConcurrency[0].inFlight, 0);
});

test('EB-482: an explicit maxConcurrentDeliveries beats the bus default in both directions', async () => {
  // Explicit 1 beats a bus default of 4: handlers stay serial.
  const strict = new EventBus();
  strict.setDefaultMaxConcurrentDeliveries(4);
  let current = 0;
  let maxSeen = 0;
  const strictSub = strict.subscribe(
    't',
    async () => {
      current += 1;
      maxSeen = Math.max(maxSeen, current);
      await sleep(15);
      current -= 1;
    },
    { maxConcurrentDeliveries: 1 },
  );
  assert.equal(concurrencyMax(strict, strictSub.id), 1);
  for (let n = 1; n <= 3; n += 1) strict.publish('t', { n });
  await sleep(150);
  assert.equal(maxSeen, 1, 'the explicit cap of 1 wins over the bus default of 4');

  // Explicit 4 beats a bus default of 1: four handlers overlap.
  const loose = new EventBus();
  loose.setDefaultMaxConcurrentDeliveries(1);
  let current2 = 0;
  let maxSeen2 = 0;
  const looseSub = loose.subscribe(
    't',
    async () => {
      current2 += 1;
      maxSeen2 = Math.max(maxSeen2, current2);
      await sleep(20);
      current2 -= 1;
    },
    { maxConcurrentDeliveries: 4 },
  );
  assert.equal(concurrencyMax(loose, looseSub.id), 4);
  for (let n = 1; n <= 4; n += 1) loose.publish('t', { n });
  await sleep(150);
  assert.equal(maxSeen2, 4, 'the explicit cap of 4 wins over the bus default of 1');
});

test('EB-482: the bus default beats the maxInflight-derived value', () => {
  const bus = new EventBus();
  bus.setDefaultMaxConcurrentDeliveries(4);
  // An explicit maxInflight of 8 would have doubled as the concurrency
  // default under EB-159 alone; the more specific bus default wins.
  const sub = bus.subscribe('t', () => {}, { maxInflight: 8 });
  assert.equal(concurrencyMax(bus, sub.id), 4);
});

test('EB-482: with no bus default the EB-159 maxInflight rule still stands', async () => {
  const bus = new EventBus();
  let current = 0;
  let maxSeen = 0;
  const sub = bus.subscribe(
    't',
    async () => {
      current += 1;
      maxSeen = Math.max(maxSeen, current);
      await sleep(15);
      current -= 1;
    },
    { maxInflight: 3 },
  );
  assert.equal(concurrencyMax(bus, sub.id), 3);
  for (let n = 1; n <= 3; n += 1) bus.publish('t', { n });
  await sleep(100);
  assert.equal(maxSeen, 3);
});

test('EB-482: clearing with null returns new subscribers to the parse chain', () => {
  const bus = new EventBus();
  bus.setDefaultMaxConcurrentDeliveries(2);
  bus.setDefaultMaxConcurrentDeliveries(null);
  assert.equal(bus.getDefaultMaxConcurrentDeliveries(), undefined);
  const plain = bus.subscribe('t', () => {});
  assert.equal(concurrencyMax(bus, plain.id), 1, 'cleared: plain subscriber is serial again');
  const windowed = bus.subscribe('t', () => {}, { maxInflight: 5 });
  assert.equal(concurrencyMax(bus, windowed.id), 5, 'cleared: maxInflight fallback is back');
});

test('EB-482: revising the bus default never moves an already-subscribed cap', () => {
  const bus = new EventBus();
  bus.setDefaultMaxConcurrentDeliveries(2);
  const early = bus.subscribe('t', () => {});
  assert.equal(concurrencyMax(bus, early.id), 2);
  bus.setDefaultMaxConcurrentDeliveries(4);
  assert.equal(
    concurrencyMax(bus, early.id),
    2,
    'the revision never moves an already-subscribed cap',
  );
  const late = bus.subscribe('u', () => {});
  assert.equal(concurrencyMax(bus, late.id), 4, 'a subscriber joining after the revision takes the new default');
});

test('EB-482: setDeliveryConcurrency(null) restores to the bus default in effect at revision time', () => {
  const bus = new EventBus();
  // A subscriber whose cap came from maxInflight (EB-159 chain).
  const windowed = bus.subscribe('t', () => {}, { maxInflight: 3 });
  assert.equal(concurrencyMax(bus, windowed.id), 3);
  bus.setDefaultMaxConcurrentDeliveries(2);
  bus.setDeliveryConcurrency(windowed.id, null);
  assert.equal(
    concurrencyMax(bus, windowed.id),
    2,
    'null restores to the live bus default, not to the maxInflight value',
  );
  // With no default installed, null restores to the old parse chain.
  bus.setDefaultMaxConcurrentDeliveries(null);
  bus.setDeliveryConcurrency(windowed.id, null);
  assert.equal(concurrencyMax(bus, windowed.id), 3, 'null with no default falls back to maxInflight');

  // A plain subscriber (cap 1) restores to 1 when no default is set.
  const plain = bus.subscribe('u', () => {});
  assert.equal(concurrencyMax(bus, plain.id), 1);
  bus.setDeliveryConcurrency(plain.id, 5);
  assert.equal(concurrencyMax(bus, plain.id), 5);
  bus.setDeliveryConcurrency(plain.id, null);
  assert.equal(concurrencyMax(bus, plain.id), 1, 'null with no default restores to serial');
});

test('EB-482: same-key messages still serialize per key under the bus default', async () => {
  const bus = new EventBus();
  bus.setDefaultMaxConcurrentDeliveries(3);
  const activeByKey = new Map<string, number>();
  let keyOverlap = false;
  const order: string[] = [];
  bus.subscribe('t', async (msg) => {
    const key = msg.key ?? '<none>';
    activeByKey.set(key, (activeByKey.get(key) ?? 0) + 1);
    if ((activeByKey.get(key) ?? 0) > 1) keyOverlap = true;
    order.push(`start:${key}`);
    await sleep(15);
    activeByKey.set(key, (activeByKey.get(key) ?? 0) - 1);
    order.push(`end:${key}`);
  });
  bus.publish('t', 'a', { key: 'k1' });
  bus.publish('t', 'b', { key: 'k1' });
  bus.publish('t', 'c', { key: 'k2' });
  bus.publish('t', 'd');
  await sleep(200);
  assert.equal(keyOverlap, false, 'same-key handlers never overlap, even under the bus default');
  assert.equal(order.filter((e) => e.startsWith('start:')).length, 4, 'all four messages were delivered');
  const aEnd = order.indexOf('end:k1');
  const bStart = order.indexOf('start:k1', order.indexOf('start:k1') + 1);
  assert.ok(aEnd >= 0 && bStart > aEnd, 'same-key order preserved: a fully before b');
});

test('EB-482: revising to the current default is a no-op with no audit, and the getter follows the live value', () => {
  const bus = new EventBus();
  bus.setDefaultMaxConcurrentDeliveries(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_max_concurrent_deliveries_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setDefaultMaxConcurrentDeliveries(2);
  assert.equal(bus.getDefaultMaxConcurrentDeliveries(), 2);
  bus.setDefaultMaxConcurrentDeliveries(2);
  assert.equal(bus.getDefaultMaxConcurrentDeliveries(), 2);
  const audit = bus.getSubscriptionAudit({ action: 'default_max_concurrent_deliveries_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { previous: null, maxConcurrentDeliveries: 2 });
  bus.setDefaultMaxConcurrentDeliveries(4, 'ops');
  assert.equal(bus.getDefaultMaxConcurrentDeliveries(), 4);
  const revised = bus.getSubscriptionAudit({ action: 'default_max_concurrent_deliveries_revised' });
  assert.equal(revised.length, 2);
  assert.deepEqual(revised[1].detail, { previous: 2, maxConcurrentDeliveries: 4 });
  assert.equal(revised[1].actor, 'ops');
  bus.setDefaultMaxConcurrentDeliveries(null);
  assert.equal(bus.getDefaultMaxConcurrentDeliveries(), undefined);
  const cleared = bus.getSubscriptionAudit({ action: 'default_max_concurrent_deliveries_revised' });
  assert.equal(cleared.length, 3);
  assert.deepEqual(cleared[2].detail, { previous: 4, maxConcurrentDeliveries: null });
});

test('EB-482: an illegal value or actor throws RangeError and the live default is untouched', () => {
  const bus = new EventBus();
  bus.setDefaultMaxConcurrentDeliveries(2);
  for (const bad of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, '2', {}] as unknown[]) {
    assert.throws(
      () => bus.setDefaultMaxConcurrentDeliveries(bad as number),
      RangeError,
      `max=${String(bad)}`,
    );
    assert.equal(bus.getDefaultMaxConcurrentDeliveries(), 2);
  }
  assert.throws(() => bus.setDefaultMaxConcurrentDeliveries(4, ''), RangeError);
  assert.equal(bus.getDefaultMaxConcurrentDeliveries(), 2);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_max_concurrent_deliveries_revised' }).length,
    1,
    'failed revisions must not audit',
  );
  // A failed subscribe-time value still throws from subscribe, not from the default.
  assert.throws(
    () => bus.subscribe('t', () => {}, { maxConcurrentDeliveries: 0 }),
    RangeError,
  );
});
