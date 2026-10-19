import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/**
 * A manually-advanced clock handed to the bus via `EventBusOptions.now`,
 * so due-time tests are deterministic instead of racing wall time. Due
 * sweeps run at the start of every flush, so advancing this clock past a
 * `deliverAt` and then publishing anything delivers deterministically.
 */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

/** A scripted random source: returns the given values in order, then repeats the last. */
function scriptedRandom(values: number[]) {
  let calls = 0;
  const fn = () => {
    calls += 1;
    return values[Math.min(calls - 1, values.length - 1)];
  };
  return { fn, calls: () => calls };
}

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-jitter-'));

test('invalid expiryJitterMs throws RangeError and schedules nothing', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {});
  for (const bad of [-1, -0.5, NaN, Infinity, '100' as unknown as number]) {
    assert.throws(
      () => bus.publishDelayed('t', 'x', { delayMs: 100, expiryJitterMs: bad }),
      RangeError,
      `expiryJitterMs=${String(bad)}`,
    );
  }
  assert.equal(bus.getStats().pendingDelayed, 0);
});

test('delayJitterRandom must be a function', () => {
  assert.throws(
    () => new EventBus({ delayJitterRandom: 'nope' as unknown as () => number }),
    RangeError,
  );
});

test('negative jitter shifts the due time earlier, deterministically', async () => {
  const clock = controllableClock();
  // random() -> 0 draws the most negative offset: (0*2-1)*200 = -200.
  const bus = new EventBus({ now: clock.now, delayJitterRandom: () => 0 });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  // Nominal due at t=2000; jittered due at t=1800.
  bus.publishDelayed('t', 'early', { delayMs: 1000, expiryJitterMs: 200 });
  await flush();
  assert.deepEqual(received, []);
  clock.nowMs = 1_799;
  bus.publish('t', 'poke');
  await flush();
  assert.deepEqual(received, ['poke']);
  clock.nowMs = 1_800;
  bus.publish('t', 'poke2');
  await flush();
  assert.deepEqual(received, ['poke', 'poke2', 'early']);
  assert.equal(bus.getStats().pendingDelayed, 0);
});

test('positive jitter shifts the due time later, deterministically', async () => {
  const clock = controllableClock();
  // random() -> 0.75 draws (0.75*2-1)*200 = +100.
  const bus = new EventBus({ now: clock.now, delayJitterRandom: () => 0.75 });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  // Nominal due at t=2000; jittered due at t=2100.
  bus.publishDelayed('t', 'late', { delayMs: 1000, expiryJitterMs: 200 });
  clock.nowMs = 2_000;
  bus.publish('t', 'poke');
  await flush();
  assert.deepEqual(received, ['poke']); // not yet: due at 2100
  clock.nowMs = 2_100;
  bus.publish('t', 'poke2');
  await flush();
  assert.deepEqual(received, ['poke', 'poke2', 'late']);
});

test('a batch of same-due schedules spreads out instead of fanning out in lockstep', async () => {
  const clock = controllableClock();
  // Three schedules, nominal due t=2000: draws give -200, 0, +200.
  const random = scriptedRandom([0, 0.5, 1 - Number.EPSILON]);
  const bus = new EventBus({ now: clock.now, delayJitterRandom: random.fn });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publishDelayed('t', 'a', { delayMs: 1000, expiryJitterMs: 200 });
  bus.publishDelayed('t', 'b', { delayMs: 1000, expiryJitterMs: 200 });
  bus.publishDelayed('t', 'c', { delayMs: 1000, expiryJitterMs: 200 });
  assert.equal(random.calls(), 3);
  clock.nowMs = 1_800;
  bus.publish('t', 'poke');
  await flush();
  assert.deepEqual(received, ['poke', 'a']); // only the -200 one is due
  clock.nowMs = 2_000;
  bus.publish('t', 'poke2');
  await flush();
  assert.deepEqual(received, ['poke', 'a', 'poke2', 'b']); // the +0 one joins
  clock.nowMs = 2_200;
  bus.publish('t', 'poke3');
  await flush();
  assert.deepEqual(received, ['poke', 'a', 'poke2', 'b', 'poke3', 'c']);
});

test('zero/absent jitter draws nothing from the random source', async () => {
  const clock = controllableClock();
  const random = scriptedRandom([0]);
  const bus = new EventBus({ now: clock.now, delayJitterRandom: random.fn });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publishDelayed('t', 'z', { delayMs: 1000, expiryJitterMs: 0 });
  bus.publishDelayed('t', 'plain', { delayMs: 1000 });
  assert.equal(random.calls(), 0);
  clock.nowMs = 2_000;
  bus.publish('t', 'poke');
  await flush();
  assert.deepEqual(received, ['poke', 'z', 'plain']);
});

test('cancelDelayed works on a jittered schedule', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, delayJitterRandom: () => 0 });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  const id = bus.publishDelayed('t', 'gone', { delayMs: 1000, expiryJitterMs: 200 });
  assert.equal(bus.getStats().pendingDelayed, 1);
  assert.equal(bus.cancelDelayed(id as string), true);
  clock.nowMs = 5_000;
  bus.publish('t', 'poke');
  await flush();
  assert.deepEqual(received, ['poke']);
  assert.equal(bus.getStats().pendingDelayed, 0);
});

test('jittered keyed schedules keep schedule-order per-key delivery', async () => {
  const clock = controllableClock();
  // First schedule draws ~+200 (due 2200), second draws -100 (due 1900):
  // fan-out order inverts, but per-key delivery must stay schedule order
  // (the key sequence numbers were assigned at schedule time).
  const random = scriptedRandom([1 - Number.EPSILON, 0.25]);
  const bus = new EventBus({ now: clock.now, delayJitterRandom: random.fn });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publishDelayed('t', 'first', { delayMs: 1000, expiryJitterMs: 200, key: 'k' });
  bus.publishDelayed('t', 'second', { delayMs: 1000, expiryJitterMs: 200, key: 'k' });
  clock.nowMs = 2_200;
  bus.publish('t', 'poke');
  await flush();
  // 'second' fanned out first (due 1900 < 2200) but keyed delivery holds
  // schedule order: first, then second.
  assert.deepEqual(received, ['poke', 'first', 'second']);
});

test('a jittered schedule survives restart with its jittered due time', async () => {
  const dir = freshDir();
  const clock = controllableClock();
  // random() -> 0: nominal due t=2000, jittered due t=1800.
  const bus1 = new EventBus({ now: clock.now, delayJitterRandom: () => 0, durableLogDir: dir });
  bus1.subscribe('t', () => {});
  bus1.publishDelayed('t', 'survivor', { delayMs: 1000, expiryJitterMs: 200 });
  await flush();
  // Simulate a crash: a new bus over the same directory WITHOUT closing
  // bus1 (close() cancels pending delayed schedules by design). The
  // schedule record carries the jittered deliverAt, so the rebuilt timer
  // honors it.
  const received: unknown[] = [];
  const bus2 = new EventBus({ now: clock.now, durableLogDir: dir });
  bus2.subscribe('t', (msg) => received.push(msg.payload));
  await flush();
  assert.deepEqual(received, []);
  clock.nowMs = 1_799;
  bus2.publish('t', 'poke');
  await flush();
  assert.deepEqual(received, ['poke']);
  clock.nowMs = 1_800;
  bus2.publish('t', 'poke2');
  await flush();
  assert.deepEqual(received, ['poke', 'poke2', 'survivor']);
  await bus2.close();
});
