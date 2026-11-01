import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function makeClock(startMs = 0) {
  let nowMs = startMs;
  return {
    now: () => nowMs,
    advance: (ms: number) => {
      nowMs += ms;
    },
  };
}

test('setSlowStart replaces the gate and re-ramps from the revision moment', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    slowStart: { initialPerSec: 1, rampMs: 10000 },
  });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0], 'the original gate admits its one-token initial budget');
  assert.equal(bus.pendingCount(sub.id), 4);

  bus.setSlowStart(sub.id, { initialPerSec: 3, rampMs: 10000 });
  // The replacement gate starts at the new initial rate: its 3-token
  // budget drains three of the four queued messages on the next flush.
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3], 'the new gate paces from the revision moment');
  assert.equal(bus.pendingCount(sub.id), 1);

  const events = bus.getSubscriptionAudit({ action: 'slow_start_revised' });
  assert.equal(events.length, 1);
  assert.equal(events[0].subId, sub.id);
  assert.deepEqual(events[0].detail, {
    slowStart: { initialPerSec: 3, rampMs: 10000, strategy: 'linear' },
    oldSlowStart: { initialPerSec: 1, rampMs: 10000, strategy: 'linear' },
  });
  sub.unsubscribe();
});

test('setSlowStart re-ramps a completed gate that is still attached', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    slowStart: { initialPerSec: 1, rampMs: 100 },
  });
  for (let i = 0; i < 3; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0]);
  assert.equal(bus.pendingCount(sub.id), 2);

  // Advance past the ramp window without draining: the gate is
  // complete but no drain has dropped it yet.
  clock.advance(200);
  assert.equal(bus.getStats().rampingSubscribers, 0, 'the elapsed gate no longer counts as ramping');

  bus.setSlowStart(sub.id, { initialPerSec: 2, rampMs: 10000 });
  assert.equal(bus.getStats().rampingSubscribers, 1, 'the revision re-ramps from the revision moment');
  bus.publish('t', 3);
  await flush();
  assert.deepEqual(received, [0, 1, 2], 'the fresh gate admits its new initial budget');
  assert.equal(bus.pendingCount(sub.id), 1);

  const events = bus.getSubscriptionAudit({ action: 'slow_start_revised' });
  assert.equal(events.length, 1);
  assert.deepEqual(events[0].detail, {
    slowStart: { initialPerSec: 2, rampMs: 10000, strategy: 'linear' },
    oldSlowStart: { initialPerSec: 1, rampMs: 100, strategy: 'linear' },
  });
  sub.unsubscribe();
});

test('setSlowStart with null clears the gate and a later revision is refused', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    slowStart: { initialPerSec: 1, rampMs: 10000 },
  });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0]);
  assert.equal(bus.pendingCount(sub.id), 4);

  bus.setSlowStart(sub.id, null);
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3, 4], 'clearing the gate drains the backlog at full speed');
  assert.equal(bus.pendingCount(sub.id), 0);

  const events = bus.getSubscriptionAudit({ action: 'slow_start_revised' });
  assert.equal(events.length, 1);
  assert.deepEqual(events[0].detail, {
    slowStart: null,
    oldSlowStart: { initialPerSec: 1, rampMs: 10000, strategy: 'linear' },
  });

  assert.throws(() => bus.setSlowStart(sub.id, { initialPerSec: 5 }), RangeError);
  assert.throws(() => bus.setSlowStart(sub.id, null), RangeError);
  sub.unsubscribe();
});

test('an identical revision is a no-op and true takes the defaults', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    slowStart: { initialPerSec: 3, rampMs: 2000, strategy: 'exponential' },
  });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0, 1, 2]);
  assert.equal(bus.pendingCount(sub.id), 2);

  bus.setSlowStart(sub.id, { initialPerSec: 3, rampMs: 2000, strategy: 'exponential' });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'slow_start_revised' }).length,
    0,
    're-affirming the current ramp audits nothing',
  );

  // `true` takes the documented defaults (10 / 1000ms / linear) — the
  // fresh gate's 10-token budget drains the backlog and the next
  // publishes up to it.
  bus.setSlowStart(sub.id, true);
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3, 4], 'the default gate drains the backlog');
  for (let i = 5; i < 17; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(
    received,
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
    'the default gate admits ten messages at once',
  );
  assert.equal(bus.pendingCount(sub.id), 4);

  const events = bus.getSubscriptionAudit({ action: 'slow_start_revised' });
  assert.equal(events.length, 1);
  assert.deepEqual(events[0].detail, {
    slowStart: { initialPerSec: 10, rampMs: 1000, strategy: 'linear' },
    oldSlowStart: { initialPerSec: 3, rampMs: 2000, strategy: 'exponential' },
  });
  sub.unsubscribe();
});

test('a partial revision takes the documented defaults for omitted fields', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    slowStart: { initialPerSec: 3, rampMs: 2000, strategy: 'exponential' },
  });

  // The revision is a full replacement, not a partial merge: the
  // omitted fields take the defaults (10 / linear), not the live values.
  bus.setSlowStart(sub.id, { rampMs: 500 });
  const events = bus.getSubscriptionAudit({ action: 'slow_start_revised' });
  assert.equal(events.length, 1);
  assert.deepEqual(events[0].detail, {
    slowStart: { initialPerSec: 10, rampMs: 500, strategy: 'linear' },
    oldSlowStart: { initialPerSec: 3, rampMs: 2000, strategy: 'exponential' },
  });
  for (let i = 0; i < 12; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(
    received,
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
    'the replacement gate admits its default ten-token budget',
  );
  sub.unsubscribe();
});

test('a revision with no headroom over the live shaping cap is a no-op', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    deliveryShaping: { messagesPerSec: 10, burst: 10 },
    slowStart: { initialPerSec: 1, rampMs: 10000 },
  });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0]);
  assert.equal(bus.pendingCount(sub.id), 4);

  // 20 >= the live shaping cap of 10: no ramp to run, so the live
  // gate is left alone — only an explicit `null` clears it.
  bus.setSlowStart(sub.id, { initialPerSec: 20, rampMs: 100 });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'slow_start_revised' }).length,
    0,
    'a degenerate revision audits nothing',
  );
  bus.publish('t', 5);
  await flush();
  assert.deepEqual(received, [0], 'the old gate still paces delivery');
  assert.equal(bus.pendingCount(sub.id), 5);

  // The same holds at exactly the cap; below it a real gate is built.
  bus.setSlowStart(sub.id, { initialPerSec: 10, rampMs: 100 });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'slow_start_revised' }).length,
    0,
    'initialPerSec equal to the live cap is still degenerate',
  );
  bus.setSlowStart(sub.id, { initialPerSec: 9, rampMs: 10000 });
  await flush();
  assert.deepEqual(
    received,
    [0, 1, 2, 3, 4, 5],
    'a below-cap revision builds a fresh gate that drains the backlog',
  );
  assert.equal(bus.pendingCount(sub.id), 0);
  const events = bus.getSubscriptionAudit({ action: 'slow_start_revised' });
  assert.equal(events.length, 1);
  assert.deepEqual(events[0].detail, {
    slowStart: { initialPerSec: 9, rampMs: 10000, strategy: 'linear' },
    oldSlowStart: { initialPerSec: 1, rampMs: 10000, strategy: 'linear' },
  });
  sub.unsubscribe();
});

test('setSlowStart is fail-closed: illegal revisions leave the gate untouched', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    slowStart: { initialPerSec: 1, rampMs: 10000 },
  });
  const plain = bus.subscribe('t', () => {});
  for (let i = 0; i < 3; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0]);
  assert.equal(bus.pendingCount(sub.id), 2);

  assert.throws(() => bus.setSlowStart('sub-999', { initialPerSec: 2 }), RangeError);
  assert.throws(() => bus.setSlowStart('', { initialPerSec: 2 }), RangeError);
  assert.throws(
    () => bus.setSlowStart(plain.id, { initialPerSec: 2 }),
    RangeError,
    'a subscriber that never opted into slowStart refuses revision',
  );
  const bad: unknown[] = [
    undefined,
    false,
    'yes',
    42,
    { initialPerSec: 0 },
    { initialPerSec: -1 },
    { initialPerSec: Number.NaN },
    { initialPerSec: Number.POSITIVE_INFINITY },
    { rampMs: 0 },
    { rampMs: -5 },
    { rampMs: Number.POSITIVE_INFINITY },
    { strategy: 'cubic' },
  ];
  for (const opts of bad) {
    assert.throws(
      () => bus.setSlowStart(sub.id, opts as never),
      RangeError,
      `expected RangeError for ${String(opts)}`,
    );
  }

  assert.equal(
    bus.getSubscriptionAudit({ action: 'slow_start_revised' }).length,
    0,
    'failed revisions audit nothing',
  );
  bus.publish('t', 3);
  await flush();
  assert.deepEqual(received, [0], 'the live gate is untouched by failed revisions');
  assert.equal(bus.pendingCount(sub.id), 3);
  sub.unsubscribe();
  plain.unsubscribe();
});
