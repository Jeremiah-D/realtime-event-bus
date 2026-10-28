import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Injectable clock so window slides are deterministic. */
function makeClock(startMs = 0) {
  let nowMs = startMs;
  return {
    now: () => nowMs,
    advance: (ms: number) => {
      nowMs += ms;
    },
  };
}

const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

function waitingFor(bus: EventBus, subscriberId: string): number {
  const row = bus.getStats().rateLimitedWaiting.find((r) => r.subscriberId === subscriberId);
  assert.ok(row !== undefined, 'expected a rateLimitedWaiting row');
  return row.waiting;
}

test('setRateLimit tightening starts a fresh window: old deliveries do not count', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    rateLimit: { maxMessages: 5, perWindowMs: 1000 },
  });
  bus.publish('t', 0);
  bus.publish('t', 1);
  await flush();
  assert.deepEqual(received, [0, 1]);

  // Tighten to one per window: the two deliveries under the old
  // window must not count against the new one.
  bus.setRateLimit(sub.id, { maxMessages: 1, perWindowMs: 1000 });
  bus.publish('t', 2);
  await flush();
  assert.deepEqual(received, [0, 1, 2], 'the fresh window admits one delivery');
  bus.publish('t', 3);
  await flush();
  assert.deepEqual(received, [0, 1, 2], 'the tightened window now binds');
  assert.equal(bus.pendingCount(sub.id), 1);
  assert.equal(waitingFor(bus, sub.id), 1);

  const audit = bus.getSubscriptionAudit({ action: 'rate_limit_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].subId, sub.id);
  assert.deepEqual(audit[0].detail, {
    rateLimit: { maxMessages: 1, perWindowMs: 1000 },
    oldRateLimit: { maxMessages: 5, perWindowMs: 1000 },
  });
  sub.unsubscribe();
});

test('setRateLimit loosening admits held messages immediately', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    rateLimit: { maxMessages: 1, perWindowMs: 60_000 },
  });
  bus.publish('t', 'a');
  await flush();
  bus.publish('t', 'b');
  await flush();
  assert.deepEqual(received, ['a']);
  assert.equal(waitingFor(bus, sub.id), 1);

  bus.setRateLimit(sub.id, { maxMessages: 3, perWindowMs: 60_000 });
  await flush();
  assert.deepEqual(received, ['a', 'b'], 'loosening drains the held message without a new publish');
  assert.equal(bus.pendingCount(sub.id), 0);

  const audit = bus.getSubscriptionAudit({ action: 'rate_limit_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    rateLimit: { maxMessages: 3, perWindowMs: 60_000 },
    oldRateLimit: { maxMessages: 1, perWindowMs: 60_000 },
  });
  sub.unsubscribe();
});

test('setRateLimit with the current values resets the window instead of no-op-ing', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    rateLimit: { maxMessages: 1, perWindowMs: 60_000 },
  });
  bus.publish('t', 'a');
  await flush();
  bus.publish('t', 'b');
  await flush();
  assert.deepEqual(received, ['a']);
  assert.equal(waitingFor(bus, sub.id), 1);

  // Same bounds, but the limiter instance is replaced: the window
  // starts fresh, so the held message drains and the next one waits.
  bus.setRateLimit(sub.id, { maxMessages: 1, perWindowMs: 60_000 });
  await flush();
  assert.deepEqual(received, ['a', 'b']);
  bus.publish('t', 'c');
  await flush();
  assert.deepEqual(received, ['a', 'b']);
  assert.equal(bus.pendingCount(sub.id), 1);

  const audit = bus.getSubscriptionAudit({ action: 'rate_limit_revised' });
  assert.equal(audit.length, 1, 'a same-values revision is still audited');
  assert.deepEqual(audit[0].detail, {
    rateLimit: { maxMessages: 1, perWindowMs: 60_000 },
    oldRateLimit: { maxMessages: 1, perWindowMs: 60_000 },
  });
  sub.unsubscribe();
});

test('setRateLimit null clears the limit, drains held messages, and refuses later revisions', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    rateLimit: { maxMessages: 1, perWindowMs: 60_000 },
  });
  bus.publish('t', 'a');
  await flush();
  bus.publish('t', 'b');
  await flush();
  assert.deepEqual(received, ['a']);
  assert.equal(waitingFor(bus, sub.id), 1);

  bus.setRateLimit(sub.id, null);
  await flush();
  assert.deepEqual(received, ['a', 'b'], 'clearing drains the held message ungated');
  await sleep(50);
  assert.deepEqual(received, ['a', 'b'], 'the cancelled re-flush timer never fires a second copy');
  bus.publish('t', 'c');
  await flush();
  assert.deepEqual(received, ['a', 'b', 'c'], 'later messages are unlimited');

  const audit = bus.getSubscriptionAudit({ action: 'rate_limit_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    rateLimit: null,
    oldRateLimit: { maxMessages: 1, perWindowMs: 60_000 },
  });

  assert.throws(
    () => bus.setRateLimit(sub.id, { maxMessages: 2, perWindowMs: 1000 }),
    RangeError,
    'a revision after a clear is refused',
  );
  sub.unsubscribe();
});

test('setRateLimit is fail-closed and leaves the live limiter and audit untouched', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    rateLimit: { maxMessages: 2, perWindowMs: 1000 },
  });
  const plain = bus.subscribe('u', () => {});

  assert.throws(() => bus.setRateLimit('no-such-sub', { maxMessages: 2, perWindowMs: 1000 }), RangeError);
  assert.throws(() => bus.setRateLimit(plain.id, { maxMessages: 2, perWindowMs: 1000 }), RangeError, 'an unlimited subscriber cannot be revised');
  assert.throws(() => bus.setRateLimit(sub.id, undefined as never), RangeError, 'undefined is not a clear');
  assert.throws(() => bus.setRateLimit(sub.id, 'x' as never), RangeError);
  assert.throws(() => bus.setRateLimit(sub.id, [] as never), RangeError);
  for (const bad of [
    { maxMessages: 2 }, // missing perWindowMs: revisions must be complete
    { perWindowMs: 1000 }, // missing maxMessages
    {},
    { maxMessages: 0, perWindowMs: 1000 },
    { maxMessages: -1, perWindowMs: 1000 },
    { maxMessages: 1.5, perWindowMs: 1000 },
    { maxMessages: NaN, perWindowMs: 1000 },
    { maxMessages: 2, perWindowMs: 0 },
    { maxMessages: 2, perWindowMs: -5 },
    { maxMessages: 2, perWindowMs: NaN },
    { maxMessages: 2, perWindowMs: Infinity },
  ]) {
    assert.throws(() => bus.setRateLimit(sub.id, bad as never), RangeError);
  }
  assert.equal(bus.getSubscriptionAudit({ action: 'rate_limit_revised' }).length, 0, 'no failed revision is audited');

  // The original limit is untouched by every failed attempt.
  bus.publish('t', 0);
  bus.publish('t', 1);
  await flush();
  assert.deepEqual(received, [0, 1]);
  bus.publish('t', 2);
  await flush();
  assert.deepEqual(received, [0, 1]);
  assert.equal(waitingFor(bus, sub.id), 1);
  sub.unsubscribe();
  plain.unsubscribe();
});
