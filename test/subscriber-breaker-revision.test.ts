import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  BusClosedError,
  type SubscriberBreakerTrippedEvent,
} from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** Subscribes a handler that throws while `fail` is true. */
function failingSubscriber(bus: EventBus, topic = 't') {
  let calls = 0;
  let fail = true;
  const sub = bus.subscribe(topic, () => {
    calls += 1;
    if (fail) throw new Error('boom');
  });
  return {
    sub,
    calls: () => calls,
    setFail: (v: boolean) => {
      fail = v;
    },
  };
}

const revisedAudit = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'subscriber_breaker_revised' });

test('tightening trips earlier at the new threshold: the streak is kept', async () => {
  const bus = new EventBus();
  bus.setSubscriberBreaker({ maxConsecutiveFailures: 5 });
  const { sub, calls } = failingSubscriber(bus);
  bus.publish('t', 'a'); // streak 1
  bus.publish('t', 'b'); // streak 2
  await flush();
  assert.equal(calls(), 2);
  assert.equal(bus.getStats().subscriberBreaker.tripped.length, 0);

  // Tighten below the live streak: the next failure trips at 3, not 5.
  const snapshot = bus.reviseSubscriberBreaker({ maxConsecutiveFailures: 3 });
  assert.deepEqual(snapshot, { maxConsecutiveFailures: 3, tripped: [] });
  bus.publish('t', 'c'); // streak 3 → trips at the new threshold
  await flush();
  assert.equal(calls(), 3);
  assert.equal(bus.getStats().pausedSubscribers, 1);
  assert.equal(bus.getStats().subscriberBreaker.tripped.length, 1);
  assert.equal(bus.getStats().subscriberBreaker.tripped[0].subscriberId, sub.id);
  assert.equal(bus.getStats().subscriberBreaker.tripped[0].consecutiveFailures, 3);
});

test('loosening after a trip: the subscriber stays tripped, streak not zeroed', async () => {
  const bus = new EventBus();
  bus.setSubscriberBreaker({ maxConsecutiveFailures: 3 });
  const { sub, calls } = failingSubscriber(bus);
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  bus.publish('t', 'c');
  await flush();
  assert.equal(calls(), 3);
  assert.equal(bus.getStats().subscriberBreaker.tripped.length, 1);

  // Loosen the threshold: the trip stands, the streak is untouched.
  const snapshot = bus.reviseSubscriberBreaker({ maxConsecutiveFailures: 5 });
  assert.equal(snapshot.maxConsecutiveFailures, 5);
  assert.equal(snapshot.tripped.length, 1);
  assert.equal(snapshot.tripped[0].subscriberId, sub.id);
  assert.equal(snapshot.tripped[0].consecutiveFailures, 3);
  assert.equal(bus.getStats().subscriberBreaker.tripped.length, 1);

  // Still paused: further publishes stay queued, no repeat trip.
  bus.publish('t', 'd');
  await flush();
  assert.equal(calls(), 3);
  assert.equal(bus.pendingCount(sub.id), 1);
});

test('streak accumulates across a revision that only swaps the callback', async () => {
  const bus = new EventBus();
  const first: SubscriberBreakerTrippedEvent[] = [];
  const second: SubscriberBreakerTrippedEvent[] = [];
  bus.setSubscriberBreaker({
    maxConsecutiveFailures: 3,
    onTripped: (e) => first.push(e),
  });
  const { calls } = failingSubscriber(bus);
  bus.publish('t', 'a'); // streak 1
  bus.publish('t', 'b'); // streak 2
  await flush();
  assert.equal(calls(), 2);

  // Swap only the callback: the threshold is unchanged and the streak
  // is not reset — one more failure trips, not three.
  bus.reviseSubscriberBreaker({ onTripped: (e) => second.push(e) });
  bus.publish('t', 'c'); // streak 3 → trips
  await flush();
  assert.equal(calls(), 3);
  assert.equal(first.length, 0);
  assert.equal(second.length, 1);
  assert.equal(second[0].consecutiveFailures, 3);
});

test('illegal merge throws RangeError and the live rule is fully untouched', async () => {
  const bus = new EventBus();
  const events: SubscriberBreakerTrippedEvent[] = [];
  bus.setSubscriberBreaker({
    maxConsecutiveFailures: 3,
    onTripped: (e) => events.push(e),
  });
  const { calls } = failingSubscriber(bus);
  bus.publish('t', 'a'); // streak 1
  await flush();
  assert.equal(calls(), 1);

  const before = bus.getSubscriberBreaker();
  const bad: Array<unknown> = [
    { maxConsecutiveFailures: 0 },
    { maxConsecutiveFailures: -2 },
    { maxConsecutiveFailures: 1.5 },
    { maxConsecutiveFailures: Number.NaN },
    { onTripped: 'not-a-function' },
    { onTripped: 42 },
    // One good field does not save a bad sibling.
    { maxConsecutiveFailures: 4, onTripped: 'nope' },
    // A non-object patch asks nothing valid to move.
    null,
    42,
    'nope',
    [],
  ];
  for (const patch of bad) {
    assert.throws(
      () => bus.reviseSubscriberBreaker(patch as never),
      RangeError,
      `expected RangeError for ${JSON.stringify(patch)}`,
    );
  }
  assert.deepEqual(bus.getSubscriberBreaker(), before);
  assert.equal(revisedAudit(bus).length, 0);

  // The live rule is intact: two more failures trip it at the
  // unchanged threshold of 3 (streak 1 + 2, not a reset streak).
  bus.publish('t', 'b');
  bus.publish('t', 'c');
  await flush();
  assert.equal(calls(), 3);
  assert.equal(events.length, 1);
  assert.equal(events[0].consecutiveFailures, 3);
});

test('revising an unarmed bus throws RangeError', () => {
  const bus = new EventBus();
  assert.throws(
    () => bus.reviseSubscriberBreaker({ maxConsecutiveFailures: 2 }),
    RangeError,
  );
  assert.equal(revisedAudit(bus).length, 0);
  // Clearing the rule un-arms it again: a later revision is refused.
  bus.setSubscriberBreaker({ maxConsecutiveFailures: 2 });
  assert.equal(bus.clearSubscriberBreaker(), true);
  assert.throws(
    () => bus.reviseSubscriberBreaker({ maxConsecutiveFailures: 2 }),
    RangeError,
  );
  assert.equal(revisedAudit(bus).length, 0);
});

test('same-value revision is a no-op: no audit', () => {
  const bus = new EventBus();
  const onTripped = () => {};
  bus.setSubscriberBreaker({ maxConsecutiveFailures: 3, onTripped });
  assert.deepEqual(bus.reviseSubscriberBreaker({ maxConsecutiveFailures: 3 }), {
    maxConsecutiveFailures: 3,
    tripped: [],
  });
  assert.deepEqual(bus.reviseSubscriberBreaker({ onTripped }), {
    maxConsecutiveFailures: 3,
    tripped: [],
  });
  assert.deepEqual(bus.reviseSubscriberBreaker({}), {
    maxConsecutiveFailures: 3,
    tripped: [],
  });
  assert.equal(revisedAudit(bus).length, 0);
});

test('audit records previous and current threshold and callback presence', () => {
  const bus = new EventBus();
  bus.setSubscriberBreaker({
    maxConsecutiveFailures: 5,
    onTripped: () => {},
  });
  bus.reviseSubscriberBreaker({ maxConsecutiveFailures: 2 });
  assert.deepEqual(revisedAudit(bus)[0].detail, {
    previous: { maxConsecutiveFailures: 5, onTripped: true },
    current: { maxConsecutiveFailures: 2, onTripped: true },
  });
  // Clearing the callback is a change: the audit says so.
  bus.reviseSubscriberBreaker({ onTripped: null });
  assert.deepEqual(revisedAudit(bus)[1].detail, {
    previous: { maxConsecutiveFailures: 2, onTripped: true },
    current: { maxConsecutiveFailures: 2, onTripped: false },
  });
  assert.equal(revisedAudit(bus).length, 2);
});

test('getSubscriberBreaker returns the live threshold and tripped list, or undefined when unarmed', async () => {
  const bus = new EventBus();
  assert.equal(bus.getSubscriberBreaker(), undefined);
  bus.setSubscriberBreaker({ maxConsecutiveFailures: 5 });
  assert.deepEqual(bus.getSubscriberBreaker(), {
    maxConsecutiveFailures: 5,
    tripped: [],
  });
  const { sub } = failingSubscriber(bus);
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  // No trip yet at threshold 5: the tripped list stays empty.
  assert.deepEqual(bus.getSubscriberBreaker(), {
    maxConsecutiveFailures: 5,
    tripped: [],
  });
  bus.reviseSubscriberBreaker({ maxConsecutiveFailures: 2 });
  // The threshold moved live; the streak (2) already meets it, but the
  // trip itself only happens on the next failure — the getter reflects
  // the live rule, not a re-evaluation.
  assert.deepEqual(bus.getSubscriberBreaker(), {
    maxConsecutiveFailures: 2,
    tripped: [],
  });
  bus.publish('t', 'c'); // streak 3 ≥ 2 → trips
  await flush();
  const snapshot = bus.getSubscriberBreaker();
  assert.equal(snapshot?.maxConsecutiveFailures, 2);
  assert.equal(snapshot?.tripped.length, 1);
  assert.equal(snapshot?.tripped[0].subscriberId, sub.id);
  assert.equal(snapshot?.tripped[0].consecutiveFailures, 3);
  assert.equal(snapshot?.tripped[0].pendingMessages, 0);

  assert.equal(bus.clearSubscriberBreaker(), true);
  assert.equal(bus.getSubscriberBreaker(), undefined);
});

test('replacing onTripped: the next trip calls the new callback only', async () => {
  const bus = new EventBus();
  const first: SubscriberBreakerTrippedEvent[] = [];
  const second: SubscriberBreakerTrippedEvent[] = [];
  bus.setSubscriberBreaker({
    maxConsecutiveFailures: 3,
    onTripped: (e) => first.push(e),
  });
  const { calls } = failingSubscriber(bus);
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  bus.reviseSubscriberBreaker({ onTripped: (e) => second.push(e) });
  bus.publish('t', 'c'); // trips
  await flush();
  assert.equal(calls(), 3);
  assert.equal(first.length, 0);
  assert.equal(second.length, 1);
  assert.equal(second[0].consecutiveFailures, 3);
});

test('onTripped: null clears the callback — the trip still pauses, silently', async () => {
  const bus = new EventBus();
  const events: SubscriberBreakerTrippedEvent[] = [];
  bus.setSubscriberBreaker({
    maxConsecutiveFailures: 3,
    onTripped: (e) => events.push(e),
  });
  const { sub, calls } = failingSubscriber(bus);
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  bus.reviseSubscriberBreaker({ onTripped: null });
  bus.publish('t', 'c'); // trips
  await flush();
  assert.equal(calls(), 3);
  assert.equal(events.length, 0);
  assert.equal(bus.getStats().pausedSubscribers, 1);
  assert.equal(bus.getStats().subscriberBreaker.tripped.length, 1);
  // The subscriber is still recoverable by hand.
  bus.resumeSubscriber(sub.id);
  assert.equal(bus.getStats().pausedSubscribers, 0);
});

test('revision after close is rejected with BusClosedError', async () => {
  const bus = new EventBus();
  bus.setSubscriberBreaker({ maxConsecutiveFailures: 3 });
  await bus.close();
  assert.throws(
    () => bus.reviseSubscriberBreaker({ maxConsecutiveFailures: 2 }),
    (err: unknown) => {
      assert.ok(err instanceof BusClosedError, `expected BusClosedError, got ${String(err)}`);
      assert.equal((err as Error).name, 'BusClosedError');
      assert.equal((err as BusClosedError).code, 'BUS_CLOSED');
      assert.equal((err as BusClosedError).action, 'subscribe');
      return true;
    },
  );
  assert.equal(revisedAudit(bus).length, 0);
});
