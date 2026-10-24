import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type SubscriberBreakerTrippedEvent } from '../src/bus.ts';
import { RecordingAlertSink } from '../src/ops-alert.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const tick = (ms = 10) => new Promise<void>((resolve) => setTimeout(resolve, ms));

test('trips after maxConsecutiveFailures sync throws: auto-pauses, alerts, needs manual resume', async () => {
  const sink = new RecordingAlertSink();
  const bus = new EventBus({
    busId: 'breaker-1',
    opsAlert: { sink, secret: 's', dedupWindowMs: 60_000 },
  });
  const events: SubscriberBreakerTrippedEvent[] = [];
  bus.setSubscriberBreaker({
    maxConsecutiveFailures: 3,
    onTripped: (e) => events.push(e),
  });
  let calls = 0;
  let fail = true;
  const sub = bus.subscribe('t', () => {
    calls += 1;
    if (fail) throw new Error('boom');
  });

  bus.publish('t', 'a');
  bus.publish('t', 'b');
  bus.publish('t', 'c');
  await flush();

  // The third consecutive failure tripped the breaker mid-drain: no
  // throw escaped, the subscriber auto-paused via pauseSubscriber.
  assert.equal(calls, 3);
  assert.equal(events.length, 1);
  assert.equal(events[0].subscriberId, sub.id);
  assert.equal(events[0].pattern, 't');
  assert.equal(events[0].consecutiveFailures, 3);
  assert.equal(events[0].pendingMessages, 0);
  assert.equal(bus.getStats().pausedSubscribers, 1);

  // getStats exposes the breaker state.
  const breaker = bus.getStats().subscriberBreaker;
  assert.equal(breaker.enabled, true);
  assert.equal(breaker.maxConsecutiveFailures, 3);
  assert.equal(breaker.tripped.length, 1);
  assert.equal(breaker.tripped[0].subscriberId, sub.id);
  assert.equal(breaker.tripped[0].consecutiveFailures, 3);

  // The trip is audited as a subscriber_paused with the breaker reason.
  const audit = bus.getSubscriptionAudit({ action: 'subscriber_paused' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].subId, sub.id);
  assert.match((audit[0].detail as { reason: string }).reason, /subscriber-breaker/);

  // The ops alert fired on the ops-alert channel.
  await tick();
  assert.equal(sink.deliveries.length, 1);
  assert.equal(sink.deliveries[0].alert.kind, 'subscriber-breaker-tripped');
  assert.equal(sink.deliveries[0].alert.detail.subscriberId, sub.id);
  assert.equal(sink.deliveries[0].alert.detail.consecutiveFailures, 3);

  // Paused: new publishes stay queued (preserved, not dropped), none delivered.
  bus.publish('t', 'd');
  bus.publish('t', 'e');
  await flush();
  assert.equal(calls, 3);
  assert.equal(events.length, 1); // no repeat trip while already paused
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.droppedCount(sub.id), 0);

  // Recovery is manual: resumeSubscriber clears the pause and resets
  // the streak, so the backlog redelivers in order.
  fail = false;
  assert.equal(bus.resumeSubscriber(sub.id), true);
  await flush();
  assert.equal(calls, 5);
  assert.equal(bus.getStats().pausedSubscribers, 0);
  assert.equal(bus.getStats().subscriberBreaker.tripped.length, 0);

  // The streak was reset: two more failures do not re-trip...
  fail = true;
  bus.publish('t', 'f');
  bus.publish('t', 'g');
  await flush();
  assert.equal(calls, 7);
  assert.equal(events.length, 1);
  // ...but the third consecutive failure trips again.
  bus.publish('t', 'h');
  await flush();
  assert.equal(events.length, 2);
  assert.equal(events[1].consecutiveFailures, 3);
  bus.destroy?.();
});

test('a successful delivery resets the consecutive-failure streak', async () => {
  const bus = new EventBus();
  bus.setSubscriberBreaker({ maxConsecutiveFailures: 3 });
  let calls = 0;
  let fail = true;
  const sub = bus.subscribe('t', () => {
    calls += 1;
    if (fail) throw new Error('boom');
  });

  bus.publish('t', 'a'); // fails (1)
  bus.publish('t', 'b'); // fails (2)
  await flush();
  assert.equal(calls, 2);
  assert.equal(bus.getStats().pausedSubscribers, 0);

  fail = false;
  bus.publish('t', 'c'); // succeeds: streak back to 0
  await flush();
  assert.equal(calls, 3);

  fail = true;
  bus.publish('t', 'd'); // fails (1)
  bus.publish('t', 'e'); // fails (2)
  await flush();
  assert.equal(calls, 5);
  assert.equal(bus.getStats().pausedSubscribers, 0);
  assert.equal(bus.getStats().subscriberBreaker.tripped.length, 0);
  void sub;
  bus.destroy?.();
});

test('async rejections count exactly like sync throws', async () => {
  const bus = new EventBus();
  const events: SubscriberBreakerTrippedEvent[] = [];
  bus.setSubscriberBreaker({
    maxConsecutiveFailures: 2,
    onTripped: (e) => events.push(e),
  });
  let calls = 0;
  const sub = bus.subscribe('t', () => {
    calls += 1;
    return Promise.reject(new Error('async boom'));
  });

  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  await tick(20); // let both rejections settle

  // No unhandled rejection, no throw escaped: the drain survived.
  assert.equal(calls, 2);
  assert.equal(events.length, 1);
  assert.equal(events[0].consecutiveFailures, 2);
  assert.equal(events[0].subscriberId, sub.id);
  assert.equal(bus.getStats().pausedSubscribers, 1);

  // An async fulfillment resets the streak: resume, then one success,
  // then one failure must not trip.
  assert.equal(bus.resumeSubscriber(sub.id), true);
  let fail = false;
  const sub2 = bus.subscribe('t2', () => {
    calls += 1;
    return fail ? Promise.reject(new Error('x')) : Promise.resolve();
  });
  void sub2;
  bus.publish('t2', 'ok');
  await flush();
  await tick(20);
  fail = true;
  bus.publish('t2', 'bad');
  await flush();
  await tick(20);
  assert.equal(bus.getStats().pausedSubscribers, 0);
  bus.destroy?.();
});

test('breaker and fault isolation are independent: each recovers its own way', async () => {
  const bus = new EventBus();
  const tripped: SubscriberBreakerTrippedEvent[] = [];
  bus.setSubscriberBreaker({ maxConsecutiveFailures: 3, onTripped: (e) => tripped.push(e) });
  let calls = 0;
  const sub = bus.subscribe(
    't',
    () => {
      calls += 1;
      throw new Error('boom');
    },
    { handlerFaultIsolation: { maxConsecutiveErrors: 2 } },
  );

  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  // Isolation quarantined at 2; the breaker streak sits at 2, untripped.
  assert.equal(calls, 2);
  assert.equal(bus.getStats().quarantinedSubscribers, 1);
  assert.equal(bus.getStats().pausedSubscribers, 0);
  assert.equal(tripped.length, 0);

  // The quarantine recovers through resumeQuarantined — the breaker's
  // streak is untouched.
  assert.equal(bus.resumeQuarantined(sub.id), true);
  bus.publish('t', 'c');
  await flush();
  // Isolation streak was 1, breaker streak hit 3: the breaker trips
  // into the EB-113 pause, not the quarantine.
  assert.equal(calls, 3);
  assert.equal(tripped.length, 1);
  assert.equal(bus.getStats().pausedSubscribers, 1);
  assert.equal(bus.getStats().quarantinedSubscribers, 0);

  // And it recovers through resumeSubscriber, not resumeQuarantined.
  assert.equal(bus.resumeSubscriber(sub.id), true);
  assert.equal(bus.resumeQuarantined(sub.id), false);
  assert.equal(bus.getStats().pausedSubscribers, 0);
  bus.destroy?.();
});

test('re-setting the breaker resets every streak; disarming stops observation', async () => {
  const bus = new EventBus();
  bus.setSubscriberBreaker({ maxConsecutiveFailures: 3 });
  let calls = 0;
  bus.subscribe('t', () => {
    calls += 1;
    throw new Error('boom');
  });

  bus.publish('t', 'a');
  bus.publish('t', 'b'); // streak 2
  await flush();
  assert.equal(bus.getStats().pausedSubscribers, 0);

  bus.setSubscriberBreaker({ maxConsecutiveFailures: 3 }); // streak reset
  bus.publish('t', 'c');
  bus.publish('t', 'd'); // streak 2 again — no trip
  await flush();
  assert.equal(calls, 4);
  assert.equal(bus.getStats().pausedSubscribers, 0);

  assert.equal(bus.clearSubscriberBreaker(), true);
  assert.equal(bus.clearSubscriberBreaker(), false);
  assert.equal(bus.getStats().subscriberBreaker.enabled, false);
  assert.equal(bus.getStats().subscriberBreaker.maxConsecutiveFailures, null);
  bus.destroy?.();
});

test('setSubscriberBreaker validates its options', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setSubscriberBreaker({ maxConsecutiveFailures: 0 }), RangeError);
  assert.throws(() => bus.setSubscriberBreaker({ maxConsecutiveFailures: -2 }), RangeError);
  assert.throws(() => bus.setSubscriberBreaker({ maxConsecutiveFailures: 1.5 }), RangeError);
  assert.throws(
    () => bus.setSubscriberBreaker({ onTripped: 42 as unknown as () => void }),
    RangeError,
  );
  // Defaults: threshold 5.
  bus.setSubscriberBreaker();
  assert.equal(bus.getStats().subscriberBreaker.enabled, true);
  assert.equal(bus.getStats().subscriberBreaker.maxConsecutiveFailures, 5);
  bus.destroy?.();
});

test('a disarmed bus keeps its old delivery behavior', () => {
  const bus = new EventBus();
  const breaker = bus.getStats().subscriberBreaker;
  assert.equal(breaker.enabled, false);
  assert.equal(breaker.maxConsecutiveFailures, null);
  assert.deepEqual(breaker.tripped, []);
  bus.destroy?.();
});
