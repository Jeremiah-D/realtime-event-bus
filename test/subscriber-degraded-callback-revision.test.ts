import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type DegradedEvent } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/**
 * Live revision of the degraded callback (EB-435): a health-probed
 * subscriber's `onDegraded` can be swapped or cleared without
 * re-subscribing, so changing the alert recipient never loses the
 * health counter or the degraded state.
 */

test('setDegradedCallback swaps the callback before the next degradation', async () => {
  const bus = new EventBus();
  const oldEvents: DegradedEvent[] = [];
  const newEvents: DegradedEvent[] = [];
  const oldCb = (e: DegradedEvent) => oldEvents.push(e);
  const newCb = (e: DegradedEvent) => newEvents.push(e);
  const sub = bus.subscribe(
    't',
    () => {
      throw new Error('boom');
    },
    { healthProbe: { maxConsecutiveFailures: 2 }, onDegraded: oldCb },
  );

  bus.publish('t', 'a');
  await flush();
  assert.equal(oldEvents.length, 0, 'one failure stays under the threshold of 2');
  assert.equal(bus.subscriberHealth(sub.id).consecutiveFailures, 1);

  bus.setDegradedCallback(sub.id, newCb);
  bus.publish('t', 'b');
  await flush();
  assert.equal(newEvents.length, 1, 'the next failure degrades into the new callback');
  assert.equal(newEvents[0].consecutiveFailures, 2);
  assert.equal(newEvents[0].reason, 'error');
  assert.equal(oldEvents.length, 0, 'the old callback never fires again');

  const audit = bus.getSubscriptionAudit({ action: 'subscriber_degraded_callback_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].subId, sub.id);
  assert.deepEqual(audit[0].detail, { onDegraded: 'set', oldOnDegraded: 'set' });
  sub.unsubscribe();
});

test('setDegradedCallback with null clears the callback but the pause still happens', async () => {
  const bus = new EventBus();
  const events: DegradedEvent[] = [];
  const sub = bus.subscribe(
    't',
    () => {
      throw new Error('boom');
    },
    { healthProbe: { maxConsecutiveFailures: 1 }, onDegraded: (e) => events.push(e) },
  );

  bus.setDegradedCallback(sub.id, null);
  bus.publish('t', 'a');
  await flush();
  assert.equal(events.length, 0, 'a cleared callback never fires');
  assert.deepEqual(bus.subscriberHealth(sub.id), {
    subscriberId: sub.id,
    enabled: true,
    degraded: true,
    consecutiveFailures: 1,
  });

  const audit = bus.getSubscriptionAudit({ action: 'subscriber_degraded_callback_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { onDegraded: 'cleared', oldOnDegraded: 'set' });
  sub.unsubscribe();
});

test('setDegradedCallback rejects an unknown or empty subscriber id', () => {
  const bus = new EventBus();
  const cb = () => {};
  assert.throws(() => bus.setDegradedCallback('no-such-sub', cb), RangeError);
  assert.throws(() => bus.setDegradedCallback('', cb), RangeError);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'subscriber_degraded_callback_revised' }).length,
    0,
    'a refused revision writes no audit',
  );
});

test('setDegradedCallback rejects a subscriber with no healthProbe', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {});
  assert.throws(() => bus.setDegradedCallback(sub.id, () => {}), RangeError);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'subscriber_degraded_callback_revised' }).length,
    0,
    'a refused revision writes no audit',
  );
  sub.unsubscribe();
});

test('setDegradedCallback rejects an illegal callback value and leaves the live callback untouched', async () => {
  const bus = new EventBus();
  const events: DegradedEvent[] = [];
  const sub = bus.subscribe(
    't',
    () => {
      throw new Error('boom');
    },
    { healthProbe: { maxConsecutiveFailures: 1 }, onDegraded: (e) => events.push(e) },
  );

  assert.throws(
    () =>
      bus.setDegradedCallback(
        sub.id,
        'not-a-function' as unknown as (event: DegradedEvent) => void,
      ),
    RangeError,
  );
  assert.equal(
    bus.getSubscriptionAudit({ action: 'subscriber_degraded_callback_revised' }).length,
    0,
    'a refused revision writes no audit',
  );

  bus.publish('t', 'a');
  await flush();
  assert.equal(events.length, 1, 'the live callback still fires after the refused revision');
  sub.unsubscribe();
});

test('setDegradedCallback with the live callback is a no-op with no audit', () => {
  const bus = new EventBus();
  const cb = () => {};
  const sub = bus.subscribe('t', () => {}, { healthProbe: { maxConsecutiveFailures: 3 } });

  // Clearing a callback that was never set is a no-op, not a clear.
  bus.setDegradedCallback(sub.id, null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'subscriber_degraded_callback_revised' }).length,
    0,
  );

  bus.setDegradedCallback(sub.id, cb);
  let audit = bus.getSubscriptionAudit({ action: 'subscriber_degraded_callback_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { onDegraded: 'set', oldOnDegraded: 'absent' });

  // Re-affirming the same function reference is a no-op.
  bus.setDegradedCallback(sub.id, cb);
  audit = bus.getSubscriptionAudit({ action: 'subscriber_degraded_callback_revised' });
  assert.equal(audit.length, 1, 'a same-value revision writes no second event');
  sub.unsubscribe();
});

test('swapping the degraded callback leaves the counter and recovery semantics untouched', async () => {
  const bus = new EventBus();
  const oldEvents: DegradedEvent[] = [];
  const newEvents: DegradedEvent[] = [];
  let fail = true;
  const sub = bus.subscribe(
    't',
    () => {
      if (fail) throw new Error('boom');
    },
    {
      healthProbe: { maxConsecutiveFailures: 1 },
      onDegraded: (e) => oldEvents.push(e),
    },
  );

  bus.publish('t', 'a');
  await flush();
  assert.equal(oldEvents.length, 1, 'first failure degrades into the old callback');
  assert.equal(bus.subscriberHealth(sub.id).degraded, true);

  // Revising while degraded is allowed: the counter and the flag stay.
  bus.setDegradedCallback(sub.id, (e) => newEvents.push(e));
  assert.deepEqual(bus.subscriberHealth(sub.id), {
    subscriberId: sub.id,
    enabled: true,
    degraded: true,
    consecutiveFailures: 1,
  });

  assert.equal(bus.resume(sub.id), true);
  fail = false;
  bus.publish('t', 'b');
  await flush();
  assert.deepEqual(bus.subscriberHealth(sub.id), {
    subscriberId: sub.id,
    enabled: true,
    degraded: false,
    consecutiveFailures: 0,
  });

  fail = true;
  bus.publish('t', 'c');
  await flush();
  assert.equal(newEvents.length, 1, 'the next degradation fires the swapped callback');
  assert.equal(oldEvents.length, 1, 'the old callback stays retired through resume');
  sub.unsubscribe();
});
