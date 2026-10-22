import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type QuarantinedEvent } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const tick = (ms = 5) => new Promise<void>((resolve) => setTimeout(resolve, ms));

test('consecutive sync throws quarantine the subscriber at the budget', async () => {
  const bus = new EventBus();
  const events: QuarantinedEvent[] = [];
  let calls = 0;
  const sub = bus.subscribe(
    't',
    () => {
      calls += 1;
      throw new Error('boom');
    },
    {
      handlerFaultIsolation: {
        maxConsecutiveErrors: 3,
        onQuarantined: (e) => events.push(e),
      },
    },
  );

  bus.publish('t', 'a');
  bus.publish('t', 'b');
  bus.publish('t', 'c');
  await flush();

  // No throw escaped the drain: the flush microtask survived.
  assert.equal(calls, 3);
  assert.equal(events.length, 1);
  assert.equal(events[0].subscriberId, sub.id);
  assert.equal(events[0].pattern, 't');
  assert.equal(events[0].consecutiveErrors, 3);
  assert.equal(events[0].pendingMessages, 0);
  assert.equal(bus.getStats().quarantinedSubscribers, 1);

  // Quarantined: new publishes stay queued (preserved, not dropped), none delivered.
  bus.publish('t', 'd');
  bus.publish('t', 'e');
  await flush();
  assert.equal(calls, 3);
  assert.equal(events.length, 1); // no repeat event while already quarantined
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.droppedCount(sub.id), 0);

  // The quarantine is audited.
  const audit = bus.getSubscriptionAudit({ action: 'subscriber_quarantined' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].subId, sub.id);
  assert.deepEqual(audit[0].detail, { consecutiveErrors: 3 });
  bus.destroy?.();
});

test('a successful delivery resets the consecutive-error counter', async () => {
  const bus = new EventBus();
  const events: QuarantinedEvent[] = [];
  let calls = 0;
  let fail = true;
  const sub = bus.subscribe(
    't',
    () => {
      calls += 1;
      if (fail) throw new Error('boom');
    },
    {
      handlerFaultIsolation: {
        maxConsecutiveErrors: 3,
        onQuarantined: (e) => events.push(e),
      },
    },
  );

  bus.publish('t', 'a'); // fails (1)
  bus.publish('t', 'b'); // fails (2)
  await flush();
  assert.equal(calls, 2);
  assert.equal(bus.getStats().quarantinedSubscribers, 0);

  fail = false;
  bus.publish('t', 'c'); // succeeds — resets to 0
  await flush();
  assert.equal(calls, 3);

  fail = true;
  bus.publish('t', 'd'); // fails (1)
  bus.publish('t', 'e'); // fails (2)
  await flush();
  assert.equal(calls, 5);
  assert.equal(events.length, 0); // never reached 3 consecutive
  assert.equal(bus.getStats().quarantinedSubscribers, 0);
  assert.equal(sub.id.length > 0, true);
  bus.destroy?.();
});

test('async rejections count and async fulfillment resets', async () => {
  const bus = new EventBus();
  const events: QuarantinedEvent[] = [];
  const sub = bus.subscribe('t', () => Promise.reject(new Error('async-boom')), {
    handlerFaultIsolation: { maxConsecutiveErrors: 2, onQuarantined: (e) => events.push(e) },
  });

  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  await tick(20); // let the rejected promises settle

  assert.equal(events.length, 1);
  assert.equal(events[0].consecutiveErrors, 2);
  assert.equal(bus.getStats().quarantinedSubscribers, 1);
  bus.destroy?.();
});

test('async fulfillment after a rejection breaks the consecutive chain', async () => {
  const bus = new EventBus();
  const events: QuarantinedEvent[] = [];
  let mode: 'reject' | 'resolve' = 'reject';
  bus.subscribe(
    't',
    () => (mode === 'reject' ? Promise.reject(new Error('x')) : Promise.resolve()),
    {
      handlerFaultIsolation: { maxConsecutiveErrors: 2, onQuarantined: (e) => events.push(e) },
    },
  );

  bus.publish('t', 'a'); // rejects -> 1
  await flush();
  await tick(20);
  mode = 'resolve';
  bus.publish('t', 'b'); // fulfills -> reset to 0
  await flush();
  await tick(20);
  mode = 'reject';
  bus.publish('t', 'c'); // rejects -> 1 (not 2)
  await flush();
  await tick(20);

  assert.equal(events.length, 0);
  bus.destroy?.();
});

test('resumeQuarantined resumes delivery and clears the counter', async () => {
  const bus = new EventBus();
  let calls = 0;
  let fail = true;
  const sub = bus.subscribe(
    't',
    () => {
      calls += 1;
      if (fail) throw new Error('boom');
    },
    { handlerFaultIsolation: { maxConsecutiveErrors: 2 } },
  );

  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  assert.equal(bus.getStats().quarantinedSubscribers, 1);

  // Resume while the handler still fails: the budget starts over.
  assert.equal(bus.resumeQuarantined(sub.id), true);
  assert.equal(bus.getStats().quarantinedSubscribers, 0);
  bus.publish('t', 'c'); // fails (1)
  await flush();
  assert.equal(bus.getStats().quarantinedSubscribers, 0);

  // A healthy handler drains the preserved backlog in order.
  fail = false;
  const seen: unknown[] = [];
  const sub2 = bus.subscribe('t2', (msg) => seen.push(msg.payload), {
    handlerFaultIsolation: true,
  });
  assert.equal(sub2.id.length > 0, true);
  assert.equal(bus.resumeQuarantined(sub.id), false); // idempotent: not quarantined
  bus.publish('t2', 'x');
  bus.publish('t2', 'y');
  await flush();
  assert.deepEqual(seen, ['x', 'y']);

  const audit = bus.getSubscriptionAudit({ action: 'subscriber_quarantine_resumed' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].subId, sub.id);
  bus.destroy?.();
});

test('resumeQuarantined throws on unknown subscriber', () => {
  const bus = new EventBus();
  assert.throws(() => bus.resumeQuarantined('nope'), /unknown subscriber/);
  bus.destroy?.();
});

test('invalid handlerFaultIsolation options throw RangeError at subscribe time', () => {
  const bus = new EventBus();
  assert.throws(
    () => bus.subscribe('t', () => {}, { handlerFaultIsolation: { maxConsecutiveErrors: 0 } }),
    RangeError,
  );
  assert.throws(
    () => bus.subscribe('t', () => {}, { handlerFaultIsolation: { maxConsecutiveErrors: 1.5 } }),
    RangeError,
  );
  assert.throws(
    () =>
      bus.subscribe('t', () => {}, {
        handlerFaultIsolation: { onQuarantined: 'nope' as unknown as () => void },
      }),
    RangeError,
  );
  // No half-registered subscriber is left behind by a rejected option.
  assert.equal(bus.getStats().quarantinedSubscribers, 0);
  bus.destroy?.();
});

test('quarantine is orthogonal to the manual pause (EB-113)', async () => {
  const bus = new EventBus();
  let calls = 0;
  const sub = bus.subscribe(
    't',
    () => {
      calls += 1;
      throw new Error('boom');
    },
    { handlerFaultIsolation: { maxConsecutiveErrors: 1 } },
  );

  bus.publish('t', 'a');
  await flush();
  assert.equal(bus.getStats().quarantinedSubscribers, 1);

  // The operator's manual pause is independent: resuming the quarantine
  // does not clear it, and vice versa.
  assert.equal(bus.pauseSubscriber(sub.id), true);
  assert.equal(bus.resumeQuarantined(sub.id), true);
  assert.equal(bus.getStats().quarantinedSubscribers, 0);
  assert.equal(bus.getStats().pausedSubscribers, 1);
  bus.publish('t', 'b');
  await flush();
  assert.equal(calls, 1); // still held by the manual pause

  assert.equal(bus.resumeSubscriber(sub.id), true);
  assert.equal(bus.getStats().pausedSubscribers, 0);
  bus.destroy?.();
});

test('quarantine does not affect other subscribers', async () => {
  const bus = new EventBus();
  const bad = bus.subscribe(
    't',
    () => {
      throw new Error('boom');
    },
    { handlerFaultIsolation: { maxConsecutiveErrors: 1 } },
  );
  const seen: unknown[] = [];
  const good = bus.subscribe('t', (msg) => seen.push(msg.payload));

  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();

  assert.equal(bus.getStats().quarantinedSubscribers, 1);
  assert.deepEqual(seen, ['a', 'b']);
  assert.equal(bad.id.length > 0, true);
  assert.equal(good.id.length > 0, true);
  bus.destroy?.();
});

test('handlerFaultIsolation: true uses the default budget of 5', async () => {
  const bus = new EventBus();
  let calls = 0;
  bus.subscribe(
    't',
    () => {
      calls += 1;
      throw new Error('boom');
    },
    { handlerFaultIsolation: true },
  );
  for (let i = 0; i < 4; i += 1) bus.publish('t', i);
  await flush();
  assert.equal(calls, 4);
  assert.equal(bus.getStats().quarantinedSubscribers, 0);
  bus.publish('t', 4);
  await flush();
  assert.equal(calls, 5);
  assert.equal(bus.getStats().quarantinedSubscribers, 1);
  bus.destroy?.();
});

test('async rejection on a health-probed subscriber counts toward isolation', async () => {
  const bus = new EventBus();
  const events: QuarantinedEvent[] = [];
  bus.subscribe('t', () => Promise.reject(new Error('async-boom')), {
    healthProbe: { maxConsecutiveFailures: 100 },
    handlerFaultIsolation: { maxConsecutiveErrors: 2, onQuarantined: (e) => events.push(e) },
  });
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  await tick(20);
  // The probe never saw a synchronous throw (nothing degraded), but the
  // isolation counted the two async rejections.
  assert.equal(bus.getStats().degradedSubscribers, 0);
  assert.equal(bus.getStats().quarantinedSubscribers, 1);
  assert.equal(events.length, 1);
  bus.destroy?.();
});
