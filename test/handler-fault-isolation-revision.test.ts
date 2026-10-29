import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type QuarantinedEvent } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

test('setHandlerFaultIsolation tightening trips on the next failure, at the new budget', async () => {
  const bus = new EventBus();
  const events: QuarantinedEvent[] = [];
  const sub = bus.subscribe(
    't',
    () => {
      throw new Error('boom');
    },
    {
      handlerFaultIsolation: {
        maxConsecutiveErrors: 5,
        onQuarantined: (e) => events.push(e),
      },
    },
  );

  bus.publish('t', 'a'); // failure 1 of 5
  await flush();
  assert.equal(bus.getStats().quarantinedSubscribers, 0);

  bus.setHandlerFaultIsolation(sub.id, { maxConsecutiveErrors: 2 });
  bus.publish('t', 'b'); // failure 2 — at the tightened budget
  await flush();
  assert.equal(events.length, 1);
  assert.equal(events[0].consecutiveErrors, 2);
  assert.equal(bus.getStats().quarantinedSubscribers, 1);

  const audit = bus.getSubscriptionAudit({ action: 'handler_fault_isolation_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    handlerFaultIsolation: { maxConsecutiveErrors: 2, onQuarantined: 'set' },
    oldHandlerFaultIsolation: { maxConsecutiveErrors: 5, onQuarantined: 'set' },
  });
  sub.unsubscribe();
});

test('setHandlerFaultIsolation loosening requires more failures to trip', async () => {
  const bus = new EventBus();
  const events: QuarantinedEvent[] = [];
  const sub = bus.subscribe(
    't',
    () => {
      throw new Error('boom');
    },
    {
      handlerFaultIsolation: {
        maxConsecutiveErrors: 2,
        onQuarantined: (e) => events.push(e),
      },
    },
  );

  bus.publish('t', 'a'); // failure 1 of 2
  await flush();
  assert.equal(bus.getStats().quarantinedSubscribers, 0);

  bus.setHandlerFaultIsolation(sub.id, { maxConsecutiveErrors: 4 });
  bus.publish('t', 'b');
  bus.publish('t', 'c'); // failures 2 and 3 of 4
  await flush();
  assert.equal(events.length, 0, 'the loosened budget has not been reached yet');
  assert.equal(bus.getStats().quarantinedSubscribers, 0);

  bus.publish('t', 'd'); // failure 4 of 4
  await flush();
  assert.equal(events.length, 1);
  assert.equal(events[0].consecutiveErrors, 4);
  sub.unsubscribe();
});

test('setHandlerFaultIsolation partial revision keeps the callback, and a revised callback replaces it', async () => {
  const bus = new EventBus();
  const oldEvents: QuarantinedEvent[] = [];
  const newEvents: QuarantinedEvent[] = [];
  const sub = bus.subscribe(
    't',
    () => {
      throw new Error('boom');
    },
    {
      handlerFaultIsolation: {
        maxConsecutiveErrors: 3,
        onQuarantined: (e) => oldEvents.push(e),
      },
    },
  );

  bus.publish('t', 'a'); // failure 1
  await flush();

  // Partial revision: budget only — the callback is kept.
  bus.setHandlerFaultIsolation(sub.id, { maxConsecutiveErrors: 3 });
  // Same budget, so this is a no-op even though the object is new.
  let audit = bus.getSubscriptionAudit({ action: 'handler_fault_isolation_revised' });
  assert.equal(audit.length, 0, 'restating the budget alone changes nothing');

  bus.setHandlerFaultIsolation(sub.id, { onQuarantined: (e) => newEvents.push(e) });
  audit = bus.getSubscriptionAudit({ action: 'handler_fault_isolation_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    handlerFaultIsolation: { maxConsecutiveErrors: 3, onQuarantined: 'set' },
    oldHandlerFaultIsolation: { maxConsecutiveErrors: 3, onQuarantined: 'set' },
  });

  bus.publish('t', 'b');
  bus.publish('t', 'c'); // failures 2 and 3 — quarantine
  await flush();
  assert.equal(oldEvents.length, 0, 'the replaced callback never fires again');
  assert.equal(newEvents.length, 1);
  assert.equal(newEvents[0].consecutiveErrors, 3);
  sub.unsubscribe();
});

test('setHandlerFaultIsolation preserves the consecutive-error count', async () => {
  const bus = new EventBus();
  const events: QuarantinedEvent[] = [];
  const sub = bus.subscribe(
    't',
    () => {
      throw new Error('boom');
    },
    {
      handlerFaultIsolation: {
        maxConsecutiveErrors: 5,
        onQuarantined: (e) => events.push(e),
      },
    },
  );

  for (let i = 0; i < 4; i += 1) {
    bus.publish('t', i);
  }
  await flush();
  assert.equal(bus.getStats().quarantinedSubscribers, 0, '4 of 5 failures so far');

  // A callback-only revision must not reset the counter: the next
  // failure is the 5th consecutive one, and it must quarantine.
  bus.setHandlerFaultIsolation(sub.id, { onQuarantined: (e) => events.push(e) });
  bus.publish('t', 'x');
  await flush();
  assert.equal(events.length, 1);
  assert.equal(events[0].consecutiveErrors, 5);
  sub.unsubscribe();
});

test('setHandlerFaultIsolation on a quarantined subscriber preserves the latch; only resume clears it', async () => {
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
        maxConsecutiveErrors: 2,
        onQuarantined: (e) => events.push(e),
      },
    },
  );

  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  assert.equal(events.length, 1);
  assert.equal(bus.getStats().quarantinedSubscribers, 1);

  // Revising the budget of a quarantined subscriber neither
  // un-quarantines it nor re-fires the callback.
  bus.setHandlerFaultIsolation(sub.id, { maxConsecutiveErrors: 5 });
  assert.equal(bus.getStats().quarantinedSubscribers, 1);
  assert.equal(events.length, 1, 'no repeat quarantine event');
  bus.publish('t', 'c');
  await flush();
  assert.equal(calls, 2, 'delivery stays paused while quarantined');

  // Recovery is via resumeQuarantined only — and it resets the count.
  assert.equal(bus.resumeQuarantined(sub.id), true);
  fail = false;
  await flush();
  assert.equal(calls, 3, 'the preserved backlog redelivers on resume');
  bus.publish('t', 'd');
  await flush();
  assert.equal(calls, 4);
  sub.unsubscribe();
});

test('setHandlerFaultIsolation null clears the isolation, and revision after a clear is refused', async () => {
  const bus = new EventBus();
  let calls = 0;
  const sub = bus.subscribe(
    't',
    () => {
      calls += 1;
    },
    { handlerFaultIsolation: { maxConsecutiveErrors: 2 } },
  );

  bus.setHandlerFaultIsolation(sub.id, null);
  const audit = bus.getSubscriptionAudit({ action: 'handler_fault_isolation_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    handlerFaultIsolation: null,
    oldHandlerFaultIsolation: { maxConsecutiveErrors: 2 },
  });

  // Delivery proceeds without the isolation (and without any
  // quarantine accounting): a cleared subscriber is simply untracked.
  for (let i = 0; i < 5; i += 1) bus.publish('t', i);
  await flush();
  assert.equal(calls, 5);
  assert.equal(bus.getStats().quarantinedSubscribers, 0);

  assert.throws(() => bus.setHandlerFaultIsolation(sub.id, { maxConsecutiveErrors: 3 }), RangeError);
  assert.throws(() => bus.setHandlerFaultIsolation(sub.id, null), RangeError);
  sub.unsubscribe();
});

test('setHandlerFaultIsolation null on a quarantined subscriber is refused until it is resumed', async () => {
  const bus = new EventBus();
  const sub = bus.subscribe(
    't',
    () => {
      throw new Error('boom');
    },
    { handlerFaultIsolation: { maxConsecutiveErrors: 1 } },
  );

  bus.publish('t', 'a');
  await flush();
  assert.equal(bus.getStats().quarantinedSubscribers, 1);

  assert.throws(() => bus.setHandlerFaultIsolation(sub.id, null), RangeError);
  assert.equal(bus.getStats().quarantinedSubscribers, 1, 'the refused clear left the quarantine intact');
  assert.equal(
    bus.getSubscriptionAudit({ action: 'handler_fault_isolation_revised' }).length,
    0,
    'no failed clear is audited',
  );

  // After a resume, the clear succeeds — the quarantine audit trail
  // was not bypassed, it was completed first.
  assert.equal(bus.resumeQuarantined(sub.id), true);
  bus.setHandlerFaultIsolation(sub.id, null);
  assert.equal(bus.getSubscriptionAudit({ action: 'handler_fault_isolation_revised' }).length, 1);
  sub.unsubscribe();
});

test('setHandlerFaultIsolation identical revision is a no-op with no audit', () => {
  const bus = new EventBus();
  const cb = () => {};
  const sub = bus.subscribe('t', () => {}, {
    handlerFaultIsolation: { maxConsecutiveErrors: 3, onQuarantined: cb },
  });
  bus.setHandlerFaultIsolation(sub.id, { maxConsecutiveErrors: 3 });
  bus.setHandlerFaultIsolation(sub.id, { onQuarantined: cb });
  bus.setHandlerFaultIsolation(sub.id, {});
  assert.equal(
    bus.getSubscriptionAudit({ action: 'handler_fault_isolation_revised' }).length,
    0,
    'identical revisions (including an empty object) audit nothing',
  );
  sub.unsubscribe();
});

test('setHandlerFaultIsolation is fail-closed for unknown, isolation-less, or illegal values', () => {
  const bus = new EventBus();
  const isolated = bus.subscribe('t', () => {}, {
    handlerFaultIsolation: { maxConsecutiveErrors: 3, onQuarantined: () => {} },
  });
  const plain = bus.subscribe('u', () => {});

  const bad: Array<() => void> = [
    () => bus.setHandlerFaultIsolation('', { maxConsecutiveErrors: 2 }),
    () => bus.setHandlerFaultIsolation('sub-nope', { maxConsecutiveErrors: 2 }),
    () => bus.setHandlerFaultIsolation(plain.id, { maxConsecutiveErrors: 2 }),
    () => bus.setHandlerFaultIsolation(isolated.id, undefined as never),
    () => bus.setHandlerFaultIsolation(isolated.id, 'x' as never),
    () => bus.setHandlerFaultIsolation(isolated.id, [] as never),
    () => bus.setHandlerFaultIsolation(isolated.id, { maxConsecutiveErrors: 0 }),
    () => bus.setHandlerFaultIsolation(isolated.id, { maxConsecutiveErrors: -1 }),
    () => bus.setHandlerFaultIsolation(isolated.id, { maxConsecutiveErrors: 2.5 }),
    () => bus.setHandlerFaultIsolation(isolated.id, { maxConsecutiveErrors: Number.NaN }),
    () => bus.setHandlerFaultIsolation(isolated.id, { onQuarantined: 'x' as never }),
  ];
  for (const call of bad) assert.throws(call, RangeError);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'handler_fault_isolation_revised' }).length,
    0,
    'no failed revision is audited',
  );

  // The live parameters survived every failed revision.
  bus.setHandlerFaultIsolation(isolated.id, { maxConsecutiveErrors: 4 });
  const audit = bus.getSubscriptionAudit({ action: 'handler_fault_isolation_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail.oldHandlerFaultIsolation, {
    maxConsecutiveErrors: 3,
    onQuarantined: 'set',
  });
  isolated.unsubscribe();
  plain.unsubscribe();
});
