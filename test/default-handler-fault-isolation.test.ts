import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type QuarantinedEvent } from '../src/bus.ts';

/**
 * Bus-level default handler-fault-isolation budget (EB-501):
 * `setDefaultHandlerFaultIsolation` installs the
 * `maxConsecutiveErrors` new opt-in `handlerFaultIsolation`
 * subscribers fall back to when they omit the budget — the priority
 * chain is subscriber-explicit > bus default > 5 — and the default
 * never enables isolation: a subscriber that does not opt into
 * `handlerFaultIsolation` is untouched by it. The default carries no
 * callback: `onQuarantined` stays the subscriber's own.
 *
 * Quarantine assertions follow `handler-fault-isolation.test.ts`: a
 * synchronously throwing handler is counted at drain time, so one
 * `flush()` settles the verdict; an async rejection needs a short
 * `tick()` for the promise to settle.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const tick = (ms = 20) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** A handler that throws synchronously on every invocation. */
function throwingHandler(counter: { calls: number }) {
  return () => {
    counter.calls += 1;
    throw new Error('boom');
  };
}

test('EB-501: the factory bus default is unset (getter undefined, clearing audits nothing)', () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultHandlerFaultIsolation(), undefined);
  bus.setDefaultHandlerFaultIsolation(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_handler_fault_isolation_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
});

test('EB-501: `true` opt-in takes the default budget', async () => {
  const bus = new EventBus();
  bus.setDefaultHandlerFaultIsolation({ maxConsecutiveErrors: 2 });
  const counter = { calls: 0 };
  const sub = bus.subscribe('t', throwingHandler(counter), { handlerFaultIsolation: true });
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  bus.publish('t', 'c');
  await flush();
  assert.equal(counter.calls, 2, 'the third message stays queued once the default budget trips');
  assert.equal(bus.getStats().quarantinedSubscribers, 1);
  assert.equal(bus.pendingCount(sub.id), 1);
});

test('EB-501: an object that omits the budget takes the default budget and fires its own callback', async () => {
  const bus = new EventBus();
  bus.setDefaultHandlerFaultIsolation({ maxConsecutiveErrors: 2 });
  const counter = { calls: 0 };
  const events: QuarantinedEvent[] = [];
  const sub = bus.subscribe('t', throwingHandler(counter), {
    handlerFaultIsolation: { onQuarantined: (e) => events.push(e) },
  });
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  bus.publish('t', 'c');
  await flush();
  assert.equal(counter.calls, 2);
  assert.equal(events.length, 1);
  assert.equal(events[0].subscriberId, sub.id);
  assert.equal(events[0].pattern, 't');
  assert.equal(events[0].consecutiveErrors, 2, 'the event carries the default budget');
  assert.equal(events[0].pendingMessages, 1);
  assert.equal(bus.getStats().quarantinedSubscribers, 1);
});

test('EB-501: an explicit budget always wins over the bus default', async () => {
  const bus = new EventBus();
  bus.setDefaultHandlerFaultIsolation({ maxConsecutiveErrors: 2 });
  const counter = { calls: 0 };
  const events: QuarantinedEvent[] = [];
  bus.subscribe('t', throwingHandler(counter), {
    handlerFaultIsolation: {
      maxConsecutiveErrors: 3,
      onQuarantined: (e) => events.push(e),
    },
  });
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  bus.publish('t', 'c');
  await flush();
  assert.equal(counter.calls, 3, 'the explicit budget of 3 governs, not the default 2');
  assert.equal(events.length, 1);
  assert.equal(events[0].consecutiveErrors, 3);
  assert.equal(bus.getStats().quarantinedSubscribers, 1);
});

test('EB-501: clearing with null returns new opt-in subscribers to the built-in budget of 5', async () => {
  const bus = new EventBus();
  bus.setDefaultHandlerFaultIsolation({ maxConsecutiveErrors: 2 });
  bus.setDefaultHandlerFaultIsolation(null);
  assert.equal(bus.getDefaultHandlerFaultIsolation(), undefined);
  const counter = { calls: 0 };
  bus.subscribe('t', throwingHandler(counter), { handlerFaultIsolation: true });
  for (let i = 0; i < 4; i += 1) bus.publish('t', i);
  await flush();
  assert.equal(counter.calls, 4);
  assert.equal(bus.getStats().quarantinedSubscribers, 0, 'not yet at the built-in 5');
  bus.publish('t', 4);
  await flush();
  assert.equal(counter.calls, 5);
  assert.equal(bus.getStats().quarantinedSubscribers, 1, 'the built-in budget of 5 trips');
  const audit = bus.getSubscriptionAudit({ action: 'default_handler_fault_isolation_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, {
    previous: { maxConsecutiveErrors: 2 },
    handlerFaultIsolation: null,
  });
});

test('EB-501: the default never enables isolation for a subscriber that does not opt in', async () => {
  const bus = new EventBus();
  bus.setDefaultHandlerFaultIsolation({ maxConsecutiveErrors: 2 });
  let calls = 0;
  // An async rejection without isolation is swallowed by the
  // slot-release machinery — it never quarantines and never crashes
  // the process, so this is the safe way to prove non-enablement.
  bus.subscribe('t', () => {
    calls += 1;
    return Promise.reject(new Error('x'));
  });
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  bus.publish('t', 'c');
  await flush();
  await tick();
  assert.equal(calls, 3, 'every message was attempted');
  assert.equal(bus.getStats().quarantinedSubscribers, 0);
});

test('EB-501: revising the bus default never moves an already-subscribed budget or counter', async () => {
  const bus = new EventBus();
  bus.setDefaultHandlerFaultIsolation({ maxConsecutiveErrors: 2 });
  const early = { calls: 0 };
  bus.subscribe('t', throwingHandler(early), { handlerFaultIsolation: true });
  bus.setDefaultHandlerFaultIsolation({ maxConsecutiveErrors: 5 });
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  assert.equal(early.calls, 2, 'the early subscriber keeps the budget it was baked with');
  assert.equal(bus.getStats().quarantinedSubscribers, 1, 'it trips at the baked 2, not the new 5');
  // A subscriber joining after the revision takes the new default.
  const late = { calls: 0 };
  bus.subscribe('u', throwingHandler(late), { handlerFaultIsolation: true });
  for (let i = 0; i < 4; i += 1) bus.publish('u', i);
  await flush();
  assert.equal(late.calls, 4);
  assert.equal(bus.getStats().quarantinedSubscribers, 1, 'the new default budget is 5');
  bus.publish('u', 4);
  await flush();
  assert.equal(late.calls, 5);
  assert.equal(bus.getStats().quarantinedSubscribers, 2);
});

test('EB-501: illegal values throw RangeError with the live default untouched and no audit', () => {
  const bus = new EventBus();
  bus.setDefaultHandlerFaultIsolation({ maxConsecutiveErrors: 2 });
  const bad = [
    0,
    -1,
    1.5,
    '3',
    [],
    true,
    { maxConsecutiveErrors: 0 },
    { maxConsecutiveErrors: -2 },
    { maxConsecutiveErrors: 2.5 },
    { maxConsecutiveErrors: '3' },
    undefined,
  ] as unknown[];
  for (const value of bad) {
    assert.throws(
      () => bus.setDefaultHandlerFaultIsolation(value as never),
      RangeError,
      `opts=${String(value)}`,
    );
    assert.deepEqual(bus.getDefaultHandlerFaultIsolation(), { maxConsecutiveErrors: 2 });
  }
  assert.throws(() => bus.setDefaultHandlerFaultIsolation({ maxConsecutiveErrors: 3 }, ''), RangeError);
  assert.deepEqual(bus.getDefaultHandlerFaultIsolation(), { maxConsecutiveErrors: 2 });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_handler_fault_isolation_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});

test('EB-501: re-affirming the default is a no-op with no audit, the getter and setter copy defensively, and the audit records previous/current exactly', () => {
  const bus = new EventBus();
  bus.setDefaultHandlerFaultIsolation(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_handler_fault_isolation_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setDefaultHandlerFaultIsolation({});
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_handler_fault_isolation_revised' }).length,
    0,
    '{} installs no field coverage, equivalent to unset',
  );
  assert.equal(bus.getDefaultHandlerFaultIsolation(), undefined);
  const opts = { maxConsecutiveErrors: 2 };
  bus.setDefaultHandlerFaultIsolation(opts);
  opts.maxConsecutiveErrors = 999;
  assert.deepEqual(
    bus.getDefaultHandlerFaultIsolation(),
    { maxConsecutiveErrors: 2 },
    'the setter stores a defensive copy',
  );
  bus.setDefaultHandlerFaultIsolation({ maxConsecutiveErrors: 2 });
  bus.setDefaultHandlerFaultIsolation({ maxConsecutiveErrors: 2 });
  const audit = bus.getSubscriptionAudit({ action: 'default_handler_fault_isolation_revised' });
  assert.equal(audit.length, 1, 're-affirming the default audits nothing');
  assert.deepEqual(audit[0].detail, {
    previous: null,
    handlerFaultIsolation: { maxConsecutiveErrors: 2 },
  });
  const held = bus.getDefaultHandlerFaultIsolation();
  assert.ok(held !== undefined);
  held.maxConsecutiveErrors = 999;
  assert.deepEqual(
    bus.getDefaultHandlerFaultIsolation(),
    { maxConsecutiveErrors: 2 },
    'the getter returns a copy',
  );
  bus.setDefaultHandlerFaultIsolation({ maxConsecutiveErrors: 3 }, 'ops');
  assert.deepEqual(bus.getDefaultHandlerFaultIsolation(), { maxConsecutiveErrors: 3 });
  const revised = bus.getSubscriptionAudit({ action: 'default_handler_fault_isolation_revised' });
  assert.equal(revised.length, 2);
  assert.deepEqual(revised[1].detail, {
    previous: { maxConsecutiveErrors: 2 },
    handlerFaultIsolation: { maxConsecutiveErrors: 3 },
  });
  assert.equal(revised[1].actor, 'ops');
});

test('EB-501: the default never enters a config snapshot', () => {
  const bus = new EventBus();
  bus.setDefaultHandlerFaultIsolation({ maxConsecutiveErrors: 2 });
  const snapshot = bus.exportBusConfig();
  assert.ok(!('handlerFaultIsolation' in snapshot), 'no top-level handlerFaultIsolation key');
  assert.ok(
    !JSON.stringify(snapshot).includes('handlerFaultIsolation'),
    'the default appears nowhere in the exported snapshot',
  );
  assert.ok(
    !JSON.stringify(snapshot).includes('defaultHandlerFaultIsolation'),
    'the field name appears nowhere in the exported snapshot',
  );
});
