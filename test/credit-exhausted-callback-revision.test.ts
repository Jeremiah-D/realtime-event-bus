import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type CreditExhaustedEvent } from '../src/bus.ts';

/**
 * Credit-exhaustion callback revision (EB-438):
 * `setCreditExhaustedCallback` swaps a credit-granted subscriber's
 * `onCreditExhausted` without re-subscribing — the only credit
 * revision surface `grantCredits` left uncovered: it moves the
 * balance, this one moves who hears about the balance hitting zero.
 * Only the callback moves: the balance, the `exhaustedLatch`, and the
 * `grantCredits` semantics are untouched, and a throwing callback is
 * swallowed by `fireCreditExhausted`, so observation never breaks the
 * drain or the flush loop.
 *
 * Determinism trick (as in `credits.test.ts`): grant 2, publish 5 —
 * exactly 2 are delivered, 3 stay queued, and the exhaustion edge
 * fires exactly once.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** The `credit_exhausted_callback_revised` audit events on the bus. */
function callbackRevisionAudits(bus: EventBus) {
  return bus.getSubscriptionAudit({ action: 'credit_exhausted_callback_revised' });
}

/** Drains one subscriber to exhaustion: grants 2, publishes 5. */
async function exhaustOnce(bus: EventBus, subId: string): Promise<void> {
  bus.grantCredits(subId, 2);
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
}

test('swapping the callback routes the next exhaustion to the new callback only', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const first: CreditExhaustedEvent[] = [];
  const second: CreditExhaustedEvent[] = [];
  const sub = bus.subscribe('t', (m) => received.push(m.payload), {
    onCreditExhausted: (e) => first.push(e),
  });
  await exhaustOnce(bus, sub.id);
  assert.deepEqual(received, [0, 1]);
  assert.equal(first.length, 1);
  assert.equal(first[0].subscriberId, sub.id);
  assert.equal(first[0].pattern, 't');
  assert.equal(first[0].pendingMessages, 3);
  assert.equal(bus.getStats().creditExhaustedSubscribers, 1);

  // Swap in the second callback and re-arm with a fresh grant: the
  // next exhaustion reaches it only, with exact event fields.
  bus.setCreditExhaustedCallback(sub.id, (e) => second.push(e));
  bus.grantCredits(sub.id, 2);
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3]);
  assert.equal(first.length, 1, 'the old callback never fires again');
  assert.equal(second.length, 1);
  assert.equal(second[0].subscriberId, sub.id);
  assert.equal(second[0].pattern, 't');
  assert.equal(second[0].pendingMessages, 1);
  assert.deepEqual(callbackRevisionAudits(bus)[0].detail, {
    hasCallback: true,
    oldHasCallback: true,
  });
  sub.unsubscribe();
});

test('clearing with null stops delivery of the alert but exhaustion still pauses delivery', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const events: CreditExhaustedEvent[] = [];
  const sub = bus.subscribe('t', (m) => received.push(m.payload), {
    onCreditExhausted: (e) => events.push(e),
  });
  await exhaustOnce(bus, sub.id);
  assert.deepEqual(received, [0, 1]);
  assert.equal(events.length, 1);

  // Clear the callback, then re-arm with a fresh grant: the backlog
  // resumes, the balance hits zero again, but no callback fires.
  bus.setCreditExhaustedCallback(sub.id, null);
  bus.grantCredits(sub.id, 2);
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3]);
  assert.equal(events.length, 1, 'no callback fires after clearing');
  assert.equal(bus.getStats().creditExhaustedSubscribers, 1, 'the latch still walks');
  assert.deepEqual(callbackRevisionAudits(bus)[0].detail, {
    hasCallback: false,
    oldHasCallback: true,
  });
  sub.unsubscribe();
});

test('setCreditExhaustedCallback rejects unknown, empty, ungranted, and invalid revisions fail-closed', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const events: CreditExhaustedEvent[] = [];
  const sub = bus.subscribe('t', (m) => received.push(m.payload), {
    onCreditExhausted: (e) => events.push(e),
  });
  // Granted, so this subscriber can be revised; the plain one below
  // never received a grant and refuses.
  bus.grantCredits(sub.id, 2);
  const plain = bus.subscribe('t2', () => {});
  const replacement = (_e: CreditExhaustedEvent): void => {};
  for (const bad of [
    () => bus.setCreditExhaustedCallback('no-such-sub', replacement),
    () => bus.setCreditExhaustedCallback('', replacement),
    () => bus.setCreditExhaustedCallback(123 as never, replacement),
    () => bus.setCreditExhaustedCallback(plain.id, replacement),
    () => bus.setCreditExhaustedCallback(sub.id, 'nope' as never),
    () => bus.setCreditExhaustedCallback(sub.id, 42 as never),
    () => bus.setCreditExhaustedCallback(sub.id, {} as never),
    () => bus.setCreditExhaustedCallback(sub.id, [] as never),
    () => bus.setCreditExhaustedCallback(sub.id, undefined as never),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(
    callbackRevisionAudits(bus).length,
    0,
    'failed revisions must not audit',
  );
  // The live callback, the balance, and the latch are untouched: the
  // original callback still fires on the next exhaustion.
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0, 1]);
  assert.equal(events.length, 1);
  assert.equal(events[0].pendingMessages, 3);
  sub.unsubscribe();
  plain.unsubscribe();
});

test('a revision identical to the live callback is a no-op with no audit', () => {
  const bus = new EventBus();
  const handler = (_e: CreditExhaustedEvent): void => {};
  const sub = bus.subscribe('t', () => {}, {
    onCreditExhausted: handler,
  });
  bus.grantCredits(sub.id, 1);
  // Re-installing the same reference audits nothing.
  bus.setCreditExhaustedCallback(sub.id, handler);
  assert.equal(
    callbackRevisionAudits(bus).length,
    0,
    'a same-reference revision must not audit',
  );
  // Clearing a callback that was never set is likewise a no-op.
  const bare = bus.subscribe('t2', () => {});
  bus.grantCredits(bare.id, 1);
  bus.setCreditExhaustedCallback(bare.id, null);
  assert.equal(
    callbackRevisionAudits(bus).length,
    0,
    'clearing an absent callback must not audit',
  );
  sub.unsubscribe();
  bare.unsubscribe();
});

test('a throwing callback is swallowed and the delivery path is unaffected', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const events: CreditExhaustedEvent[] = [];
  const sub = bus.subscribe('t', (m) => received.push(m.payload), {
    onCreditExhausted: (e) => events.push(e),
  });
  await exhaustOnce(bus, sub.id);
  assert.deepEqual(received, [0, 1]);
  assert.equal(events.length, 1);

  // Swap in a throwing callback and add a second subscriber on the
  // same topic: the throw must not break this subscriber's drain, the
  // flush loop, or the other subscriber's delivery.
  bus.setCreditExhaustedCallback(sub.id, () => {
    throw new Error('boom');
  });
  const otherReceived: unknown[] = [];
  const other = bus.subscribe('t', (m) => otherReceived.push(m.payload));
  bus.grantCredits(sub.id, 2);
  for (let i = 5; i < 10; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3], 'the throwing callback never broke the drain');
  assert.deepEqual(otherReceived, [5, 6, 7, 8, 9], 'the flush loop still reached the other subscriber');
  assert.equal(events.length, 1, 'the old callback never fires again');
  assert.equal(bus.getStats().creditExhaustedSubscribers, 1, 'the latch still set');
  sub.unsubscribe();
  other.unsubscribe();
});

test('the swap audits exact presence flags with the subscriber', () => {
  const bus = new EventBus();
  const first = (_e: CreditExhaustedEvent): void => {};
  const second = (_e: CreditExhaustedEvent): void => {};
  const sub = bus.subscribe('t', () => {});
  bus.grantCredits(sub.id, 1);
  bus.setCreditExhaustedCallback(sub.id, first);
  bus.setCreditExhaustedCallback(sub.id, second);
  bus.setCreditExhaustedCallback(sub.id, null);
  const audit = callbackRevisionAudits(bus);
  assert.equal(audit.length, 3);
  assert.deepEqual(audit[0].detail, { hasCallback: true, oldHasCallback: false });
  assert.deepEqual(audit[1].detail, { hasCallback: true, oldHasCallback: true });
  assert.deepEqual(audit[2].detail, { hasCallback: false, oldHasCallback: true });
  for (const entry of audit) {
    assert.equal(entry.subId, sub.id);
    assert.equal(entry.pattern, 't');
    assert.equal(entry.actor, 'system');
  }
  // The function itself is never recorded — only its presence.
  assert.ok(!JSON.stringify(audit).includes('first'));
  sub.unsubscribe();
});

test('swapping inside an exhausted episode does not re-fire; the next grant reaches the new callback', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const first: CreditExhaustedEvent[] = [];
  const second: CreditExhaustedEvent[] = [];
  const sub = bus.subscribe('t', (m) => received.push(m.payload), {
    onCreditExhausted: (e) => first.push(e),
  });
  await exhaustOnce(bus, sub.id);
  assert.equal(first.length, 1);

  // Swap while the latch is set: more arrivals queue behind the zero
  // balance without firing anything.
  bus.setCreditExhaustedCallback(sub.id, (e) => second.push(e));
  for (let i = 5; i < 7; i++) bus.publish('t', i);
  await flush();
  assert.equal(first.length, 1);
  assert.equal(second.length, 0, 'no re-fire while the latch is set');
  assert.equal(bus.getStats().creditExhaustedSubscribers, 1);

  // A fresh grant re-arms the latch; the next exhaustion fires the
  // new callback only.
  bus.grantCredits(sub.id, 2);
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3]);
  assert.equal(first.length, 1, 'the old callback never fires again');
  assert.equal(second.length, 1);
  assert.equal(second[0].pendingMessages, 3);
  sub.unsubscribe();
});

test('grantCredits balance semantics are unaffected by a callback revision', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const events: CreditExhaustedEvent[] = [];
  const sub = bus.subscribe('t', (m) => received.push(m.payload), {
    onCreditExhausted: (e) => events.push(e),
  });
  bus.grantCredits(sub.id, 2);
  // The callback swap moves nothing else: the grant still accumulates.
  const replacement: CreditExhaustedEvent[] = [];
  bus.setCreditExhaustedCallback(sub.id, (e) => replacement.push(e));
  assert.equal(bus.grantCredits(sub.id, 3), 5, 'the balance survives the callback revision');
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3, 4]);
  assert.equal(events.length, 0, 'the old callback never fires again');
  assert.equal(replacement.length, 1, 'the new callback fires on the exhaustion');
  assert.equal(replacement[0].pendingMessages, 0);
  sub.unsubscribe();
});
