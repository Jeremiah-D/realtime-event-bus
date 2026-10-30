import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const collect = (bus: EventBus, pattern: string, opts?: Parameters<EventBus['subscribe']>[2]) => {
  const received: BusMessage[] = [];
  const sub = bus.subscribe(pattern, (msg) => received.push(msg), opts);
  return { received, sub };
};

const payloads = (msgs: BusMessage[]) => msgs.map((m) => m.payload);

test('setKeyRange rejects unknown, empty, unset-range, and invalid revisions fail-closed', async () => {
  const bus = new EventBus();
  const { received, sub } = collect(bus, 't', { keyRange: { start: 'b', end: 'd' } });
  const plain = bus.subscribe('t2', () => {});
  bus.publish('t', 'b-msg', { key: 'b' });
  bus.publish('t', 'a-msg', { key: 'a' });
  await flush();
  assert.deepEqual(payloads(received), ['b-msg']);
  for (const bad of [
    () => bus.setKeyRange('no-such-sub', { start: 'a', end: 'z' }),
    () => bus.setKeyRange('', { start: 'a', end: 'z' }),
    () => bus.setKeyRange(plain.id, { start: 'a', end: 'z' }),
    () => bus.setKeyRange(sub.id, undefined as never),
    () => bus.setKeyRange(sub.id, 'a-z' as never),
    () => bus.setKeyRange(sub.id, 42 as never),
    () => bus.setKeyRange(sub.id, { start: 1 as never, end: 'z' }),
    () => bus.setKeyRange(sub.id, { start: 'a', end: 2 as never }),
    () => bus.setKeyRange(sub.id, { start: 'd', end: 'd' }),
    () => bus.setKeyRange(sub.id, { start: 'z', end: 'a' }),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(
    bus.getSubscriptionAudit({ action: 'key_range_revised' }).length,
    0,
    'failed revisions must not audit',
  );
  // The live range is untouched: 'a' is still skipped, 'b' delivered.
  bus.publish('t', 'a-msg-2', { key: 'a' });
  bus.publish('t', 'c-msg', { key: 'c' });
  await flush();
  assert.deepEqual(payloads(received), ['b-msg', 'c-msg']);
  assert.equal(bus.getStats().sequenceGaps, 0);
  sub.unsubscribe();
  plain.unsubscribe();
});

test('setKeyRange narrowing governs the next delivery, already-delivered messages untouched', async () => {
  const bus = new EventBus();
  const { received, sub } = collect(bus, 't', { keyRange: { start: 'a', end: 'z' } });
  bus.publish('t', 'm-msg', { key: 'm' });
  await flush();
  assert.deepEqual(payloads(received), ['m-msg']);
  bus.setKeyRange(sub.id, { start: 'a', end: 'm' });
  bus.publish('t', 'm-msg-2', { key: 'm' });
  bus.publish('t', 'b-msg', { key: 'b' });
  await flush();
  assert.deepEqual(payloads(received), ['m-msg', 'b-msg']);
  assert.equal(bus.getStats().sequenceGaps, 0);
  const audit = bus.getSubscriptionAudit({ action: 'key_range_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    keyRange: { start: 'a', end: 'm' },
    oldKeyRange: { start: 'a', end: 'z' },
  });
  sub.unsubscribe();
});

test('setKeyRange widening admits previously skipped keys', async () => {
  const bus = new EventBus();
  const { received, sub } = collect(bus, 't', { keyRange: { start: 'm', end: 'q' } });
  bus.publish('t', 'a-msg', { key: 'a' });
  await flush();
  assert.deepEqual(payloads(received), []);
  bus.setKeyRange(sub.id, { start: 'a' });
  bus.publish('t', 'a-msg-2', { key: 'a' });
  bus.publish('t', 'z-msg', { key: 'z' });
  await flush();
  assert.deepEqual(payloads(received), ['a-msg-2', 'z-msg']);
  sub.unsubscribe();
});

test('setKeyRange null clears back to no narrowing: keyless delivery resumes, and a cleared range refuses further revision', async () => {
  const bus = new EventBus();
  const { received, sub } = collect(bus, 't', { keyRange: { start: 'b', end: 'd' } });
  bus.publish('t', 'keyless-msg');
  await flush();
  assert.deepEqual(payloads(received), []);
  bus.setKeyRange(sub.id, null);
  bus.publish('t', 'keyless-msg-2');
  bus.publish('t', 'a-msg', { key: 'a' });
  await flush();
  assert.deepEqual(payloads(received), ['keyless-msg-2', 'a-msg']);
  const audit = bus.getSubscriptionAudit({ action: 'key_range_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    keyRange: null,
    oldKeyRange: { start: 'b', end: 'd' },
  });
  // A cleared range is no longer revisable until re-subscribe.
  assert.throws(() => bus.setKeyRange(sub.id, { start: 'a', end: 'z' }), RangeError);
  sub.unsubscribe();
});

test('setKeyRange identical revision is a no-op with no audit, and {} is a real range distinct from a clear', async () => {
  const bus = new EventBus();
  const { received, sub } = collect(bus, 't', { keyRange: {} });
  bus.publish('t', 'a-msg', { key: 'a' });
  bus.publish('t', 'keyless-msg');
  await flush();
  assert.deepEqual(payloads(received), ['a-msg']);
  bus.setKeyRange(sub.id, {});
  assert.equal(bus.getSubscriptionAudit({ action: 'key_range_revised' }).length, 0);
  // {} still governs: keyed delivered, keyless still excluded.
  bus.publish('t', 'z-msg', { key: 'z' });
  bus.publish('t', 'keyless-msg-2');
  await flush();
  assert.deepEqual(payloads(received), ['a-msg', 'z-msg']);
  // Revising {} to an equal-by-fields object is also a no-op.
  bus.setKeyRange(sub.id, { start: undefined, end: undefined });
  assert.equal(bus.getSubscriptionAudit({ action: 'key_range_revised' }).length, 0);
  sub.unsubscribe();
});

test('setKeyRange copies its input: later mutations of the caller object do not move the subscription', async () => {
  const bus = new EventBus();
  const { received, sub } = collect(bus, 't', { keyRange: { start: 'b', end: 'd' } });
  const range: { start: string; end: string } = { start: 'a', end: 'z' };
  bus.setKeyRange(sub.id, range);
  range.start = 'q';
  range.end = 'r';
  // 'b' is inside the installed [a, z) but outside the mutated [q, r):
  // delivered only if the revision copied its input.
  bus.publish('t', 'b-msg', { key: 'b' });
  bus.publish('t', 'q-msg', { key: 'q' });
  await flush();
  assert.deepEqual(payloads(received), ['b-msg', 'q-msg']);
  sub.unsubscribe();
});
