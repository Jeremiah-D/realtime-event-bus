import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/**
 * Bus-level default key range (EB-508): `setDefaultKeyRange`
 * installs the `start` / `end` new opt-in `keyRange` subscribers
 * fall back to when they omit an end — the priority chain is
 * subscriber-explicit > bus default > unbounded — and the default
 * never enables a key range: a subscriber that does not opt into
 * `keyRange` still receives keyed and keyless messages as before.
 *
 * Delivery assertions follow `key-range.test.ts`: the bus flushes
 * on a microtask, so one `setImmediate` settles every publish.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

test('EB-508: the factory bus default is unset (getter undefined, clearing audits nothing)', () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultKeyRange(), undefined);
  bus.setDefaultKeyRange(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_key_range_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
});

test('EB-508: `keyRange: {}` opts in and takes the whole default — in-range delivered, out-of-range filtered, keyless excluded', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.setDefaultKeyRange({ start: 'a', end: 'm' });
  bus.subscribe('t.**', (msg) => received.push(msg.payload), { keyRange: {} });
  const before = bus.getStats().filteredMessages;
  bus.publish('t.x', 'a-msg', { key: 'a' }); // start inclusive: delivered
  bus.publish('t.x', 'g-msg', { key: 'g' }); // inside: delivered
  bus.publish('t.x', 'm-msg', { key: 'm' }); // end exclusive: skipped
  bus.publish('t.x', 'z-msg', { key: 'z' }); // above end: skipped
  bus.publish('t.x', 'keyless-msg'); // no key: never matches a key range
  await flush();
  assert.deepEqual(received, ['a-msg', 'g-msg']);
  assert.equal(bus.getStats().filteredMessages, before + 3);
});

test('EB-508: an explicit start / end each wins over the bus default', async () => {
  // Explicit start 'h' wins over the default 'a': 'b' is out.
  {
    const bus = new EventBus();
    const received: unknown[] = [];
    bus.setDefaultKeyRange({ start: 'a', end: 'm' });
    bus.subscribe('t', (msg) => received.push(msg.payload), { keyRange: { start: 'h' } });
    bus.publish('t', 'b-msg', { key: 'b' });
    bus.publish('t', 'k-msg', { key: 'k' });
    await flush();
    assert.deepEqual(received, ['k-msg']);
  }
  // Explicit end 'c' wins over the default 'm': 'g' is out.
  {
    const bus = new EventBus();
    const received: unknown[] = [];
    bus.setDefaultKeyRange({ start: 'a', end: 'm' });
    bus.subscribe('t', (msg) => received.push(msg.payload), { keyRange: { end: 'c' } });
    bus.publish('t', 'b-msg', { key: 'b' });
    bus.publish('t', 'g-msg', { key: 'g' });
    await flush();
    assert.deepEqual(received, ['b-msg']);
  }
});

test('EB-508: a default end only combines with an explicit start, and vice versa', async () => {
  // Default start only + explicit end: the merged [a, m) governs.
  {
    const bus = new EventBus();
    const received: unknown[] = [];
    bus.setDefaultKeyRange({ start: 'a' });
    bus.subscribe('t', (msg) => received.push(msg.payload), { keyRange: { end: 'm' } });
    bus.publish('t', 'a-msg', { key: 'a' });
    bus.publish('t', 'g-msg', { key: 'g' });
    bus.publish('t', 'm-msg', { key: 'm' });
    bus.publish('t', 'z-msg', { key: 'z' });
    await flush();
    assert.deepEqual(received, ['a-msg', 'g-msg']);
  }
  // Default end only + explicit start: the merged [g, m) governs.
  {
    const bus = new EventBus();
    const received: unknown[] = [];
    bus.setDefaultKeyRange({ end: 'm' });
    bus.subscribe('t', (msg) => received.push(msg.payload), { keyRange: { start: 'g' } });
    bus.publish('t', 'a-msg', { key: 'a' });
    bus.publish('t', 'g-msg', { key: 'g' });
    bus.publish('t', 'm-msg', { key: 'm' });
    await flush();
    assert.deepEqual(received, ['g-msg']);
  }
});

test('EB-508: ends that meet only after the merge still throw and the live default is untouched', () => {
  const bus = new EventBus();
  bus.setDefaultKeyRange({ start: 'm' });
  // Explicit end 'a' <= default start 'm': merged start >= end.
  assert.throws(() => bus.subscribe('t', () => {}, { keyRange: { end: 'a' } }), RangeError);
  // The reverse: default end 'm' with an explicit start 'z'.
  bus.setDefaultKeyRange({ end: 'm' });
  assert.throws(() => bus.subscribe('t', () => {}, { keyRange: { start: 'z' } }), RangeError);
  // No half-registered subscriber, and the live default did not move.
  assert.equal(bus.subscriberCount(), 0);
  assert.deepEqual(bus.getDefaultKeyRange(), { end: 'm' });
  assert.equal(bus.getSubscriptionAudit({ action: 'default_key_range_revised' }).length, 2);
});

test('EB-508: clearing with null returns new opt-in subscribers to the subscribe-time gates', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.setDefaultKeyRange({ start: 'a', end: 'm' });
  bus.setDefaultKeyRange(null);
  assert.equal(bus.getDefaultKeyRange(), undefined);
  assert.throws(() => bus.subscribe('t', () => {}, { keyRange: { start: 'm', end: 'a' } }), RangeError);
  // `{}` with no default is the old semantics: every keyed message,
  // still no keyless one.
  bus.subscribe('t', (msg) => received.push(msg.payload), { keyRange: {} });
  bus.publish('t', 'a-msg', { key: 'a' });
  bus.publish('t', 'z-msg', { key: 'z' });
  bus.publish('t', 'keyless-msg');
  await flush();
  assert.deepEqual(received, ['a-msg', 'z-msg']);
  const audit = bus.getSubscriptionAudit({ action: 'default_key_range_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, {
    previous: { start: 'a', end: 'm' },
    keyRange: null,
  });
});

test('EB-508: a same-value revision is a no-op with no audit', () => {
  const bus = new EventBus();
  bus.setDefaultKeyRange({ start: 'a', end: 'm' });
  bus.setDefaultKeyRange({ start: 'a', end: 'm' });
  bus.setDefaultKeyRange({ end: 'm', start: 'a' });
  assert.equal(bus.getSubscriptionAudit({ action: 'default_key_range_revised' }).length, 1);
  bus.setDefaultKeyRange(null);
  bus.setDefaultKeyRange(null);
  assert.equal(bus.getSubscriptionAudit({ action: 'default_key_range_revised' }).length, 2);
});

test('EB-508: illegal values throw RangeError and the live default is untouched', () => {
  const bus = new EventBus();
  bus.setDefaultKeyRange({ start: 'a', end: 'm' });
  assert.throws(() => bus.setDefaultKeyRange('a-m' as never), RangeError);
  assert.throws(() => bus.setDefaultKeyRange(42 as never), RangeError);
  assert.throws(() => bus.setDefaultKeyRange(undefined as never), RangeError);
  assert.throws(() => bus.setDefaultKeyRange({ start: 5 as never }), RangeError);
  assert.throws(() => bus.setDefaultKeyRange({ end: null as never }), RangeError);
  assert.throws(() => bus.setDefaultKeyRange({ start: 'm', end: 'a' }), RangeError);
  assert.throws(() => bus.setDefaultKeyRange({ start: 'a', end: 'a' }), RangeError);
  assert.throws(() => bus.setDefaultKeyRange({ start: 'a', end: 'm' }, ''), RangeError);
  assert.deepEqual(bus.getDefaultKeyRange(), { start: 'a', end: 'm' });
  assert.equal(bus.getSubscriptionAudit({ action: 'default_key_range_revised' }).length, 1);
});

test('EB-508: a revision never moves an already-subscribed range', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.setDefaultKeyRange({ start: 'a', end: 'm' });
  bus.subscribe('t', (msg) => received.push(msg.payload), { keyRange: {} });
  // The default moves to a disjoint slice — the subscriber keeps
  // the [a, m) it was baked with.
  bus.setDefaultKeyRange({ start: 's', end: 'z' });
  bus.publish('t', 'g-msg', { key: 'g' });
  bus.publish('t', 't-msg', { key: 't' });
  await flush();
  assert.deepEqual(received, ['g-msg']);
});

test('EB-508: the default never enables a key range — omitting keyRange receives keyed and keyless as before', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.setDefaultKeyRange({ start: 'a', end: 'm' });
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publish('t', 'a-msg', { key: 'a' });
  bus.publish('t', 'z-msg', { key: 'z' });
  bus.publish('t', 'keyless-msg');
  await flush();
  assert.deepEqual(received, ['a-msg', 'z-msg', 'keyless-msg']);
  assert.equal(bus.getStats().keyRangeSubscribers, 0);
});

test('EB-508: setKeyRange on a defaulted subscriber is still a whole-range revision for set ranges only, and clearing never re-applies the default', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.setDefaultKeyRange({ start: 'a', end: 'm' });
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), { keyRange: {} });
  // Whole-object revision: the baked [a, m) becomes [b, c).
  bus.setKeyRange(sub.id, { start: 'b', end: 'c' });
  bus.publish('t', 'a-msg', { key: 'a' });
  bus.publish('t', 'b-msg', { key: 'b' });
  await flush();
  assert.deepEqual(received, ['b-msg']);
  // Clearing removes the range — the bus default does not re-apply.
  bus.setKeyRange(sub.id, null);
  bus.publish('t', 'keyless-msg');
  await flush();
  assert.deepEqual(received, ['b-msg', 'keyless-msg']);
  // A cleared range cannot be revised again, even with a default set.
  assert.throws(() => bus.setKeyRange(sub.id, { start: 'a', end: 'm' }), RangeError);
  // And a never-set subscriber cannot be revised either.
  const plain = bus.subscribe('u', () => {});
  assert.throws(() => bus.setKeyRange(plain.id, { start: 'a', end: 'm' }), RangeError);
});

test('EB-508: the audit records previous/keyRange exactly and the getter follows the live value and returns a copy', () => {
  const bus = new EventBus();
  const input = { start: 'a', end: 'm' };
  bus.setDefaultKeyRange(input);
  // The setter stores a defensive copy: mutating the input does not move it.
  input.start = 'z';
  assert.deepEqual(bus.getDefaultKeyRange(), { start: 'a', end: 'm' });
  const audit = bus.getSubscriptionAudit({ action: 'default_key_range_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    previous: null,
    keyRange: { start: 'a', end: 'm' },
  });
  // The getter returns a copy: mutating the result does not move it.
  const held = bus.getDefaultKeyRange();
  assert.ok(held !== undefined);
  held.end = 'z';
  assert.deepEqual(bus.getDefaultKeyRange(), { start: 'a', end: 'm' });
  // A revision moves the live value and audits both sides.
  bus.setDefaultKeyRange({ start: 's', end: 'z' }, 'ops');
  assert.deepEqual(bus.getDefaultKeyRange(), { start: 's', end: 'z' });
  const revised = bus.getSubscriptionAudit({ action: 'default_key_range_revised' });
  assert.equal(revised.length, 2);
  assert.deepEqual(revised[1].detail, {
    previous: { start: 'a', end: 'm' },
    keyRange: { start: 's', end: 'z' },
  });
  assert.equal(revised[1].actor, 'ops');
});
