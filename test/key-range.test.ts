import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

test('keyRange: only keyed messages with keys in [start, end) are delivered', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('orders.**', (msg) => received.push(msg.payload), {
    keyRange: { start: 'b', end: 'd' },
  });
  bus.publish('orders.new', 'a-msg', { key: 'a' }); // below start: skipped
  bus.publish('orders.new', 'b-msg', { key: 'b' }); // start inclusive: delivered
  bus.publish('orders.new', 'c-msg', { key: 'c' }); // inside: delivered
  bus.publish('orders.new', 'd-msg', { key: 'd' }); // end exclusive: skipped
  bus.publish('orders.new', 'keyless-msg'); // no key: never matches a key range
  await flush();
  assert.deepEqual(received, ['b-msg', 'c-msg']);
});

test('keyRange: a subscription without keyRange is unaffected', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('orders.**', (msg) => received.push(msg.payload));
  bus.publish('orders.new', 'a-msg', { key: 'a' });
  bus.publish('orders.new', 'z-msg', { key: 'z' });
  bus.publish('orders.new', 'keyless-msg');
  await flush();
  assert.deepEqual(received, ['a-msg', 'z-msg', 'keyless-msg']);
});

test('keyRange: omitted ends are unbounded, both omitted matches every keyed message', async () => {
  const bus = new EventBus();
  const fromM: unknown[] = [];
  const belowM: unknown[] = [];
  const allKeyed: unknown[] = [];
  bus.subscribe('t.**', (msg) => fromM.push(msg.payload), { keyRange: { start: 'm' } });
  bus.subscribe('t.**', (msg) => belowM.push(msg.payload), { keyRange: { end: 'm' } });
  bus.subscribe('t.**', (msg) => allKeyed.push(msg.payload), { keyRange: {} });
  bus.publish('t.x', 'a-msg', { key: 'a' });
  bus.publish('t.x', 'm-msg', { key: 'm' });
  bus.publish('t.x', 'z-msg', { key: 'z' });
  bus.publish('t.x', 'keyless-msg');
  await flush();
  assert.deepEqual(fromM, ['m-msg', 'z-msg']);
  assert.deepEqual(belowM, ['a-msg']);
  assert.deepEqual(allKeyed, ['a-msg', 'm-msg', 'z-msg']);
});

test('keyRange: invalid ranges throw RangeError and register nothing', () => {
  const bus = new EventBus();
  assert.equal(bus.getStats().keyRangeSubscribers, 0);
  assert.throws(
    () => bus.subscribe('t', () => {}, { keyRange: { start: 'd', end: 'b' } }),
    RangeError,
  );
  assert.throws(
    () => bus.subscribe('t', () => {}, { keyRange: { start: 'b', end: 'b' } }),
    RangeError,
  );
  assert.throws(
    () => bus.subscribe('t', () => {}, { keyRange: { start: 5 as never } }),
    RangeError,
  );
  assert.throws(
    () => bus.subscribe('t', () => {}, { keyRange: { end: null as never } }),
    RangeError,
  );
  assert.equal(bus.getStats().keyRangeSubscribers, 0);
  assert.equal(bus.subscriberCount(), 0);
});

test('keyRange: getStats exposes the live key-range subscription count', async () => {
  const bus = new EventBus();
  assert.equal(bus.getStats().keyRangeSubscribers, 0);
  const sub1 = bus.subscribe('t', () => {}, { keyRange: { start: 'a', end: 'z' } });
  bus.subscribe('t', () => {}); // plain subscription: not counted
  assert.equal(bus.getStats().keyRangeSubscribers, 1);
  const sub2 = bus.subscribe('t', () => {}, { keyRange: { start: 'a' } });
  assert.equal(bus.getStats().keyRangeSubscribers, 2);
  sub1.unsubscribe();
  assert.equal(bus.getStats().keyRangeSubscribers, 1);
  sub2.unsubscribe();
  assert.equal(bus.getStats().keyRangeSubscribers, 0);
  await flush();
});

test('keyRange: in-range messages keep per-key publish order, skips never wedge it', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t.**', (msg) => received.push(msg.payload), {
    keyRange: { start: 'b', end: 'd' },
  });
  bus.publish('t.x', 'b-1', { key: 'b' });
  bus.publish('t.x', 'a-1', { key: 'a' }); // out of range, different key
  bus.publish('t.x', 'b-2', { key: 'b' });
  bus.publish('t.x', 'z-1', { key: 'z' }); // out of range, different key
  bus.publish('t.x', 'b-3', { key: 'b' });
  await flush();
  assert.deepEqual(received, ['b-1', 'b-2', 'b-3']);
});

test('keyRange: skips count as filtered, never as sequence gaps', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t.**', (msg) => received.push(msg.payload), {
    keyRange: { start: 'b', end: 'd' },
  });
  const before = bus.getStats().filteredMessages;
  bus.publish('t.x', 'a-msg', { key: 'a' });
  bus.publish('t.x', 'b-msg', { key: 'b' });
  await flush();
  assert.deepEqual(received, ['b-msg']);
  assert.equal(bus.getStats().filteredMessages, before + 1);
});

test('keyRange: composes with reliable delivery — skipped messages carry no ack obligation', async () => {
  const bus = new EventBus();
  const seen: unknown[] = [];
  bus.subscribeReliable(
    't.**',
    (d) => {
      seen.push(d.msg.payload);
      d.ack();
    },
    { keyRange: { start: 'b', end: 'd' }, ackTimeoutMs: 30 },
  );
  bus.publish('t.x', 'a-msg', { key: 'a' });
  bus.publish('t.x', 'b-msg', { key: 'b' });
  await flush();
  assert.deepEqual(seen, ['b-msg']);
  assert.equal(bus.getStats().unackedDeliveries, 0);
});

test('keyRange: composes with a content filter — both must accept', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t.**', (msg) => received.push(msg.payload), {
    keyRange: { start: 'a', end: 'z' },
    filter: (payload) => (payload as number) % 2 === 0,
  });
  bus.publish('t.x', 1, { key: 'b' }); // in range, filter rejects
  bus.publish('t.x', 2, { key: 'b' }); // in range, filter accepts
  bus.publish('t.x', 4, { key: 'zz' }); // range rejects, filter would accept
  await flush();
  assert.deepEqual(received, [2]);
});
