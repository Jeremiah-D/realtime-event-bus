import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BackpressureEvent } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

test('exact topic match delivers the payload', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('market.btc', (msg) => received.push(msg.payload));
  const delivered = bus.publish('market.btc', { price: 50000 });
  assert.equal(delivered, 1);
  await flush();
  assert.deepEqual(received, [{ price: 50000 }]);
});

test('wildcard segment matches one topic level', async () => {
  const bus = new EventBus();
  const received: string[] = [];
  bus.subscribe('market.*', (msg) => received.push(msg.topic));
  bus.publish('market.btc', 1);
  bus.publish('market.eth', 2);
  bus.publish('market', 3); // different segment count: no match
  bus.publish('news.btc', 4); // different first segment: no match
  await flush();
  assert.deepEqual(received, ['market.btc', 'market.eth']);
});

test('bare * matches every topic', async () => {
  const bus = new EventBus();
  const received: string[] = [];
  bus.subscribe('*', (msg) => received.push(msg.topic));
  bus.publish('a', 1);
  bus.publish('a.b.c', 2);
  await flush();
  assert.deepEqual(received, ['a', 'a.b.c']);
});

test('** matches zero or more trailing segments', async () => {
  const bus = new EventBus();
  const received: string[] = [];
  bus.subscribe('market.**', (msg) => received.push(msg.topic));
  bus.publish('market', 1); // zero segments after the prefix
  bus.publish('market.btc', 2); // one segment
  bus.publish('market.btc.trades', 3); // two segments
  bus.publish('news.btc.trades', 4); // different prefix: no match
  await flush();
  assert.deepEqual(received, ['market', 'market.btc', 'market.btc.trades']);
});

test('** works in leading and middle positions', async () => {
  const bus = new EventBus();
  const leading: string[] = [];
  const middle: string[] = [];
  bus.subscribe('**.trades', (msg) => leading.push(msg.topic));
  bus.subscribe('market.**.trades', (msg) => middle.push(msg.topic));
  bus.publish('trades', 1);
  bus.publish('market.trades', 2);
  bus.publish('market.btc.trades', 3);
  bus.publish('market.btc.quotes', 4); // suffix differs: no match
  bus.publish('news.trades', 5); // first segment differs for middle: no match
  await flush();
  assert.deepEqual(leading, ['trades', 'market.trades', 'market.btc.trades', 'news.trades']);
  assert.deepEqual(middle, ['market.trades', 'market.btc.trades']);
});

test('bare ** matches every topic', async () => {
  const bus = new EventBus();
  const received: string[] = [];
  bus.subscribe('**', (msg) => received.push(msg.topic));
  bus.publish('a', 1);
  bus.publish('a.b.c.d', 2);
  await flush();
  assert.deepEqual(received, ['a', 'a.b.c.d']);
});

test('non-matching topics are not delivered', async () => {
  const bus = new EventBus();
  const received: string[] = [];
  bus.subscribe('orders.*', (msg) => received.push(msg.topic));
  bus.publish('market.btc', 1);
  bus.publish('orders', 2); // segment count differs
  await flush();
  assert.deepEqual(received, []);
});

test('publish fans out to all matching subscribers and returns the count', async () => {
  const bus = new EventBus();
  const hits: string[] = [];
  bus.subscribe('market.*', () => hits.push('wildcard'));
  bus.subscribe('market.btc', () => hits.push('exact'));
  bus.subscribe('news.*', () => hits.push('other'));
  const delivered = bus.publish('market.btc', 1);
  assert.equal(delivered, 2);
  await flush();
  assert.deepEqual(hits.sort(), ['exact', 'wildcard']);
});

test('unsubscribe stops delivery', async () => {
  const bus = new EventBus();
  let count = 0;
  const sub = bus.subscribe('t', () => {
    count += 1;
  });
  assert.equal(bus.subscriberCount(), 1);
  sub.unsubscribe();
  assert.equal(bus.subscriberCount(), 0);
  bus.publish('t', 1);
  await flush();
  assert.equal(count, 0);
});

test('full queue drops the oldest message and counts it', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), { queueSize: 2 });
  bus.publish('t', 'm1');
  bus.publish('t', 'm2');
  bus.publish('t', 'm3');
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.droppedCount(sub.id), 1);
  await flush();
  assert.deepEqual(received, ['m2', 'm3']); // 'm1' was shed
  assert.equal(bus.pendingCount(sub.id), 0);
});

test('drop-newest sheds the incoming message instead', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 2,
    dropPolicy: 'drop-newest',
  });
  assert.equal(bus.publish('t', 'm1'), 1);
  assert.equal(bus.publish('t', 'm2'), 1);
  assert.equal(bus.publish('t', 'm3'), 0); // rejected at enqueue time
  assert.equal(bus.droppedCount(sub.id), 1);
  await flush();
  assert.deepEqual(received, ['m1', 'm2']);
});

test('onBackpressure fires once when the queue hits its high-water mark', () => {
  const bus = new EventBus();
  const events: BackpressureEvent[] = [];
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 10,
    onBackpressure: (e) => events.push(e),
  });
  for (let i = 0; i < 7; i++) bus.publish('t', i);
  assert.equal(events.length, 0); // below 80%
  bus.publish('t', 7); // size hits 8 = 80% of capacity
  assert.equal(events.length, 1);
  assert.deepEqual(events[0], {
    subscriberId: sub.id,
    pattern: 't',
    queueSize: 8,
    capacity: 10,
    dropped: 0,
  });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  assert.equal(events.length, 1); // fires only once while above the mark
});

test('onBackpressure re-arms after the queue drains', async () => {
  const bus = new EventBus();
  const events: BackpressureEvent[] = [];
  bus.subscribe('t', () => {}, { queueSize: 10, onBackpressure: (e) => events.push(e) });
  for (let i = 0; i < 8; i++) bus.publish('t', i);
  assert.equal(events.length, 1);
  await flush();
  for (let i = 0; i < 8; i++) bus.publish('t', i);
  assert.equal(events.length, 2);
});

test('slow consumer drops are recorded and visible via droppedCount', () => {
  const bus = new EventBus();
  const events: BackpressureEvent[] = [];
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 2,
    onBackpressure: (e) => events.push(e),
  });
  bus.publish('t', 'm1');
  bus.publish('t', 'm2'); // size 2 >= 80%: high-water mark fires
  bus.publish('t', 'm3'); // queue full, drop-oldest sheds 'm1'
  assert.equal(events.length, 1);
  assert.equal(events[0].dropped, 0); // no drop had happened yet when the mark fired
  assert.equal(bus.droppedCount(sub.id), 1);
});

test('backpressure event carries the cumulative dropped count', async () => {
  const bus = new EventBus();
  const events: BackpressureEvent[] = [];
  bus.subscribe('t', () => {}, { queueSize: 2, onBackpressure: (e) => events.push(e) });
  bus.publish('t', 'm1');
  bus.publish('t', 'm2'); // fires: nothing dropped yet
  bus.publish('t', 'm3'); // drop-oldest sheds one
  await flush(); // drain: re-arms the callback
  bus.publish('t', 'm4');
  bus.publish('t', 'm5'); // fires again: one message was shed earlier
  assert.equal(events.length, 2);
  assert.equal(events[1].dropped, 1);
});

test('onBackpressure stays silent when the consumer keeps up', async () => {
  const bus = new EventBus();
  let calls = 0;
  bus.subscribe('t', () => {}, { onBackpressure: () => (calls += 1) });
  bus.publish('t', 'm1');
  await flush();
  assert.equal(calls, 0);
});

test('publishBatch delivers every message in order with one flush', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('market.*', (msg) => received.push(msg.payload));
  const delivered = bus.publishBatch([
    { topic: 'market.btc', payload: 1 },
    { topic: 'market.eth', payload: 2 },
    { topic: 'news.btc', payload: 3 }, // no match
  ]);
  assert.equal(delivered, 2);
  assert.equal(bus.pendingCount(sub.id), 2); // nothing delivered yet: still one pending flush
  await flush();
  assert.deepEqual(received, [1, 2]);
});

test('publishBatch counts accepted deliveries across subscribers', async () => {
  const bus = new EventBus();
  bus.subscribe('market.*', () => {});
  bus.subscribe('market.btc', () => {});
  bus.subscribe('news.*', () => {});
  const delivered = bus.publishBatch([
    { topic: 'market.btc', payload: 1 }, // 2 acceptances
    { topic: 'market.eth', payload: 2 }, // 1 acceptance
  ]);
  assert.equal(delivered, 3);
  await flush();
});

test('publishBatch applies drop policies like publish does', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), { queueSize: 2 });
  const delivered = bus.publishBatch([
    { topic: 't', payload: 'm1' },
    { topic: 't', payload: 'm2' },
    { topic: 't', payload: 'm3' },
  ]);
  // drop-oldest always accepts: 'm3' is enqueued, 'm1' is shed and counted.
  assert.equal(delivered, 3);
  assert.equal(bus.droppedCount(sub.id), 1);
  await flush();
  assert.deepEqual(received, ['m2', 'm3']);
});

test('publishBatch with drop-newest reports rejected messages', async () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {}, { queueSize: 2, dropPolicy: 'drop-newest' });
  const delivered = bus.publishBatch([
    { topic: 't', payload: 'm1' },
    { topic: 't', payload: 'm2' },
    { topic: 't', payload: 'm3' }, // rejected at enqueue time
  ]);
  assert.equal(delivered, 2);
  assert.equal(bus.droppedCount(sub.id), 1);
});

test('publishBatch keeps messages ordered per subscriber', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('*', (msg) => received.push(msg.payload));
  bus.publishBatch([
    { topic: 'a', payload: 'first' },
    { topic: 'b', payload: 'second' },
    { topic: 'c', payload: 'third' },
  ]);
  await flush();
  assert.deepEqual(received, ['first', 'second', 'third']);
});

test('publishBatch of an empty array is a no-op', async () => {
  const bus = new EventBus();
  let calls = 0;
  bus.subscribe('*', () => (calls += 1));
  assert.equal(bus.publishBatch([]), 0);
  await flush();
  assert.equal(calls, 0); // no flush was scheduled
});
