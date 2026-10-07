import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, compilePattern } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/**
 * Equivalence oracle test: the prefix index must never change *who* gets a
 * message. `compilePattern` is the independent authority for whether a
 * pattern matches a topic; the bus (index + regex confirmation) must agree
 * exactly, across exact patterns, `*`, `**` in leading/middle/trailing
 * position, and consecutive `**`.
 */
test('prefix index preserves exact delivery sets across pattern shapes', async () => {
  const patterns = [
    'market.btc', // exact
    'market.btc.trades', // exact, deeper
    'market.*', // single wildcard, trailing
    'market.btc.*', // single wildcard, deeper
    'market.**', // multi wildcard, trailing
    '**', // bare multi wildcard
    '*', // bare single wildcard
    '**.foo', // leading wildcard
    '*.foo', // leading single wildcard
    'market.**.trades', // middle wildcard
    '**.**.foo', // consecutive leading wildcards
    'a', // exact, short
    'a.**', // multi wildcard matching zero segments
    '**.a', // leading wildcard, short
  ];
  const topics = [
    'market',
    'market.btc',
    'market.btc.trades',
    'market.btc.trades.1',
    'market.eth',
    'foo',
    'a.foo',
    'x.y.foo',
    'a',
    'a.b',
    'news',
  ];

  const bus = new EventBus();
  const received: Array<{ pattern: string; topic: string }> = [];
  for (const pattern of patterns) {
    bus.subscribe(pattern, (msg) => received.push({ pattern, topic: msg.topic }));
  }
  for (const topic of topics) bus.publish(topic, null);
  await flush();

  const byTopic = new Map<string, string[]>();
  for (const { pattern, topic } of received) {
    const list = byTopic.get(topic) ?? [];
    list.push(pattern);
    byTopic.set(topic, list);
  }
  for (const topic of topics) {
    const expected = patterns
      .filter((p) => compilePattern(p).test(topic))
      .sort();
    const actual = (byTopic.get(topic) ?? []).sort();
    assert.deepEqual(actual, expected, `delivery set for topic ${JSON.stringify(topic)}`);
  }
});

test('subscribe/unsubscribe maintain the index incrementally', async () => {
  const bus = new EventBus();
  assert.equal(bus.getStats().indexSize, 0);

  const s1 = bus.subscribe('a.b.*', () => {}); // key 'a.b'
  assert.equal(bus.getStats().indexSize, 1);
  const s2 = bus.subscribe('a.b.c', () => {}); // key 'a.b.c'
  assert.equal(bus.getStats().indexSize, 2);
  const s3 = bus.subscribe('*.x', () => {}); // key ''
  assert.equal(bus.getStats().indexSize, 3);
  const s4 = bus.subscribe('**', () => {}); // key '' (shared)
  assert.equal(bus.getStats().indexSize, 3);

  // Key survives while one subscriber remains on it.
  s4.unsubscribe();
  assert.equal(bus.getStats().indexSize, 3);
  // Last subscriber off the '' key: the key is evicted, no unbounded growth.
  s3.unsubscribe();
  assert.equal(bus.getStats().indexSize, 2);

  s1.unsubscribe();
  assert.equal(bus.getStats().indexSize, 1); // only the 'a.b.c' key left

  // Delivery behavior follows the index bookkeeping: 'a.b.*' is gone,
  // the exact 'a.b.c' subscriber still receives.
  const received: string[] = [];
  const probe = bus.subscribe('a.b.c', (msg) => received.push(msg.topic));
  assert.equal(bus.publish('a.b.zzz', null), 0);
  assert.equal(bus.publish('a.b.c', null), 2); // s2 + probe
  await flush();
  assert.deepEqual(received, ['a.b.c']);
  probe.unsubscribe();

  s2.unsubscribe();
  assert.equal(bus.getStats().indexSize, 0);

  // Unsubscribe is idempotent: a second call must not corrupt the index.
  s2.unsubscribe();
  assert.equal(bus.getStats().indexSize, 0);
});

test('unsubscribed patterns stop receiving and free their index keys', async () => {
  const bus = new EventBus();
  const received: string[] = [];
  const sub = bus.subscribe('gone.*', (msg) => received.push(msg.topic));
  assert.equal(bus.getStats().indexSize, 1);
  bus.publish('gone.fish', null);
  await flush();
  assert.deepEqual(received, ['gone.fish']);

  sub.unsubscribe();
  assert.equal(bus.getStats().indexSize, 0);
  const accepted = bus.publish('gone.fish', null);
  assert.equal(accepted, 0);
  await flush();
  assert.deepEqual(received, ['gone.fish']); // nothing new
});

test('boundary: a.** matches the bare topic a (zero segments)', async () => {
  const bus = new EventBus();
  const received: string[] = [];
  bus.subscribe('a.**', (msg) => received.push(msg.topic));
  bus.publish('a', 1);
  bus.publish('a.b', 2);
  bus.publish('ab', 3); // different literal prefix: must not match
  await flush();
  assert.deepEqual(received, ['a', 'a.b']);
});

test('boundary: leading-wildcard patterns live under the empty key', async () => {
  const bus = new EventBus();
  assert.equal(bus.getStats().indexSize, 0);
  const leading: string[] = [];
  const s1 = bus.subscribe('**.foo', (msg) => leading.push(msg.topic));
  assert.equal(bus.getStats().indexSize, 1); // only the '' key
  bus.publish('foo', 1); // zero segments before the literal
  bus.publish('x.foo', 2);
  bus.publish('x.y.foo', 3);
  bus.publish('foo.bar', 4); // no match
  await flush();
  assert.deepEqual(leading, ['foo', 'x.foo', 'x.y.foo']);
  s1.unsubscribe();
  assert.equal(bus.getStats().indexSize, 0);
});

test('subscribeReliable routes through the same index', async () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable('r.*', (delivery) => delivery.ack());
  assert.equal(bus.getStats().indexSize, 1);
  sub.unsubscribe();
  assert.equal(bus.getStats().indexSize, 0);
});
