import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type BusMessage, type Delivery } from '../src/bus.ts';
import { compileFilterExpression } from '../src/filterexpr.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const collect = (bus: EventBus, pattern: string, opts?: Parameters<EventBus['subscribe']>[2]) => {
  const received: BusMessage[] = [];
  const sub = bus.subscribe(pattern, (msg) => received.push(msg), opts);
  return { received, sub };
};

test('filterExpr: equality and inequality operators', async () => {
  const bus = new EventBus();
  const { received } = collect(bus, 't', { filterExpr: `type == "trade"` });
  bus.publish('t', { type: 'trade', price: 1 });
  bus.publish('t', { type: 'quote', price: 2 });
  bus.publish('t', { type: 'trade', price: 3 });
  await flush();
  assert.deepEqual(
    received.map((m) => (m.payload as { price: number }).price),
    [1, 3],
  );
  assert.equal(bus.getStats().filteredMessages, 1);

  const bus2 = new EventBus();
  const got2: unknown[] = [];
  bus2.subscribe('t', (m) => got2.push(m.payload), { filterExpr: `retries != 3` });
  bus2.publish('t', { retries: 3 });
  bus2.publish('t', { retries: 0 });
  bus2.publish('t', { retries: '3' });
  await flush();
  assert.deepEqual(got2, [{ retries: 0 }, { retries: '3' }]);
});

test('filterExpr: range operators, numeric and lexicographic', async () => {
  const bus = new EventBus();
  const { received } = collect(bus, 't', { filterExpr: `price > 100 && price <= 500` });
  for (const price of [50, 100, 101, 500, 501]) bus.publish('t', { price });
  await flush();
  assert.deepEqual(
    received.map((m) => (m.payload as { price: number }).price),
    [101, 500],
  );
  assert.equal(bus.getStats().filteredMessages, 3);

  const bus2 = new EventBus();
  const got2: unknown[] = [];
  bus2.subscribe('t', (m) => got2.push(m.payload), { filterExpr: `tier >= "gold"` });
  bus2.publish('t', { tier: 'bronze' });
  bus2.publish('t', { tier: 'gold' });
  bus2.publish('t', { tier: 'platinum' });
  bus2.publish('t', { tier: 42 });
  await flush();
  assert.deepEqual(got2, [{ tier: 'gold' }, { tier: 'platinum' }]);
});

test('filterExpr: existence, both spellings, on dotted paths', async () => {
  const bus = new EventBus();
  const { received } = collect(bus, 't', { filterExpr: `exists(user.id)` });
  bus.publish('t', { user: { id: 7 } });
  bus.publish('t', { user: { name: 'x' } });
  bus.publish('t', {});
  await flush();
  assert.deepEqual(received.map((m) => m.payload), [{ user: { id: 7 } }]);
  assert.equal(bus.getStats().filteredMessages, 2);

  const bus2 = new EventBus();
  const got2: unknown[] = [];
  bus2.subscribe('t', (m) => got2.push(m.payload), { filterExpr: `user.id exists` });
  bus2.publish('t', { user: { id: 1 } });
  bus2.publish('t', { user: null });
  await flush();
  assert.deepEqual(got2, [{ user: { id: 1 } }]);
});

test('filterExpr: regex operator with flags, non-strings never match', async () => {
  const bus = new EventBus();
  const { received } = collect(bus, 't', { filterExpr: `symbol =~ /^BTC/` });
  bus.publish('t', { symbol: 'BTCUSD' });
  bus.publish('t', { symbol: 'ETHBTC' });
  bus.publish('t', { symbol: 12345 });
  bus.publish('t', {});
  await flush();
  assert.deepEqual(received.map((m) => m.payload), [{ symbol: 'BTCUSD' }]);
  assert.equal(bus.getStats().filteredMessages, 3);

  const bus2 = new EventBus();
  const got2: unknown[] = [];
  bus2.subscribe('t', (m) => got2.push(m.payload), { filterExpr: `sym =~ /^btc/i` });
  bus2.publish('t', { sym: 'BTC' });
  await flush();
  assert.deepEqual(got2, [{ sym: 'BTC' }]);
});

test('filterExpr: array-index paths and boolean composition', async () => {
  const bus = new EventBus();
  const { received } = collect(bus, 't', {
    filterExpr: `legs[0].amount >= 10 && (side == "buy" || !cancelled == true)`,
  });
  bus.publish('t', { legs: [{ amount: 10 }], side: 'buy', cancelled: false });
  bus.publish('t', { legs: [{ amount: 9 }], side: 'buy', cancelled: false });
  bus.publish('t', { legs: [{ amount: 10 }], side: 'sell', cancelled: true });
  bus.publish('t', { legs: [], side: 'buy', cancelled: false });
  await flush();
  assert.equal(received.length, 1);
  assert.equal(bus.getStats().filteredMessages, 3);
});

test('filterExpr: illegal expressions throw RangeError at subscribe time', () => {
  const bus = new EventBus();
  const bad = [
    'type = "trade"', // single = is not an operator
    'price >> 100',
    'a & b',
    'a | b',
    'price >', // truncated
    '(type == "x"', // unbalanced paren
    'type == "x")',
    'symbol =~ "^BTC"', // =~ needs a /.../ literal
    'symbol =~ /(/', // invalid regex
    'symbol == /x/', // regex literal with ==
    'legs[1.5].amount == 1', // non-integer index
    'n % 2 == 0', // % is not an operator
    'exists user.id', // exists needs parens
    '1 == 1', // left side must be a path
    '', // empty
    '   ',
  ];
  for (const expr of bad) {
    assert.throws(
      () => bus.subscribe('t', () => {}, { filterExpr: expr }),
      (err: unknown) =>
        err instanceof RangeError && (err as Error).message.startsWith('filterExpr:'),
      `expected RangeError for ${JSON.stringify(expr)}`,
    );
  }
  // A rejected subscribe registers nothing.
  assert.equal(bus.subscriberCount(), 0);
  // Non-string filterExpr is a TypeError, matching the filter convention.
  assert.throws(
    () => bus.subscribe('t', () => {}, { filterExpr: 42 as unknown as string }),
    (err: unknown) => err instanceof TypeError,
  );
});

test('filterExpr: compiles once — direct compileFilterExpression unit checks', () => {
  const fx = compileFilterExpression(`type == "trade"`);
  assert.equal(fx.source, `type == "trade"`);
  assert.equal(fx.test({ type: 'trade' }), true);
  assert.equal(fx.test({ type: 'nope' }), false);
  // Missing field: equality is false even against null; != is its negation.
  assert.equal(compileFilterExpression(`x == null`).test({}), false);
  assert.equal(compileFilterExpression(`x != null`).test({}), true);
  assert.equal(compileFilterExpression(`exists(x)`).test({}), false);
  // Never throws, even on hostile input.
  assert.equal(fx.test(null), false);
  assert.equal(fx.test(42), false);
  assert.equal(fx.test('trade'), false);
  assert.equal(fx.test(undefined), false);
  const getter = {};
  Object.defineProperty(getter, 'boom', {
    get() {
      throw new Error('getter');
    },
  });
  assert.equal(compileFilterExpression(`exists(boom)`).test(getter), false);
});

test('filterExpr: composes with the filter predicate — both must accept', async () => {
  const bus = new EventBus();
  const { received } = collect(bus, 'market.**', {
    filter: (payload) => (payload as { price: number }).price > 10,
    filterExpr: `symbol == "BTC"`,
  });
  bus.publish('market.btc', { symbol: 'BTC', price: 50 }); // both pass
  bus.publish('market.eth', { symbol: 'ETH', price: 50 }); // expr rejects
  bus.publish('market.btc', { symbol: 'BTC', price: 5 }); // predicate rejects
  bus.publish('market.eth', { symbol: 'ETH', price: 5 }); // both reject
  await flush();
  assert.deepEqual(received.map((m) => m.payload), [{ symbol: 'BTC', price: 50 }]);
  assert.equal(bus.getStats().filteredMessages, 3);
  assert.equal(bus.getStats().sequenceGaps, 0);
});

test('filterExpr: even/odd via comparison, no phantom gaps', async () => {
  const bus = new EventBus();
  const { received } = collect(bus, 't', { filterExpr: `n >= 2 && n <= 4` });
  for (let n = 1; n <= 5; n++) bus.publish('t', { n });
  await flush();
  assert.deepEqual(
    received.map((m) => (m.payload as { n: number }).n),
    [2, 3, 4],
  );
  assert.equal(bus.getStats().sequenceGaps, 0);
  assert.equal(bus.getStats().filteredMessages, 2);
  const topic = bus.getStats().topics.find((t) => t.topic === 't');
  assert.equal(topic?.filteredMessages, 2);
});

test('filterExpr: rejected messages consume no backpressure budget', async () => {
  const bus = new EventBus();
  const { received, sub } = collect(bus, 't', {
    queueSize: 2,
    filterExpr: `keep == true`,
  });
  for (let i = 0; i < 10; i++) bus.publish('t', { keep: false, i });
  bus.publish('t', { keep: true, i: 'a' });
  bus.publish('t', { keep: true, i: 'b' });
  await flush();
  assert.equal(bus.droppedCount(sub.id), 0);
  assert.deepEqual(
    received.map((m) => (m.payload as { i: unknown }).i),
    ['a', 'b'],
  );
  assert.equal(bus.getStats().filteredMessages, 10);
});

test('filterExpr: filtered messages consume no delivery credit', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (m) => received.push(m.payload), {
    filterExpr: `kind == "wanted"`,
  });
  // One credit: the wanted message is delivered, the filtered one must
  // not burn the single credit (it never reaches the drain at all).
  assert.equal(bus.grantCredits(sub.id, 1), 1);
  bus.publish('t', { kind: 'wanted', n: 1 });
  bus.publish('t', { kind: 'skipped', n: 2 });
  await flush();
  assert.deepEqual(received, [{ kind: 'wanted', n: 1 }]);
  assert.equal(bus.getStats().creditExhaustedSubscribers, 1);
  // A second wanted message stays queued for lack of credit — proving
  // the filtered message consumed zero credit.
  bus.publish('t', { kind: 'wanted', n: 3 });
  await flush();
  assert.deepEqual(received, [{ kind: 'wanted', n: 1 }]);
  bus.grantCredits(sub.id, 1);
  await flush();
  assert.deepEqual(received, [
    { kind: 'wanted', n: 1 },
    { kind: 'wanted', n: 3 },
  ]);
  assert.equal(bus.getStats().filteredMessages, 1);
});

test('filterExpr: filtered messages create no ack obligation on reliable subscriptions', async () => {
  const bus = new EventBus({ ackTimeoutMs: 50 });
  const deliveries: Delivery<BusMessage>[] = [];
  const sub = bus.subscribeReliable('t', (d) => deliveries.push(d), {
    filterExpr: `kind == "wanted"`,
  });
  bus.publish('t', { kind: 'wanted', n: 1 });
  bus.publish('t', { kind: 'skipped', n: 2 });
  await flush();
  assert.equal(deliveries.length, 1);
  // Exactly one outstanding delivery: the filtered message never entered
  // the ack tracker, so there is nothing to ack for it.
  assert.equal(bus.unackedCount(sub.id), 1);
  assert.equal(bus.getStats().unackedDeliveries, 1);
  deliveries[0].ack();
  await flush();
  assert.equal(bus.unackedCount(sub.id), 0);
  assert.equal(bus.getStats().unackedDeliveries, 0);
  assert.equal(bus.getStats().filteredMessages, 1);
  // No phantom redelivery timer fires for the filtered message.
  await new Promise<void>((resolve) => setTimeout(resolve, 120));
  assert.equal(deliveries.length, 1);
});

test('filterExpr: non-object payloads never throw — missing fields simply miss', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (m) => received.push(m.payload), {
    filterExpr: `type == "x" && exists(deep.field)`,
  });
  bus.publish('t', 'just a string');
  bus.publish('t', 42);
  bus.publish('t', null);
  bus.publish('t', [1, 2, 3]);
  bus.publish('t', { type: 'x', deep: { field: 1 } });
  await flush();
  assert.deepEqual(received, [{ type: 'x', deep: { field: 1 } }]);
  assert.equal(bus.getStats().filteredMessages, 4);
});

test('filterExpr: applies to durable-log replay without queue churn or gaps', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'eb-filterexpr-'));
  const bus = new EventBus({ durableLogDir: dir });
  for (let n = 1; n <= 4; n++) bus.publish('t', { n });
  await flush();
  const bus2 = new EventBus({ durableLogDir: dir });
  const { received } = collect(bus2, 't', {
    resumeFromSeq: 0,
    filterExpr: `n >= 2 && n <= 3`,
  });
  await flush();
  assert.deepEqual(
    received.map((m) => (m.payload as { n: number }).n),
    [2, 3],
  );
  assert.equal(bus2.getStats().filteredMessages, 2);
  assert.equal(bus2.getStats().sequenceGaps, 0);
  // Live publishes after the replay keep working with the expression.
  bus2.publish('t', { n: 5 });
  bus2.publish('t', { n: 2 });
  await flush();
  assert.deepEqual(
    received.map((m) => (m.payload as { n: number }).n),
    [2, 3, 2],
  );
  assert.equal(bus2.getStats().sequenceGaps, 0);
});

test('filterExpr: string literals with escapes and quotes', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (m) => received.push(m.payload), {
    filterExpr: `note == "say \\"hi\\"" && tag == 'it\\'s'`,
  });
  bus.publish('t', { note: 'say "hi"', tag: "it's" });
  bus.publish('t', { note: 'say "hi"', tag: 'other' });
  await flush();
  assert.deepEqual(received, [{ note: 'say "hi"', tag: "it's" }]);
});
