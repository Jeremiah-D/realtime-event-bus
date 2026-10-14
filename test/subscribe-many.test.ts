import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  AclDeniedError,
  type BusMessage,
  type SubscribeManyEntry,
} from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

test('registers independent subscriptions in registration order', async () => {
  const bus = new EventBus();
  const gotA: unknown[] = [];
  const gotB: unknown[] = [];
  const subs = bus.subscribeMany([
    { pattern: 'a.**', handler: (msg) => gotA.push(msg.payload) },
    { pattern: 'b.**', handler: (msg) => gotB.push(msg.payload) },
  ]);
  assert.equal(subs.length, 2);
  assert.ok(subs[0].id < subs[1].id, 'registration order');
  assert.equal(bus.getStats().totalSubscribers, 2);

  bus.publish('a.1', 'A');
  bus.publish('b.1', 'B');
  await flush();
  // Independent subscriptions: each handler sees only its own pattern.
  assert.deepEqual(gotA, ['A']);
  assert.deepEqual(gotB, ['B']);
});

test('failed entry rolls back everything: zero subscriptions left', async () => {
  const bus = new EventBus();
  const got: unknown[] = [];
  assert.throws(
    () =>
      bus.subscribeMany([
        { pattern: 'ok.**', handler: (msg: BusMessage) => got.push(msg.payload) },
        // queueMaxBytes <= 0 is rejected inside subscribe().
        { pattern: 'bad.**', handler: () => {}, opts: { queueMaxBytes: 0 } },
      ]),
    RangeError,
  );
  assert.equal(bus.getStats().totalSubscribers, 0);
  // The rolled-back subscription really is gone: no delivery happens.
  bus.publish('ok.1', 'x');
  await flush();
  assert.deepEqual(got, []);
});

test('ACL denial in the middle of a batch rolls back earlier entries', () => {
  const bus = new EventBus({
    acl: { rules: [{ pattern: 'secret.**', subscribe: 'deny' }] },
  });
  assert.throws(
    () =>
      bus.subscribeMany([
        { pattern: 'public.**', handler: () => {} },
        { pattern: 'secret.**', handler: () => {} },
      ]),
    AclDeniedError,
  );
  assert.equal(bus.getStats().totalSubscribers, 0);
});

test('namespace violation in a batch rolls back earlier entries', () => {
  const bus = new EventBus();
  bus.createNamespace('t1', { allowPublish: true, allowSubscribe: false });
  let thrown: unknown;
  try {
    bus.subscribeMany([
      { pattern: 'a.**', handler: () => {} },
      { pattern: 'b.**', handler: () => {}, opts: { namespace: 't1' } },
    ]);
  } catch (err) {
    thrown = err;
  }
  assert.equal((thrown as Error).name, 'NamespaceDeniedError');
  assert.equal(bus.getStats().totalSubscribers, 0);
});

test('commonOpts merge: per-entry opts win', async () => {
  const bus = new EventBus();
  const gotFiltered: unknown[] = [];
  const gotAll: unknown[] = [];
  bus.subscribeMany(
    [
      { pattern: 'm.**', handler: (msg) => gotFiltered.push(msg.payload) },
      {
        pattern: 'm.**',
        handler: (msg) => gotAll.push(msg.payload),
        opts: { filter: () => true },
      },
    ],
    { filter: (payload) => payload !== 'skip' },
  );
  bus.publish('m.1', 'keep');
  bus.publish('m.2', 'skip');
  await flush();
  assert.deepEqual(gotFiltered, ['keep']);
  assert.deepEqual(gotAll, ['keep', 'skip']);
});

test('group entries batch-join the same consumer group', async () => {
  const bus = new EventBus();
  const got1: unknown[] = [];
  const got2: unknown[] = [];
  const subs = bus.subscribeMany([
    { pattern: 'jobs.**', handler: (msg) => got1.push(msg.payload), opts: { group: 'workers' } },
    { pattern: 'jobs.**', handler: (msg) => got2.push(msg.payload), opts: { group: 'workers' } },
  ]);
  assert.equal(subs.length, 2);
  bus.publish('jobs.1', 'job-1');
  await flush();
  // Competing consumers: exactly one member gets the message.
  assert.equal(got1.length + got2.length, 1);
});

test('group batch join rolls back when a later entry fails', () => {
  const bus = new EventBus();
  assert.throws(
    () =>
      bus.subscribeMany([
        { pattern: 'a.**', handler: () => {}, opts: { group: 'g' } },
        // handoffLingerMs < 0 is rejected inside subscribeToGroup().
        { pattern: 'b.**', handler: () => {}, opts: { group: 'g', handoffLingerMs: -1 } },
      ]),
    RangeError,
  );
  assert.equal(bus.getStats().totalSubscribers, 0);
});

test('batch-delivery entries work through subscribeMany', async () => {
  const bus = new EventBus();
  const batches: unknown[][] = [];
  bus.subscribeMany([
    {
      pattern: 'bulk.**',
      handler: (msgs: BusMessage[]) => batches.push(msgs.map((m) => m.payload)),
      opts: { batch: { maxSize: 10, maxWaitMs: 20 } },
    },
  ]);
  bus.publish('bulk.1', 1);
  bus.publish('bulk.2', 2);
  await flush();
  await new Promise<void>((resolve) => setTimeout(resolve, 60));
  assert.deepEqual(batches, [[1, 2]]);
});

test('shadow combination rules apply per entry, with rollback', () => {
  const bus = new EventBus();
  assert.throws(
    () =>
      bus.subscribeMany([
        { pattern: 'a.**', handler: () => {} },
        // shadow + throttle is rejected inside subscribe().
        {
          pattern: 'b.**',
          handler: () => {},
          opts: { shadow: true, throttle: true },
        },
      ]),
    RangeError,
  );
  assert.equal(bus.getStats().totalSubscribers, 0);
});

test('namespaced entries subscribe inside their namespace', async () => {
  const bus = new EventBus();
  bus.createNamespace('t1', { allowPublish: true, allowSubscribe: true });
  const got: string[] = [];
  bus.subscribeMany([{ pattern: 'x.**', handler: (msg) => got.push(msg.topic), opts: { namespace: 't1' } }]);
  bus.publish('x.1', 1, { namespace: 't1' });
  bus.publish('x.1', 2); // global topic: not visible inside the namespace
  await flush();
  assert.deepEqual(got, ['t1/x.1']);
});

test('invalid batch shapes fail fast before anything registers', () => {
  const bus = new EventBus();
  assert.throws(() => bus.subscribeMany([]), RangeError);
  assert.throws(() => bus.subscribeMany('nope' as unknown as SubscribeManyEntry[]), RangeError);
  assert.throws(
    () => bus.subscribeMany([{ pattern: '', handler: () => {} }]),
    RangeError,
  );
  assert.throws(
    () => bus.subscribeMany([{ pattern: 'a', handler: 'nope' as never }]),
    TypeError,
  );
  assert.throws(
    () => bus.subscribeMany([{ pattern: 'a', handler: () => {} }], 'nope' as never),
    TypeError,
  );
  assert.throws(
    () => bus.subscribeMany([{ pattern: 'a', handler: () => {}, opts: { group: '' } }]),
    RangeError,
  );
  assert.equal(bus.getStats().totalSubscribers, 0);
});

test('a denied entry preserves the original error identity', () => {
  const bus = new EventBus({
    acl: { rules: [{ pattern: 'no.**', subscribe: 'deny' }] },
  });
  let thrown: unknown;
  try {
    bus.subscribeMany([
      { pattern: 'yes.**', handler: () => {} },
      { pattern: 'no.**', handler: () => {} },
    ]);
  } catch (err) {
    thrown = err;
  }
  assert.ok(thrown instanceof AclDeniedError);
  assert.equal(bus.getStats().totalSubscribers, 0);
});
