import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { EventBus, TopicRenameInProgressError } from '../src/bus.ts';

const flush = () => new Promise((resolve) => setImmediate(resolve));

test('renameTopic moves subscriptions, stats and seq counters without renumbering', async () => {
  const bus = new EventBus();
  const received: Array<{ payload: unknown; seq: number; topic: string }> = [];
  bus.subscribe('market.btc', (msg) => received.push({ payload: msg.payload, seq: msg.seq, topic: msg.topic }));
  assert.equal(bus.publish('market.btc', 'one'), 1);
  assert.equal(bus.publish('market.btc', 'two'), 1);
  assert.equal(bus.publish('market.btc', 'three'), 1);
  await flush();
  assert.deepEqual(received.map((r) => r.seq), [1, 2, 3]);

  const result = bus.renameTopic('market.btc', 'market.btc.v2');
  assert.equal(result.oldTopic, 'market.btc');
  assert.equal(result.newTopic, 'market.btc.v2');
  assert.equal(result.subscribersRewritten, 1);
  assert.equal(result.seqContinuedFrom, 3);

  // The old name is free again; the new name continues the numbering.
  assert.equal(bus.publish('market.btc.v2', 'four'), 1);
  await flush();
  assert.deepEqual(received.map((r) => r.seq), [1, 2, 3, 4]);
  assert.deepEqual(received.map((r) => r.topic), ['market.btc', 'market.btc', 'market.btc', 'market.btc.v2']);

  const stats = bus.getStats();
  const renamed = stats.topics.find((t) => t.topic === 'market.btc.v2');
  assert.ok(renamed !== undefined);
  assert.equal(renamed.publishedMessages, 4);
  assert.equal(renamed.lastSeq, 4);
  assert.equal(stats.topics.find((t) => t.topic === 'market.btc'), undefined);
});

test('renameTopic rejects patterns, unknown topics, used targets and same-name renames', () => {
  const bus = new EventBus();
  assert.throws(() => bus.renameTopic('market.*', 'x'), RangeError);
  assert.throws(() => bus.renameTopic('market.**', 'x'), RangeError);
  assert.throws(() => bus.renameTopic('', 'x'), RangeError);
  assert.throws(() => bus.renameTopic('nope', 'x'), RangeError);

  bus.subscribe('taken', () => {});
  bus.publish('taken', 1);
  bus.subscribe('other', () => {});
  assert.throws(() => bus.renameTopic('other', 'taken'), RangeError);
  assert.throws(() => bus.renameTopic('other', 'other'), RangeError);
  assert.throws(() => bus.renameTopic('ns/a', 'ns/b'), RangeError);
});

test('renameTopic leaves wildcard subscriptions untouched', async () => {
  const bus = new EventBus();
  const exact: unknown[] = [];
  const wild: unknown[] = [];
  const exactSub = bus.subscribe('a.b', (msg) => exact.push(msg.payload));
  bus.subscribe('a.**', (msg) => wild.push(msg.payload));
  bus.publish('a.b', 'before');
  await flush();

  bus.renameTopic('a.b', 'a.c');
  bus.publish('a.c', 'after');
  bus.publish('a.b', 'fresh-old-name');
  await flush();
  // The exact subscriber followed the rename (it got 'after' on the new
  // name); the wildcard kept matching both the new name and the freed
  // old name.
  assert.deepEqual(exact, ['before', 'after']);
  assert.deepEqual(wild, ['before', 'after', 'fresh-old-name']);
});

test('renameTopic moves exact-name policy rules with the topic', async () => {
  let now = 0;
  const bus = new EventBus({ now: () => now });
  const received: unknown[] = [];
  bus.subscribe('t.old', (msg) => received.push(msg.payload));
  bus.setTopicTtl('t.old', 100);
  bus.renameTopic('t.old', 't.new');
  // The TTL rule followed the rename: a message published at t=0 expires
  // when the clock jumps past the deadline.
  bus.publish('t.new', 'doomed');
  now = 500;
  await flush();
  assert.deepEqual(received, []);
  const stats = bus.getStats().topics.find((t) => t.topic === 't.new');
  assert.equal(stats?.expiredMessages, 1);
});

test('renameTopic retargets delayed schedules without dropping them', async () => {
  let now = 0;
  const bus = new EventBus({ now: () => now });
  const received: unknown[] = [];
  bus.subscribe('d.old', (msg) => received.push(msg.payload));
  bus.publishDelayed('d.old', 'later', { delayMs: 1000 });
  const result = bus.renameTopic('d.old', 'd.new');
  assert.equal(result.delayedRetargeted, 1);
  now = 1500;
  // The sweep runs on the next publish (the deferred flush path).
  bus.publish('d.new', 'trigger');
  await flush();
  await flush();
  assert.ok(received.includes('later'));
  assert.ok(received.includes('trigger'));
});

test('renameTopic rewrites multi-pattern subscriptions in place', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe(['m.old', 'other'], (msg) => received.push(msg.payload));
  bus.publish('m.old', 'a');
  bus.publish('other', 'b');
  await flush();
  bus.renameTopic('m.old', 'm.new');
  bus.publish('m.new', 'c');
  bus.publish('m.old', 'stale');
  await flush();
  assert.deepEqual(received, ['a', 'b', 'c']);
});

test('renameTopic migrates the durable log: files, seq continuity and replay', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'rename-'));
  try {
    const bus = new EventBus({ durableLogDir: dir });
    const received: unknown[] = [];
    bus.subscribe('log.old', (msg) => received.push(msg.payload));
    bus.publish('log.old', 'one');
    bus.publish('log.old', 'two');
    await flush();
    assert.deepEqual(received, ['one', 'two']);

    const result = bus.renameTopic('log.old', 'log.new');
    assert.equal(result.durableLogMigrated, true);
    assert.ok(result.seqContinuedFrom >= 2);

    const oldFile = path.join(dir, `${encodeURIComponent('log.old')}.log`);
    const newFile = path.join(dir, `${encodeURIComponent('log.new')}.log`);
    assert.equal(fs.existsSync(oldFile), false);
    assert.equal(fs.existsSync(newFile), true);

    // A new subscriber resuming from the start replays the migrated
    // history under the new name.
    const replayed: unknown[] = [];
    bus.subscribe('log.new', (msg) => replayed.push(msg.payload), { resumeFromSeq: 0 });
    await flush();
    assert.deepEqual(replayed, ['one', 'two']);

    // Numbering continues on the new name.
    bus.publish('log.new', 'three');
    await flush();
    assert.deepEqual(received, ['one', 'two', 'three']);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('renameTopic moves retained messages and consumer checkpoints', async () => {
  const bus = new EventBus();
  bus.setTopicRetained('r.old', true);
  bus.publish('r.old', 'retained', { retain: true });
  await flush();
  const groupReceived: unknown[] = [];
  bus.subscribeToGroup('g1', 'r.old', (msg) => groupReceived.push(msg.payload));
  await flush();
  bus.publish('r.old', 'm1');
  await flush();
  bus.commitOffset('g1', 'r.old', 1);

  bus.renameTopic('r.old', 'r.new');

  // The retained message is served under the new name (delivered as a
  // snapshot to new subscribers automatically).
  const retained: unknown[] = [];
  bus.subscribe('r.new', (msg) => retained.push(msg.payload));
  await flush();
  assert.ok(retained.includes('retained'));
  // Checkpoints followed the topic.
  assert.equal(bus.getCommittedOffsets('g1')['r.new'], 1);
  assert.equal(bus.getCommittedOffsets('g1')['r.old'], undefined);
});

test('renameTopic emits a topic_renamed audit event', () => {
  const bus = new EventBus();
  bus.subscribe('au.old', () => {});
  bus.publish('au.old', 1);
  bus.renameTopic('au.old', 'au.new', { actor: 'ops' });
  const events = bus.getSubscriptionAudit({ action: 'topic_renamed' });
  assert.equal(events.length, 1);
  assert.equal(events[0].topic, 'au.new');
  assert.equal(events[0].actor, 'ops');
  assert.deepEqual((events[0].detail as Record<string, unknown>)['from'], 'au.old');
  assert.equal((events[0].detail as Record<string, unknown>)['subscribersRewritten'], 1);
});

test('renameTopic freezes the old and new names for the synchronous call', () => {
  const bus = new EventBus();
  bus.subscribe('fz.old', () => {});
  bus.publish('fz.old', 1);
  // Reach into the private freeze set to simulate the in-rename window:
  // publishes to either name must fail fast, others are unaffected.
  const frozen = bus as unknown as { renameFrozen: Set<string> };
  frozen.renameFrozen.add('fz.old');
  frozen.renameFrozen.add('fz.new');
  try {
    assert.throws(() => bus.publish('fz.old', 'x'), TopicRenameInProgressError);
    assert.throws(() => bus.publish('fz.new', 'x'), TopicRenameInProgressError);
    assert.equal(bus.publish('elsewhere', 'ok'), 0);
  } finally {
    frozen.renameFrozen.delete('fz.old');
    frozen.renameFrozen.delete('fz.new');
  }
  // After the window the rename itself works.
  const result = bus.renameTopic('fz.old', 'fz.new');
  assert.equal(result.subscribersRewritten, 1);
});
