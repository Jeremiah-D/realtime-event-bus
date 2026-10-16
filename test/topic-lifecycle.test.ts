/**
 * Topic lifecycle management (EB-126): `createTopic` provisions a topic's
 * policies in one atomic call; `deleteTopic` tears it down with a
 * tombstone that refuses future publishes, optionally purging durable
 * history.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type BusMessage } from '../src/bus.ts';

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-lifecycle-'));
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const keySecret = 'lifecycle-test-signing-secret';
const filesIn = (dir: string) => readdirSync(dir).sort();

test('createTopic provisions a topic and applies TTL, compression and signing in one call', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ durableLogDir: freshDir(), now: clock.now });
  const received: BusMessage[] = [];
  bus.subscribe('orders.eu', (msg) => received.push(msg));

  const result = bus.createTopic('orders.eu', {
    metadata: { owner: 'payments-svc', description: 'eu orders', tags: ['payments'] },
    ttlMs: 100,
    compression: { thresholdBytes: 10 },
    signing: { keys: [{ kid: 'k1', secret: keySecret }] },
  });
  assert.deepEqual(result, { topic: 'orders.eu', created: true });

  const info = bus.getTopicLifecycle('orders.eu');
  assert.ok(info !== undefined);
  assert.equal(info.provisioned, true);
  assert.equal(info.metadata?.owner, 'payments-svc');
  assert.deepEqual(info.metadata?.tags, ['payments']);

  // All three policies are live: the payload is signed and compressed…
  bus.publish('orders.eu', { id: 1, note: 'a'.repeat(200) });
  await flush();
  assert.equal(received.length, 1);
  assert.equal(received[0].signature?.kid, 'k1');
  const stats = bus.getStats();
  const topicStats = stats.topics.find((t) => t.topic === 'orders.eu');
  assert.ok(topicStats !== undefined);
  assert.equal(topicStats.compressedMessages, 1);

  // …and the TTL is enforced: past the deadline the message expires.
  clock.nowMs = 5_000;
  bus.publish('orders.eu', { id: 2 });
  clock.nowMs = 5_500;
  await flush();
  assert.equal(received.length, 1); // the second message expired
  assert.equal(bus.getStats().expiredMessages, 1);

  const created = bus.getSubscriptionAudit({ action: 'topic_created' });
  assert.equal(created.length, 1);
  assert.equal(created[0].topic, 'orders.eu');
  assert.equal(created[0].detail?.updated, false);
});

test('createTopic retention maps onto the global durable-log age bound', async () => {
  const clock = controllableClock();
  const bus = new EventBus({
    durableLogDir: freshDir(),
    durableLogMaxEntriesPerTopic: 5,
    now: clock.now,
  });
  // Retention is bus-global (EB-85): the option moves the shared bound.
  bus.createTopic('t', { retention: { maxAgeMs: 1_000 } });
  const seen: string[] = [];
  bus.subscribe('t', (msg) => seen.push(msg.payload as string));
  for (let i = 0; i < 6; i++) bus.publish('t', `old-${i}`);
  clock.nowMs = 10_000;
  for (let i = 0; i < 5; i++) bus.publish('t', `new-${i}`);
  await flush();
  assert.equal(bus.getStats().durableLog?.retentionEvicted, 6);
  // …and it throws exactly like setDurableLogMaxAgeMs when misused.
  assert.throws(() => bus.createTopic('x', { retention: { maxAgeMs: 0 } }), RangeError);
  assert.throws(() => bus.createTopic('x', { retention: { maxAgeMs: -5 } }), RangeError);
  const noLog = new EventBus();
  assert.throws(() => noLog.createTopic('x', { retention: { maxAgeMs: 1_000 } }), RangeError);
});

test('createTopic validates names, actor, metadata, and the bundle atomically', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  // Illegal names: empty, non-string, and anything with a wildcard —
  // lifecycle manages concrete topics only.
  for (const bad of ['', '*', 'a*b', '**.deep']) {
    assert.throws(() => bus.createTopic(bad), RangeError, `name ${JSON.stringify(bad)}`);
    assert.throws(() => bus.deleteTopic(bad), RangeError, `name ${JSON.stringify(bad)}`);
  }
  assert.throws(() => bus.createTopic(42 as never), RangeError);
  assert.throws(() => bus.createTopic('t', { actor: '' }), RangeError);
  assert.throws(() => bus.deleteTopic('t', { actor: '' }), RangeError);
  assert.throws(() => bus.createTopic('t', { metadata: { tags: ['ok', ''] } }), RangeError);
  assert.throws(() => bus.createTopic('t', { ttlMs: -1 }), RangeError);
  assert.throws(() => bus.createTopic('t', { compression: { thresholdBytes: 0 } }), RangeError);
  assert.throws(() => bus.createTopic('t', { compression: { thresholdBytes: 10, level: 10 } }), RangeError);
  assert.throws(() => bus.createTopic('t', { signing: { keys: [] } }), RangeError);
  assert.throws(() => bus.createTopic('t', { signing: { keys: [{ kid: 'k1', secret: keySecret }], graceMs: -1 } }), RangeError);

  // Atomicity: a bad bundle applies nothing — the TTL half of this call
  // must not survive the signing failure.
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  assert.throws(
    () => bus.createTopic('t', { ttlMs: 100, signing: { keys: [] } }),
    RangeError,
  );
  assert.equal(bus.getTopicLifecycle('t'), undefined);
  bus.publish('t', 'still-fresh');
  clock.nowMs = 5_000;
  await flush();
  assert.deepEqual(received, ['still-fresh']); // no TTL rule was installed
  assert.equal(bus.getSubscriptionAudit({ action: 'topic_created' }).length, 0);
});

test('createTopic upserts: repeat create updates policies; re-provision after delete starts fresh', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  assert.deepEqual(bus.createTopic('t', { ttlMs: 100 }), { topic: 't', created: true });
  const again = bus.createTopic('t', { ttlMs: 10_000 });
  assert.deepEqual(again, { topic: 't', created: false });
  // The new TTL is the one in force: a message lives past the old deadline.
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publish('t', 'v');
  clock.nowMs = 5_000; // past ttlMs=100, inside ttlMs=10_000
  await flush();
  assert.deepEqual(received, ['v']);

  bus.deleteTopic('t');
  const reprovisioned = bus.createTopic('t', { ttlMs: 50 });
  assert.deepEqual(reprovisioned, { topic: 't', created: true });
  assert.equal(bus.getTopicLifecycle('t')?.provisioned, true);
  // The tombstone is gone: publishes are admitted again.
  assert.equal(bus.publish('t', 'back'), 1);
  await flush();
  assert.deepEqual(received, ['v', 'back']);
});

test('deleteTopic with purge destroys durable state; re-provisioned numbering restarts at 1', async () => {
  const dir = freshDir();
  const clock = controllableClock();
  const bus = new EventBus({
    durableLogDir: dir,
    durableLogSegmentRotation: { maxEntriesPerSegment: 2 },
    now: clock.now,
  });
  bus.createTopic('t', { compression: { thresholdBytes: 10, dictionary: Buffer.from('field-name'.repeat(40)) } });
  for (let i = 0; i < 5; i++) bus.publish('t', { n: i, pad: 'x'.repeat(100) });
  await flush();
  // Wait for the background archive pass to close a segment.
  const segPrefix = `${encodeURIComponent('t')}.seg-`;
  for (let i = 0; i < 200 && !filesIn(dir).some((f) => f.startsWith(segPrefix) && f.endsWith('.jsonl.gz')); i++) {
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  assert.ok(filesIn(dir).some((f) => f === `${encodeURIComponent('t')}.log`));
  assert.ok(filesIn(dir).some((f) => f.startsWith(segPrefix) && f.endsWith('.jsonl.gz')), 'a segment should have archived');

  const result = bus.deleteTopic('t', { purge: true });
  assert.deepEqual(
    { deleted: result.deleted, purged: result.purged, invalidatedSubscriptions: result.invalidatedSubscriptions },
    { deleted: true, purged: true, invalidatedSubscriptions: 0 },
  );
  // Every per-topic file is gone: live log, segments, checksum sidecars.
  assert.ok(!filesIn(dir).some((f) => f.startsWith(encodeURIComponent('t'))), `leftovers: ${filesIn(dir)}`);
  // The history is unrecoverable: a resume on a fresh bus sees nothing.
  const second = new EventBus({ durableLogDir: dir, now: clock.now });
  const replayed: unknown[] = [];
  second.subscribe('t', (msg) => replayed.push(msg.payload), { resumeFromSeq: 0 });
  await flush();
  assert.deepEqual(replayed, []);

  // Counters are zeroed: after re-provisioning, numbering restarts at 1.
  bus.createTopic('t');
  const seen: BusMessage[] = [];
  bus.subscribe('t', (msg) => seen.push(msg));
  bus.publish('t', 'fresh');
  await flush();
  assert.equal(seen.length, 1);
  assert.equal(seen[0].seq, 1);

  const deleted = bus.getSubscriptionAudit({ action: 'topic_deleted' });
  const topicEvent = deleted.find((e) => e.topic === 't' && e.subId === undefined);
  assert.ok(topicEvent !== undefined);
  assert.equal(topicEvent.detail?.purge, true);
});

test('deleteTopic without purge keeps history replayable; a later purge destroys it', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  for (let i = 0; i < 3; i++) bus.publish('t', `m-${i}`);
  await flush();

  const soft = bus.deleteTopic('t');
  assert.equal(soft.deleted, true);
  assert.equal(soft.purged, false);
  // New publishes are refused…
  assert.equal(bus.publish('t', 'nope'), 0);
  // …but the retained history replays, by seq and by time.
  const bySeq: unknown[] = [];
  bus.subscribe('t', (msg) => bySeq.push(msg.payload), { resumeFromSeq: 0 });
  await flush();
  assert.deepEqual(bySeq, ['m-0', 'm-1', 'm-2']);

  // A later purge of the soft-deleted topic destroys the retained history.
  const purged = bus.deleteTopic('t', { purge: true });
  assert.equal(purged.deleted, true);
  assert.equal(purged.purged, true);
  const gone: unknown[] = [];
  bus.subscribe('s', () => {});
  bus.subscribe('t', (msg) => gone.push(msg.payload), { resumeFromSeq: 0 });
  await flush();
  assert.deepEqual(gone, []);
  assert.ok(!filesIn(dir).some((f) => f.startsWith(encodeURIComponent('t'))));
});

test('deleteTopic invalidates wildcard subscriptions through the normal unsubscribe path', async () => {
  const bus = new EventBus();
  const orders: unknown[] = [];
  const billing: unknown[] = [];
  const subOrders = bus.subscribe('orders.*', (msg) => orders.push(msg.payload));
  const subBilling = bus.subscribe('billing.*', (msg) => billing.push(msg.payload));
  const subExact = bus.subscribe('orders.eu', () => {});

  bus.publish('orders.us', 'us-1');
  bus.publish('billing.x', 'b-1');
  await flush();
  assert.deepEqual(orders, ['us-1']);
  assert.deepEqual(billing, ['b-1']);

  const result = bus.deleteTopic('orders.eu');
  assert.equal(result.deleted, true);
  assert.equal(result.invalidatedSubscriptions, 1);

  // The wildcard subscription is gone: a sibling topic no longer delivers.
  bus.publish('orders.us', 'us-2');
  bus.publish('billing.x', 'b-2');
  await flush();
  assert.deepEqual(orders, ['us-1']);
  assert.deepEqual(billing, ['b-1', 'b-2']);

  // The removal went through the normal path: 'unsubscribe' was audited…
  const unsubs = bus.getSubscriptionAudit({ action: 'unsubscribe' });
  assert.ok(unsubs.some((e) => e.pattern === 'orders.*'), 'wildcard unsubscribe audited');
  assert.ok(!unsubs.some((e) => e.pattern === 'billing.*'), 'unrelated pattern untouched');
  assert.ok(!unsubs.some((e) => e.pattern === 'orders.eu'), 'exact subscription left alone');
  // …and each invalidation carries a topic_deleted event naming the sub.
  const invalidations = bus
    .getSubscriptionAudit({ action: 'topic_deleted' })
    .filter((e) => e.subId !== undefined);
  assert.equal(invalidations.length, 1);
  assert.equal(invalidations[0].topic, 'orders.eu');
  assert.equal(invalidations[0].pattern, 'orders.*');
  assert.equal(invalidations[0].detail?.reason, 'wildcard_subscription_invalidated');

  // The exact-topic subscription still exists: unsubscribing it is a clean no-op-free call.
  subBilling.unsubscribe();
});

test('deleteTopic on a group wildcard member fires a real onRebalance leave', async () => {
  const bus = new EventBus();
  const rebalances: Array<{ trigger: string; groupId: string; memberId: string; members: string[] }> = [];
  const capture = (event: { trigger: string; groupId: string; memberId: string; members: string[] }) =>
    rebalances.push({
      trigger: event.trigger,
      groupId: event.groupId,
      memberId: event.memberId,
      members: [...event.members],
    });
  // Two members of the same competing set; both patterns hit the deleted
  // topic, so both are invalidated — the second one observes the first
  // one's leave before its own teardown runs.
  bus.subscribeToGroup('g1', 'orders.*', () => {}, { onRebalance: capture });
  bus.subscribeToGroup('g1', 'orders.*', () => {}, { onRebalance: capture });
  assert.equal(rebalances.length, 3); // join A (A notified), join B (A and B notified)
  await flush();

  const result = bus.deleteTopic('orders.eu');
  assert.equal(result.invalidatedSubscriptions, 2);
  const leave = rebalances.find((r) => r.trigger === 'leave');
  assert.ok(leave !== undefined, 'a real GroupRebalanceEvent fired');
  assert.equal(leave.groupId, 'g1');
  assert.equal(leave.members.length, 1); // the survivor's roster, post-leave
  const leaveAudit = bus.getSubscriptionAudit({ action: 'rebalance' });
  assert.ok(
    leaveAudit.some((e) => e.groupId === 'g1' && e.detail?.trigger === 'leave'),
    'group leave audited as rebalance',
  );
});

test('publishes to a deleted topic are refused with alias-retired and consume nothing', async () => {
  const bus = new EventBus();
  const rejected: Array<{ topic: string; reason: string }> = [];
  const hooked = new EventBus({
    onAdmissionRejected: (event) => rejected.push({ topic: event.topic, reason: event.reason }),
  });
  for (const b of [bus, hooked]) {
    b.publish('t', 'before');
    b.deleteTopic('t');
  }
  // publish() sheds the message…
  assert.equal(bus.publish('t', 'after'), 0);
  assert.equal(hooked.publish('t', 'after'), 0);
  assert.deepEqual(rejected, [{ topic: 't', reason: 'alias-retired' }]);

  // …publishWithConfirmation reports the rejection receipt…
  const receipt = await bus.publishWithConfirmation('t', 'after');
  assert.equal(receipt.admitted, false);
  if (!receipt.admitted) assert.equal(receipt.reason, 'alias-retired');

  // …previewPublish names the topic-lifecycle gate…
  assert.deepEqual(bus.previewPublish('t', 'after'), {
    admitted: false,
    reason: 'alias-retired',
    failedGate: 'topic-lifecycle',
  });

  // …publishAtomic aborts the batch on the deleted entry…
  const atomic = bus.publishAtomic([{ topic: 't', payload: 'x' }]);
  assert.equal(atomic.published, 0);
  assert.equal(atomic.rejected?.reason, 'alias-retired');

  // …and nothing was consumed: the per-topic stats count the refusal,
  // and a re-provisioned topic numbers from 1 again.
  const topicStats = bus.getStats().topics.find((t) => t.topic === 't');
  assert.ok(topicStats !== undefined);
  assert.ok(topicStats.aliasRetiredMessages >= 1);
  bus.createTopic('t');
  const seen: BusMessage[] = [];
  bus.subscribe('t', (msg) => seen.push(msg));
  bus.publish('t', 'reborn');
  await flush();
  assert.equal(seen[0].seq, 1);
});

test('deleteTopic on an unknown topic is a no-op', () => {
  const bus = new EventBus();
  const result = bus.deleteTopic('never-existed');
  assert.deepEqual(result, {
    topic: 'never-existed',
    deleted: false,
    purged: false,
    invalidatedSubscriptions: 0,
  });
  assert.equal(bus.getSubscriptionAudit({ action: 'topic_deleted' }).length, 0);
  assert.equal(bus.getTopicLifecycle('never-existed'), undefined);
});

test('deleteTopic cancels pending delayed schedules for the topic', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const delayId = bus.publishDelayed('t', 'late', { delayMs: 60_000 });
  assert.ok(typeof delayId === 'string');
  assert.equal(bus.getStats().pendingDelayed, 1);

  const result = bus.deleteTopic('t');
  assert.equal(result.deleted, true);
  assert.equal(bus.getStats().pendingDelayed, 0);

  // Advancing past the due time fans out nothing: the schedule is gone.
  clock.nowMs += 120_000;
  const received: unknown[] = [];
  bus.subscribe('other', (msg) => received.push(msg.payload));
  bus.publish('other', 'poke'); // any publish triggers the flush sweep
  await flush();
  assert.deepEqual(received, ['poke']);
  const cancelled = bus.getSubscriptionAudit({ action: 'delayed_cancelled' });
  assert.ok(cancelled.some((e) => (e.detail?.cancelled as number) >= 1));
});

test('deleteTopic clears the compression rule: a re-provisioned topic starts uncompressed', async () => {
  const bus = new EventBus();
  const big = { pad: 'x'.repeat(500) };
  bus.createTopic('t', { compression: { thresholdBytes: 10 } });
  bus.publish('t', big);
  const before = bus.getStats().topics.find((t) => t.topic === 't');
  assert.equal(before?.compressedMessages, 1);

  bus.deleteTopic('t');
  bus.createTopic('t'); // no compression this time
  const received: BusMessage[] = [];
  bus.subscribe('t', (msg) => received.push(msg));
  bus.publish('t', big);
  await flush();
  assert.equal(received.length, 1);
  const after = bus.getStats().topics.find((t) => t.topic === 't');
  assert.equal(after?.compressedMessages, 1, 'only the pre-delete generation compressed');
});
