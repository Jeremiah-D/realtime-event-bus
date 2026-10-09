import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  EventBus,
  type AdmissionRejectionEvent,
  type BusMessage,
} from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-alias-'));

function collect() {
  const messages: BusMessage[] = [];
  return {
    messages,
    handler: (msg: BusMessage) => {
      messages.push(msg);
    },
  };
}

test('basic alias: publishes to the new topic reach old-topic subscribers', async () => {
  const bus = new EventBus();
  bus.setTopicAlias('orders.v1', 'orders.v2');
  const old = collect();
  const current = collect();
  bus.subscribe('orders.v1', old.handler);
  bus.subscribe('orders.v2', current.handler);
  const accepted = bus.publish('orders.v2', { id: 1 });
  await flush();
  assert.equal(accepted, 2);
  assert.equal(old.messages.length, 1);
  assert.equal(current.messages.length, 1);
  // The mirrored message keeps the resolved topic: one admitted message,
  // one (topic, seq) identity for both sides of the migration.
  assert.equal(old.messages[0].topic, 'orders.v2');
  assert.equal(old.messages[0].seq, current.messages[0].seq);
  assert.deepEqual(old.messages[0].payload, { id: 1 });
});

test('publishes to the old topic resolve to the new topic while the alias is live', async () => {
  const bus = new EventBus();
  bus.setTopicAlias('orders.v1', 'orders.v2');
  const current = collect();
  bus.subscribe('orders.v2', current.handler);
  const accepted = bus.publish('orders.v1', { id: 7 });
  await flush();
  assert.equal(accepted, 1);
  assert.equal(current.messages.length, 1);
  assert.equal(current.messages[0].topic, 'orders.v2');
  assert.equal(current.messages[0].seq, 1);
  // Stats, seq and the durable identity all key off the resolved topic.
  const stats = bus.getStats();
  const v2 = stats.topics.find((t) => t.topic === 'orders.v2');
  assert.equal(v2?.publishedMessages, 1);
  assert.equal(v2?.lastSeq, 1);
  assert.equal(
    stats.topics.find((t) => t.topic === 'orders.v1')?.publishedMessages ?? 0,
    0,
  );
});

test('no double delivery when one subscriber matches via old and new patterns', async () => {
  const bus = new EventBus();
  bus.setTopicAlias('orders.v1', 'orders.v2');
  const star = collect();
  const v1 = collect();
  const v2 = collect();
  bus.subscribe('**', star.handler);
  bus.subscribe('orders.v1', v1.handler);
  bus.subscribe('orders.v2', v2.handler);
  const accepted = bus.publish('orders.v2', { id: 1 });
  await flush();
  // Three subscriptions, one delivery each: the single fan-out pass tests
  // the resolved topic and the aliased old topic together, so '**' — which
  // matches via both — is still visited exactly once.
  assert.equal(accepted, 3);
  assert.equal(star.messages.length, 1);
  assert.equal(v1.messages.length, 1);
  assert.equal(v2.messages.length, 1);
  assert.equal(star.messages[0].topic, 'orders.v2');
});

test('self-alias and alias cycles throw RangeError, before mutating anything', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setTopicAlias('x', 'x'), RangeError);
  bus.setTopicAlias('a', 'b');
  assert.throws(() => bus.setTopicAlias('b', 'a'), RangeError);
  bus.setTopicAlias('m', 'n');
  bus.setTopicAlias('n', 'o');
  assert.throws(() => bus.setTopicAlias('o', 'm'), RangeError);
  // The rejected registrations left no aliases behind.
  assert.deepEqual(
    bus.getStats().aliases.map((a) => [a.oldTopic, a.newTopic]),
    [
      ['a', 'b'],
      ['m', 'n'],
      ['n', 'o'],
    ],
  );
});

test('invalid alias registrations throw RangeError', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setTopicAlias('', 'b'), RangeError);
  assert.throws(() => bus.setTopicAlias('a', ''), RangeError);
  assert.throws(() => bus.setTopicAlias('a', 'b', { ttlMs: -1 }), RangeError);
  assert.throws(() => bus.setTopicAlias('a', 'b', { ttlMs: NaN }), RangeError);
  assert.throws(() => bus.setTopicAlias('a', 'b', { ttlMs: Infinity }), RangeError);
  assert.deepEqual(bus.getStats().aliases, []);
});

test('alias chains forward through the whole live chain', async () => {
  const bus = new EventBus();
  bus.setTopicAlias('a', 'b');
  bus.setTopicAlias('b', 'c');
  const gotA = collect();
  const gotB = collect();
  const gotC = collect();
  bus.subscribe('a', gotA.handler);
  bus.subscribe('b', gotB.handler);
  bus.subscribe('c', gotC.handler);
  const accepted = bus.publish('a', 'x');
  await flush();
  assert.equal(accepted, 3);
  for (const got of [gotA, gotB, gotC]) {
    assert.equal(got.messages.length, 1);
    assert.equal(got.messages[0].topic, 'c');
    assert.equal(got.messages[0].payload, 'x');
  }
  // Mid-chain publishes resolve to the final target too.
  const acceptedMid = bus.publish('b', 'y');
  await flush();
  assert.equal(acceptedMid, 3);
  assert.equal(gotC.messages[1].payload, 'y');
});

test('expired alias makes the old topic read-only (alias-retired)', async () => {
  const clock = controllableClock();
  const events: AdmissionRejectionEvent[] = [];
  const bus = new EventBus({ now: clock.now, onAdmissionRejected: (e) => events.push(e) });
  bus.setTopicAlias('old', 'new', { ttlMs: 100 });
  const oldSub = collect();
  const newSub = collect();
  bus.subscribe('old', oldSub.handler);
  bus.subscribe('new', newSub.handler);
  clock.nowMs += 101; // the alias TTL lapses
  assert.equal(bus.publish('old', 'late'), 0);
  await flush();
  assert.deepEqual(oldSub.messages, []);
  assert.deepEqual(newSub.messages, []);
  // Rejection surfaces on the hook with reason 'alias-retired' and is
  // counted on the topic and the global total — reconcilable like every
  // other admission rejection.
  assert.equal(events.length, 1);
  assert.equal(events[0].topic, 'old');
  assert.equal(events[0].reason, 'alias-retired');
  const stats = bus.getStats();
  assert.equal(
    stats.topics.find((t) => t.topic === 'old')?.aliasRetiredMessages,
    1,
  );
  assert.equal(stats.aliasRetiredMessages, 1);
  // No sequence number was consumed: the next publish on the new topic
  // still starts at seq 1.
  bus.publish('new', 'z');
  await flush();
  assert.equal(newSub.messages.length, 1);
  assert.equal(newSub.messages[0].seq, 1);
});

test('an expired alias no longer mirrors new-topic publishes to old subscribers', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicAlias('old', 'new', { ttlMs: 100 });
  const oldSub = collect();
  bus.subscribe('old', oldSub.handler);
  bus.publish('new', 'a');
  await flush();
  assert.equal(oldSub.messages.length, 1);
  clock.nowMs += 101;
  bus.publish('new', 'b');
  await flush();
  assert.equal(oldSub.messages.length, 1); // the mirror is gone with the alias
});

test('getStats().aliases exposes the alias table and expiry', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicAlias('o1', 'n1');
  bus.setTopicAlias('o2', 'n2', { ttlMs: 50 });
  assert.deepEqual(bus.getStats().aliases, [
    { oldTopic: 'o1', newTopic: 'n1', expired: false },
    { oldTopic: 'o2', newTopic: 'n2', expiresAt: 1_050, expired: false },
  ]);
  clock.nowMs = 1_051;
  const aliases = bus.getStats().aliases;
  assert.equal(aliases[0].expired, false);
  assert.equal(aliases[1].expired, true);
});

test('clearTopicAlias removes the alias and stops resolution and mirroring', async () => {
  const bus = new EventBus();
  bus.setTopicAlias('old', 'new');
  const oldSub = collect();
  const newSub = collect();
  bus.subscribe('old', oldSub.handler);
  bus.subscribe('new', newSub.handler);
  bus.publish('new', 'a');
  await flush();
  assert.equal(oldSub.messages.length, 1);
  assert.equal(bus.clearTopicAlias('old'), true);
  assert.equal(bus.clearTopicAlias('old'), false);
  assert.deepEqual(bus.getStats().aliases, []);
  // After clearing, the old topic is ordinary again: no more mirroring,
  // and publishes to it stay on it.
  bus.publish('new', 'b');
  bus.publish('old', 'c');
  await flush();
  assert.equal(oldSub.messages.length, 2); // only the direct 'old' publish
  assert.equal(oldSub.messages[1].topic, 'old');
  assert.equal(oldSub.messages[1].payload, 'c');
  assert.equal(newSub.messages.length, 2);
});

test('re-registering an alias replaces it and restarts the TTL', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicAlias('o', 'n1', { ttlMs: 100 });
  clock.nowMs += 60;
  bus.setTopicAlias('o', 'n2', { ttlMs: 100 }); // replaces; new expiry at t=1160
  const n2 = collect();
  bus.subscribe('n2', n2.handler);
  clock.nowMs += 60; // t=1120: past the first TTL, inside the second
  assert.equal(bus.publish('o', 'x'), 1);
  await flush();
  assert.equal(n2.messages.length, 1);
  assert.equal(n2.messages[0].topic, 'n2');
  assert.deepEqual(
    bus.getStats().aliases.map((a) => [a.oldTopic, a.newTopic]),
    [['o', 'n2']],
  );
});

test('alias resolution runs before the ACL gate', async () => {
  const bus = new EventBus();
  bus.setAclRules([{ pattern: 'old', publish: 'deny' }]);
  const sub = collect();
  bus.subscribe('new', sub.handler);
  // Without an alias the ACL denies the old name.
  assert.equal(bus.publish('old', 'blocked'), 0);
  bus.setTopicAlias('old', 'new');
  // With the alias the publish resolves first, so the ACL judges the
  // resolved topic — the deny rule on the old name no longer applies.
  assert.equal(bus.publish('old', 'redirected'), 1);
  await flush();
  assert.equal(sub.messages.length, 1);
  assert.equal(sub.messages[0].topic, 'new');
});

test('durable-log replay attributes records to the alias-resolved topic', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, durableLogDir: freshDir() });
  bus.publish('old', 'before-alias'); // logged under 'old': pre-migration history
  clock.nowMs += 10;
  bus.setTopicAlias('old', 'new');
  bus.publish('new', 'after-alias'); // logged under 'new'
  // A new consumer subscribing with the NEW topic name replays the old
  // history (attributed via the resolved topic)...
  const byNew = collect();
  bus.subscribe('new', byNew.handler, { resumeFromSeq: 0 });
  await flush();
  assert.deepEqual(
    byNew.messages.map((m) => [m.topic, m.payload]),
    [
      ['old', 'before-alias'],
      ['new', 'after-alias'],
    ],
  );
  // ...and an old-topic subscriber replays the mirrored new-topic history.
  const byOld = collect();
  bus.subscribe('old', byOld.handler, { resumeFromSeq: 0 });
  await flush();
  assert.deepEqual(
    byOld.messages.map((m) => [m.topic, m.payload]),
    [
      ['old', 'before-alias'],
      ['new', 'after-alias'],
    ],
  );
});

test('resumeFromTime replay is alias-aware too', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, durableLogDir: freshDir() });
  bus.publish('old', 'pre'); // at=1000, logged under 'old'
  clock.nowMs = 2_000;
  bus.setTopicAlias('old', 'new');
  bus.publish('old', 'post'); // at=2000, resolved and logged under 'new'
  const byNew = collect();
  bus.subscribe('new', byNew.handler, { resumeFromTime: 1_500 });
  await flush();
  assert.deepEqual(
    byNew.messages.map((m) => [m.topic, m.payload]),
    [['new', 'post']],
  );
  const byOld = collect();
  bus.subscribe('old', byOld.handler, { resumeFromTime: 1_500 });
  await flush();
  assert.deepEqual(
    byOld.messages.map((m) => [m.topic, m.payload]),
    [['new', 'post']],
  );
});

test('publishIdempotent and publishAtomic honor aliases', async () => {
  const bus = new EventBus();
  bus.setTopicAlias('old', 'new');
  const sub = collect();
  bus.subscribe('new', sub.handler);
  // The dedup identity is (resolved topic, messageId): the same messageId
  // through either name is the same publish.
  const first = bus.publishIdempotent('old', 'p', { messageId: 'm1' });
  assert.deepEqual(first, { duplicate: false, accepted: 1 });
  const retry = bus.publishIdempotent('new', 'p', { messageId: 'm1' });
  assert.deepEqual(retry, { duplicate: true, accepted: 0 });
  await flush();
  assert.equal(sub.messages.length, 1);
  assert.equal(sub.messages[0].topic, 'new');
  // Atomic batches resolve entry topics before the shadow admission.
  const res = bus.publishAtomic([
    { topic: 'old', payload: 'a' },
    { topic: 'new', payload: 'b' },
  ]);
  assert.deepEqual(res, { published: 2 });
  await flush();
  assert.equal(sub.messages.length, 3);
  assert.deepEqual(
    sub.messages.slice(1).map((m) => m.topic),
    ['new', 'new'],
  );
});

test('publishAtomic and publishDelayed refuse retired old topics', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicAlias('old', 'new', { ttlMs: 100 });
  clock.nowMs += 101;
  const atomic = bus.publishAtomic([{ topic: 'old', payload: 'x' }]);
  assert.deepEqual(atomic, {
    published: 0,
    rejected: { index: 0, topic: 'old', reason: 'alias-retired' },
  });
  assert.equal(bus.getStats().aliasRetiredMessages, 1);
  // publishDelayed resolves at schedule time: a retired old topic never
  // becomes a scheduled delivery.
  assert.equal(bus.publishDelayed('old', 'y', { delayMs: 10 }), undefined);
  assert.equal(bus.getStats().aliasRetiredMessages, 2);
  await flush();
});

test('publishDelayed resolves the alias at schedule time', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicAlias('old', 'new');
  const received: BusMessage[] = [];
  bus.subscribe('new', (m) => received.push(m));
  bus.publishDelayed('old', 'late', { delayMs: 1_000 });
  clock.nowMs = 2_000;
  bus.publish('new', 'poke'); // any publish triggers the flush whose sweep fans out due entries
  await flush();
  assert.deepEqual(
    received.map((m) => [m.topic, m.payload]),
    [
      ['new', 'poke'],
      ['new', 'late'],
    ],
  );
});
