import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, REDACTION_SENTINEL, type BusMessage } from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-redact-'));

test('redaction masks nested field paths with "***" without mutating the caller object', async () => {
  const bus = new EventBus();
  bus.setTopicRedaction('users.**', ['user.email', 'user.profile.ssn']);
  const received: BusMessage[] = [];
  bus.subscribe('users.**', (m) => received.push(m));
  const payload = {
    user: { email: 'alice@example.com', name: 'alice', profile: { ssn: '123-45-6789', city: 'SF' } },
    event: 'signup',
  };
  bus.publish('users.signup', payload);
  await flush();
  assert.equal(received.length, 1);
  assert.deepEqual(received[0].payload, {
    user: { email: REDACTION_SENTINEL, name: 'alice', profile: { ssn: REDACTION_SENTINEL, city: 'SF' } },
    event: 'signup',
  });
  // The caller's object is never mutated by redaction.
  assert.deepEqual(payload.user.email, 'alice@example.com');
  assert.deepEqual(payload.user.profile.ssn, '123-45-6789');
});

test('redaction runs before schema validation: the schema sees the redacted payload', () => {
  const bus = new EventBus();
  // The validator requires an email-shaped value. Redaction replaces the
  // real email with "***" first, so the schema gate must reject — it can
  // never depend on the original value.
  bus.setTopicSchema('t', (p) => typeof (p as any).email === 'string' && (p as any).email.includes('@'));
  bus.setTopicRedaction('t', ['email']);
  const accepted = bus.publish('t', { email: 'real@example.com' });
  assert.equal(accepted, 0);
  const stats = bus.getStats();
  assert.equal(stats.topics[0].rejectedMessages, 1);
  // The redaction itself still counted: the replacement happened.
  assert.equal(stats.topics[0].redactedFields, 1);
  assert.equal(stats.redactedFields, 1);
});

test('redaction counters: per-topic and global redactedFields', async () => {
  const bus = new EventBus();
  bus.setTopicRedaction('a.**', ['secret']);
  bus.setTopicRedaction('b.**', ['k1', 'k2']);
  const received: unknown[] = [];
  bus.subscribe('**', (m) => received.push(m.payload));
  bus.publish('a.x', { secret: 's1', other: 1 });
  bus.publish('a.y', { secret: 's2' });
  bus.publish('b.z', { k1: 1, k2: 2, k3: 3 });
  await flush();
  assert.equal(received.length, 3);
  const stats = bus.getStats();
  const byTopic = Object.fromEntries(stats.topics.map((t) => [t.topic, t]));
  assert.equal(byTopic['a.x'].redactedFields, 1);
  assert.equal(byTopic['a.y'].redactedFields, 1);
  assert.equal(byTopic['b.z'].redactedFields, 2);
  assert.equal(stats.redactedFields, 4);
  // A missing path matches nothing and counts nothing.
  bus.publish('a.x', { other: 'no secret here' });
  assert.equal(bus.getStats().redactedFields, 4);
});

test('non-object payloads pass through untouched and uncounted', async () => {
  const bus = new EventBus();
  bus.setTopicRedaction('t', ['secret']);
  const received: unknown[] = [];
  bus.subscribe('t', (m) => received.push(m.payload));
  bus.publish('t', 42);
  bus.publish('t', 'a string');
  bus.publish('t', [1, 2, 3]);
  bus.publish('t', null);
  await flush();
  assert.deepEqual(received, [42, 'a string', [1, 2, 3], null]);
  const stats = bus.getStats();
  assert.equal(stats.topics[0].redactedFields, 0);
  assert.equal(stats.redactedFields, 0);
});

test('redaction is off by default: fully backward compatible', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (m) => received.push(m.payload));
  bus.publish('t', { email: 'real@example.com' });
  await flush();
  assert.deepEqual(received, [{ email: 'real@example.com' }]);
  const stats = bus.getStats();
  assert.equal(stats.topics[0].redactedFields, 0);
  assert.equal(stats.redactedFields, 0);
});

test('setTopicRedaction fail-fast: illegal field paths throw RangeError', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setTopicRedaction('', ['a']), RangeError);
  assert.throws(() => bus.setTopicRedaction('t', ['']), RangeError);
  assert.throws(() => bus.setTopicRedaction('t', ['ok', '']), RangeError);
  assert.throws(() => bus.setTopicRedaction('t', ['a..b']), RangeError);
  assert.throws(() => bus.setTopicRedaction('t', ['.a']), RangeError);
  assert.throws(() => bus.setTopicRedaction('t', ['a.']), RangeError);
  assert.throws(() => bus.setTopicRedaction('t', [123 as any]), RangeError);
  assert.throws(() => bus.setTopicRedaction('t', null as any), RangeError);
  assert.throws(() => bus.setTopicRedaction('t', ['__proto__.x']), RangeError);
  assert.throws(() => bus.setTopicRedaction('t', ['constructor.x']), RangeError);
  // A failed registration leaves no rule behind.
  bus.publish('t', { a: 1 });
  assert.equal(bus.getStats().redactedFields, 0);
});

test('pattern precedence: exact wins over pattern, earliest pattern wins', async () => {
  const bus = new EventBus();
  bus.setTopicRedaction('a.*', ['wide']);
  bus.setTopicRedaction('a.b', ['narrow']);
  bus.setTopicRedaction('c.*', ['first']);
  bus.setTopicRedaction('c.*', ['second']); // re-setting replaces
  const received: unknown[] = [];
  bus.subscribe('**', (m) => received.push(m.payload));
  bus.publish('a.b', { wide: 1, narrow: 2 });
  bus.publish('a.c', { wide: 1, narrow: 2 });
  bus.publish('c.d', { first: 1, second: 2 });
  await flush();
  assert.deepEqual(received[0], { wide: 1, narrow: REDACTION_SENTINEL });
  assert.deepEqual(received[1], { wide: REDACTION_SENTINEL, narrow: 2 });
  assert.deepEqual(received[2], { first: 1, second: REDACTION_SENTINEL });
});

test('clearTopicRedaction removes the rule; new publishes are unaffected', async () => {
  const bus = new EventBus();
  bus.setTopicRedaction('t', ['secret']);
  assert.equal(bus.clearTopicRedaction('t'), true);
  assert.equal(bus.clearTopicRedaction('t'), false);
  const received: unknown[] = [];
  bus.subscribe('t', (m) => received.push(m.payload));
  bus.publish('t', { secret: 's' });
  await flush();
  assert.deepEqual(received, [{ secret: 's' }]);
  assert.equal(bus.getStats().redactedFields, 0);
});

test('publishBatch redacts every entry before admission', async () => {
  const bus = new EventBus();
  bus.setTopicRedaction('t', ['secret']);
  const received: unknown[] = [];
  bus.subscribe('t', (m) => received.push(m.payload));
  const accepted = bus.publishBatch([
    { topic: 't', payload: { secret: 'a' } },
    { topic: 't', payload: { secret: 'b' } },
  ]);
  assert.equal(accepted, 2);
  await flush();
  assert.deepEqual(received, [{ secret: REDACTION_SENTINEL }, { secret: REDACTION_SENTINEL }]);
  assert.equal(bus.getStats().redactedFields, 2);
});

test('publishAtomic redacts in shadow admission and counts once per entry', async () => {
  const bus = new EventBus();
  bus.setTopicRedaction('t', ['secret']);
  const received: unknown[] = [];
  bus.subscribe('t', (m) => received.push(m.payload));
  const result = bus.publishAtomic([
    { topic: 't', payload: { secret: 'a' } },
    { topic: 't', payload: { secret: 'b' } },
  ]);
  assert.deepEqual(result, { published: 2 });
  await flush();
  // The commit phase fans out the redacted bytes (pre-admitted) — the
  // count moved once per entry in shadow admission, never twice.
  assert.deepEqual(received, [{ secret: REDACTION_SENTINEL }, { secret: REDACTION_SENTINEL }]);
  assert.equal(bus.getStats().redactedFields, 2);
});

test('publishDelayed redacts at schedule time: the persisted schedule is redacted', async () => {
  const clock = controllableClock();
  const dir = freshDir();
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  bus.setTopicRedaction('t', ['secret']);
  const received: unknown[] = [];
  bus.subscribe('t', (m) => received.push(m.payload));
  const id = bus.publishDelayed('t', { secret: 'sched-secret' }, { delayMs: 1000 });
  assert.match(id as string, /^delayed-\d+$/);
  // The persisted schedule record carries the redacted payload.
  const log = DurableTopicLog.open({ dir });
  const records = log.readSince('t', -1);
  assert.equal(records.length, 1);
  assert.deepEqual(records[0].payload, { secret: REDACTION_SENTINEL });
  // The due-time fan-out delivers the redacted bytes (no re-redaction).
  clock.nowMs = 2_000;
  bus.publish('t', 'poke'); // any publish triggers the flush whose sweep fans out due entries
  await flush();
  assert.deepEqual(received, ['poke', { secret: REDACTION_SENTINEL }]);
  assert.equal(bus.getStats().redactedFields, 1);
});

test('durable log persists redacted bytes: replay never resurrects the original', async () => {
  const clock = controllableClock();
  const dir = freshDir();
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  bus.setTopicRedaction('t', ['email']);
  bus.publish('t', { email: 'real@example.com', id: 7 });
  // The log on disk holds the redacted payload.
  const log = DurableTopicLog.open({ dir });
  const records = log.readSince('t', 0);
  assert.equal(records.length, 1);
  assert.deepEqual(records[0].payload, { email: REDACTION_SENTINEL, id: 7 });
  // A resubscribing consumer replays the redacted bytes, not the original.
  const resumed: unknown[] = [];
  bus.subscribe('t', (m) => resumed.push(m.payload), { resumeFromSeq: 0 });
  await flush();
  assert.deepEqual(resumed, [{ email: REDACTION_SENTINEL, id: 7 }]);
});

test('route downstream sees redacted data at the destination', async () => {
  const bus = new EventBus();
  bus.setTopicRoute('src.topic', 'dst.topic');
  bus.setTopicRedaction('dst.topic', ['secret']);
  const received: unknown[] = [];
  bus.subscribe('dst.topic', (m) => received.push(m.payload));
  bus.publish('src.topic', { secret: 's', other: 1 });
  await flush();
  assert.deepEqual(received, [{ secret: REDACTION_SENTINEL, other: 1 }]);
  // The source topic's own stats carry no redaction count; the
  // destination's do (dst admission ran its own rule).
  const stats = bus.getStats();
  const byTopic = Object.fromEntries(stats.topics.map((t) => [t.topic, t]));
  assert.equal(byTopic['src.topic'].redactedFields, 0);
  assert.equal(byTopic['dst.topic'].redactedFields, 1);
});

test('redaction is idempotent: a field already holding "***" is not counted again', async () => {
  const bus = new EventBus();
  bus.setTopicRedaction('t', ['secret']);
  const received: unknown[] = [];
  bus.subscribe('t', (m) => received.push(m.payload));
  bus.publish('t', { secret: REDACTION_SENTINEL });
  await flush();
  assert.deepEqual(received, [{ secret: REDACTION_SENTINEL }]);
  assert.equal(bus.getStats().redactedFields, 0);
});

test('redaction composes with compression: subscribers inflate the redacted payload', async () => {
  const bus = new EventBus();
  bus.setTopicRedaction('t', ['secret']);
  bus.setTopicCompression('t', { thresholdBytes: 10 });
  const received: unknown[] = [];
  bus.subscribe('t', (m) => received.push(m.payload));
  bus.publish('t', { secret: 'a-very-long-secret-value', padding: 'x'.repeat(200) });
  await flush();
  assert.equal(received.length, 1);
  const payload = received[0] as Record<string, unknown>;
  assert.equal(payload.secret, REDACTION_SENTINEL);
  assert.equal(payload.padding, 'x'.repeat(200));
  const stats = bus.getStats();
  assert.equal(stats.topics[0].compressedMessages, 1);
  assert.equal(stats.topics[0].redactedFields, 1);
});

test('array element paths are unsupported: arrays are opaque to redaction', async () => {
  const bus = new EventBus();
  bus.setTopicRedaction('t', ['users.email']);
  const received: unknown[] = [];
  bus.subscribe('t', (m) => received.push(m.payload));
  bus.publish('t', { users: [{ email: 'a@x.com' }] });
  await flush();
  assert.deepEqual(received, [{ users: [{ email: 'a@x.com' }] }]);
  assert.equal(bus.getStats().redactedFields, 0);
});
