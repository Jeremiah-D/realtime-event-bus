import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { existsSync, mkdtempSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  EventBus,
  type AdmissionRejectionEvent,
  type ConfirmationReceipt,
} from '../src/bus.ts';

/** Injectable clock so receipt timestamps are deterministic. */
function makeClock(startMs = 7_000) {
  let nowMs = startMs;
  return {
    now: () => nowMs,
    advance: (ms: number) => {
      nowMs += ms;
    },
  };
}

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-confirm-'));

const sha256hex = (s: string | Uint8Array) => createHash('sha256').update(s).digest('hex');
const jsonHash = (payload: unknown) => sha256hex(JSON.stringify(payload));

/** Collects admission-rejection hook events in order. */
function makeSink() {
  const events: AdmissionRejectionEvent[] = [];
  const hook = (event: AdmissionRejectionEvent) => {
    events.push(event);
  };
  return { events, hook };
}

function assertAdmissionShape(r: ConfirmationReceipt, topic: string, seq: number, atMs: number) {
  assert.equal(r.admitted, true);
  assert.deepEqual(Object.keys(r).sort(), [
    'admitted',
    'admittedAtMs',
    'payloadSha256',
    'seq',
    'topic',
  ]);
  if (r.admitted !== true) throw new Error('unreachable');
  assert.equal(r.topic, topic);
  assert.equal(r.seq, seq);
  assert.equal(r.admittedAtMs, atMs);
  assert.match(r.payloadSha256, /^[0-9a-f]{64}$/);
}

function assertRejectionShape(
  r: ConfirmationReceipt,
  topic: string,
  reason: string,
  atMs: number,
) {
  assert.equal(r.admitted, false);
  assert.deepEqual(Object.keys(r).sort(), ['admitted', 'reason', 'rejectedAtMs', 'topic']);
  if (r.admitted !== false) throw new Error('unreachable');
  assert.equal(r.topic, topic);
  assert.equal(r.reason, reason);
  assert.equal(r.rejectedAtMs, atMs);
}

test('admitted receipt carries topic, seq, bus-clock timestamp and content hash', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const payload = { id: 1, sku: 'abc' };
  const r = await bus.publishWithConfirmation('orders', payload);
  assertAdmissionShape(r, 'orders', 1, 7_000);
  // The digest is publisher-reproducible: sha256 of the canonical JSON bytes.
  assert.equal(r.admitted === true && r.payloadSha256, jsonHash(payload));
});

test('seq is monotonic per topic across confirmed publishes', async () => {
  const bus = new EventBus();
  const a = await bus.publishWithConfirmation('t', 'x');
  const b = await bus.publishWithConfirmation('t', 'y');
  const c = await bus.publishWithConfirmation('t', 'z');
  assert.equal(a.admitted && a.seq, 1);
  assert.equal(b.admitted && b.seq, 2);
  assert.equal(c.admitted && c.seq, 3);
  // A different topic has its own sequence space.
  const other = await bus.publishWithConfirmation('u', 'x');
  assert.equal(other.admitted && other.seq, 1);
});

test('waitForDurable: receipt resolves only after the record is on disk', async () => {
  const dir = freshDir();
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  const payload = { id: 9 };
  const r = await bus.publishWithConfirmation('t', payload, { waitForDurable: true });
  assertAdmissionShape(r, 't', 1, 7_000);
  // The topic's log file is visible with the record landed.
  const logPath = join(dir, `${encodeURIComponent('t')}.log`);
  assert.ok(existsSync(logPath), 'durable log file exists after resolve');
  const lines = readFileSync(logPath, 'utf8').trim().split('\n');
  assert.equal(lines.length, 1);
  const record = JSON.parse(lines[0]);
  assert.equal(record.seq, 1);
  assert.equal(record.topic, 't');
  assert.deepEqual(record.payload, payload);
});

test('waitForDurable without a durable log resolves immediately as durable', async () => {
  const bus = new EventBus();
  const r = await bus.publishWithConfirmation('t', { a: 1 }, { waitForDurable: true });
  assert.equal(r.admitted, true);
  if (r.admitted === true) assert.equal(r.seq, 1);
});

test('schema rejection returns a rejection receipt', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicSchema('t', (p) => typeof p === 'string');
  const r = await bus.publishWithConfirmation('t', 42);
  assertRejectionShape(r, 't', 'schema', 7_000);
});

test('message-too-big rejection returns a rejection receipt', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicMaxMessageBytes('t', 10);
  const r = await bus.publishWithConfirmation('t', '0123456789abcdef');
  assertRejectionShape(r, 't', 'message-too-big', 7_000);
});

test('payload-too-large rejection returns a rejection receipt', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicMaxPayloadBytes('t', 10);
  const r = await bus.publishWithConfirmation('t', '0123456789abcdef');
  assertRejectionShape(r, 't', 'payload-too-large', 7_000);
});

test('rate-limit shed returns a rejection receipt', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicRateLimit('hot', 1, { burst: 1 });
  const first = await bus.publishWithConfirmation('hot', { n: 1 });
  assert.equal(first.admitted, true);
  const shed = await bus.publishWithConfirmation('hot', { n: 2 });
  assertRejectionShape(shed, 'hot', 'rate-limit', 7_000);
});

test('publisher-rate-limit shed returns a rejection receipt', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setPublisherRateLimit('svc', { messagesPerSec: 1, burst: 1 });
  const first = await bus.publishWithConfirmation('hot', { n: 1 }, { publisherId: 'svc' });
  assert.equal(first.admitted, true);
  const shed = await bus.publishWithConfirmation('hot', { n: 2 }, { publisherId: 'svc' });
  assertRejectionShape(shed, 'hot', 'publisher-rate-limit', 7_000);
});

test('acl denial returns a rejection receipt', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setAclRules([{ pattern: 'admin.**', publish: 'deny' }]);
  const r = await bus.publishWithConfirmation('admin.cmd', {});
  assertRejectionShape(r, 'admin.cmd', 'acl', 7_000);
});

test('retired alias returns a rejection receipt', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicAlias('old', 'new', { ttlMs: 100 });
  clock.advance(101); // the alias TTL lapses
  const r = await bus.publishWithConfirmation('old', 'late');
  assertRejectionShape(r, 'old', 'alias-retired', 7_101);
});

test('paused topic (shed) returns a rejection receipt', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.pauseTopic('t');
  const r = await bus.publishWithConfirmation('t', { a: 1 });
  assertRejectionShape(r, 't', 'topic-paused', 7_000);
});

test('paused topic (queue) reports topic-paused while the message is buffered', async () => {
  const clock = makeClock();
  // Queue mode buffers on the durable side, so it needs a log dir.
  const bus = new EventBus({ now: clock.now, durableLogDir: freshDir() });
  bus.pauseTopic('t', { mode: 'queue' });
  const r = await bus.publishWithConfirmation('t', { a: 1 });
  // Not admitted yet — held for resume — and not rejected either; the
  // receipt reports the pause state, and no second receipt fires at resume.
  assertRejectionShape(r, 't', 'topic-paused', 7_000);
});

test('rejection consumes no sequence number: no gap for subscribers', async () => {
  const bus = new EventBus();
  bus.setTopicSchema('t', (p) => typeof p === 'string');
  const rejected = await bus.publishWithConfirmation('t', 42);
  assert.equal(rejected.admitted, false);
  const a = await bus.publishWithConfirmation('t', 'ok-1');
  const b = await bus.publishWithConfirmation('t', 'ok-2');
  assert.equal(a.admitted && a.seq, 1);
  assert.equal(b.admitted && b.seq, 2);
  assert.equal(bus.getStats().topics[0].lastSeq, 2);
});

test('rejection burns no rate-limit budget and writes no durable record', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  bus.setTopicRateLimit('hot', 1, { burst: 1 });
  bus.setTopicSchema('hot', (p) => typeof p === 'string');
  // The schema gate runs before the rate-limit gate: this rejection must
  // not take the single token.
  const rejected = await bus.publishWithConfirmation('hot', 42);
  assert.equal(rejected.admitted, false);
  if (rejected.admitted === false) assert.equal(rejected.reason, 'schema');
  const admitted = await bus.publishWithConfirmation('hot', 'fine');
  assert.equal(admitted.admitted, true);
  // Nothing reached the durable log for the rejected publish.
  const logPath = join(dir, `${encodeURIComponent('hot')}.log`);
  const lines = readFileSync(logPath, 'utf8').trim().split('\n');
  assert.equal(lines.length, 1);
  assert.equal(JSON.parse(lines[0]).seq, 1);
});

test('rejection fires onAdmissionRejected exactly like publish', async () => {
  const clock = makeClock();
  const sink = makeSink();
  const bus = new EventBus({ now: clock.now, onAdmissionRejected: sink.hook });
  bus.setTopicSchema('t', () => false);
  const r = await bus.publishWithConfirmation('t', { a: 1 });
  assert.equal(r.admitted, false);
  assert.equal(sink.events.length, 1);
  assert.equal(sink.events[0].topic, 't');
  assert.equal(sink.events[0].reason, 'schema');
  assert.equal(sink.events[0].at, 7_000);
  assert.equal(bus.getStats().rejectedMessages, 1);
});

test('invalid arguments throw RangeError fail-fast with zero side effects', () => {
  const bus = new EventBus();
  assert.throws(() => bus.publishWithConfirmation('', { a: 1 }), RangeError);
  assert.throws(
    // @ts-expect-error: deliberate type violation
    () => bus.publishWithConfirmation(undefined, { a: 1 }),
    RangeError,
  );
  assert.throws(
    () => bus.publishWithConfirmation('t', { a: 1 }, { key: 123 as unknown as string }),
    RangeError,
  );
  assert.throws(
    () =>
      bus.publishWithConfirmation('t', { a: 1 }, { priority: 'bogus' as 'normal' }),
    RangeError,
  );
  assert.equal(bus.getStats().totalPublished, 0);
});

test('receipt topic is the namespace-resolved concrete topic', async () => {
  const bus = new EventBus();
  bus.createNamespace('t1');
  const r = await bus.publishWithConfirmation('orders', { id: 1 }, { namespace: 't1' });
  assert.equal(r.admitted, true);
  if (r.admitted === true) assert.equal(r.topic, 't1/orders');
});

test('binary payload hashes the raw bytes', async () => {
  const bus = new EventBus();
  const bytes = new Uint8Array([1, 2, 3, 250]);
  const r = await bus.publishWithConfirmation('bin', bytes, { binary: true });
  assert.equal(r.admitted, true);
  if (r.admitted === true) assert.equal(r.payloadSha256, sha256hex(bytes));
});

test('hash covers the redacted bytes — what the log persists', async () => {
  const bus = new EventBus();
  bus.setTopicRedaction('t', ['secret']);
  const r = await bus.publishWithConfirmation('t', { secret: 'hunter2', a: 1 });
  assert.equal(r.admitted, true);
  if (r.admitted === true) {
    // Redaction replaces the field with "***" before admission; the
    // receipt hashes the admitted (redacted) bytes, not the original.
    assert.equal(r.payloadSha256, jsonHash({ secret: '***', a: 1 }));
    assert.notEqual(r.payloadSha256, jsonHash({ secret: 'hunter2', a: 1 }));
  }
});

test('barrier control message confirms with seq 0 and no durable write', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  const r = await bus.publishWithConfirmation('t', { cut: true }, { barrier: { epoch: 3 } });
  assert.equal(r.admitted, true);
  if (r.admitted === true) {
    assert.equal(r.seq, 0);
    assert.equal(r.topic, 't');
    assert.equal(r.payloadSha256, jsonHash({ cut: true }));
  }
  assert.ok(!existsSync(join(dir, `${encodeURIComponent('t')}.log`)));
});
