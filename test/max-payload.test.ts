import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  EventBus,
  type AdmissionRejectionEvent,
  type AdmissionRejectReason,
} from '../src/bus.ts';

const flush = () => new Promise((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** Collects admission-rejection hook events in order. */
function makeSink() {
  const events: AdmissionRejectionEvent[] = [];
  const hook = (event: AdmissionRejectionEvent) => {
    events.push(event);
  };
  return { events, hook };
}

/** UTF-8 JSON byte size of a payload, as the admission gate measures it. */
const bytesOf = (payload: unknown): number => Buffer.byteLength(JSON.stringify(payload), 'utf8');

test('no rule means no size check', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  const big = { data: 'x'.repeat(10_000) };
  assert.equal(bus.publish('t', big), 1);
  await flush();
  assert.deepEqual(received, [big]);
  assert.equal(bus.getStats().rejectedMessages, 0);
  assert.equal(bus.getStats().payloadRejectedBytes, 0);
});

test('oversized publish is rejected with zero side effects', async () => {
  const { events, hook } = makeSink();
  const bus = new EventBus({ onAdmissionRejected: hook });
  const seqs: number[] = [];
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => {
    seqs.push(msg.seq);
    received.push(msg.payload);
  });
  // Cap: 10 bytes. {"a":"x"} is 9 bytes, {"a":"xy"} is 10 bytes,
  // {"a":"xyz"} is 11 bytes.
  bus.setTopicMaxPayloadBytes('t', 10);
  const small = { a: 'x' };
  const boundary = { a: 'xy' };
  const big = { a: 'xyz' };
  assert.equal(bytesOf(small), 9);
  assert.equal(bytesOf(boundary), 10);
  assert.equal(bytesOf(big), 11);
  assert.equal(bus.publish('t', small), 1);
  assert.equal(bus.publish('t', boundary), 1);
  assert.equal(bus.publish('t', big), 0);
  assert.equal(bus.publish('t', small), 1);
  await flush();
  // The rejection consumed no sequence number: the next publish kept
  // marching (1, 2, 3 — no gap) and subscribers saw no false gap.
  assert.deepEqual(seqs, [1, 2, 3]);
  assert.deepEqual(received, [small, boundary, small]);
  assert.equal(bus.getStats().topics.find((t) => t.topic === 't')?.sequenceGaps, 0);
  assert.equal(bus.getStats().rejectedMessages, 1);
  assert.equal(bus.getStats().payloadRejectedBytes, 11);
  assert.equal(bus.getStats().topics.find((t) => t.topic === 't')?.payloadRejectedBytes, 11);
  assert.equal(events.length, 1);
  assert.equal(events[0].reason, 'payload-too-large');
  assert.equal(events[0].topic, 't');
  assert.equal(events[0].payloadBytes, 11);
  // The hook carries the size, never the payload body.
  assert.ok(!('payload' in events[0]));
});

test('multi-byte UTF-8 payloads are measured in bytes, not characters', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  // 'é' is 2 UTF-8 bytes: JSON '"ééé"' is 1 + 6 + 1 = 8 bytes, 5 chars.
  const payload = 'é'.repeat(3);
  assert.equal(payload.length, 3);
  assert.equal(bytesOf(payload), 8);
  bus.setTopicMaxPayloadBytes('t', 7);
  assert.equal(bus.publish('t', payload), 0);
  assert.equal(bus.getStats().payloadRejectedBytes, 8);
  bus.setTopicMaxPayloadBytes('t', 8);
  assert.equal(bus.publish('t', payload), 1);
  assert.equal(bus.getStats().payloadRejectedBytes, 8);
});

test('binary payloads measure as their wire bytes', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicMaxPayloadBytes('t', 10);
  assert.equal(bus.publish('t', new Uint8Array(11), { binary: true }), 0);
  assert.equal(bus.getStats().rejectedMessages, 1);
  assert.equal(bus.getStats().payloadRejectedBytes, 11);
  assert.equal(bus.publish('t', new Uint8Array(10), { binary: true }), 1);
  await flush();
  assert.equal(received.length, 1);
});

test('unserializable payloads measure 0 bytes and always pass', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.setTopicMaxPayloadBytes('t', 10);
  const circular: { self?: unknown } = {};
  circular.self = circular;
  // Passthrough semantics (EB-22 convention): no JSON encoding means no
  // measurable size — the gate admits it rather than guessing.
  assert.equal(bus.publish('t', circular), 1);
  assert.equal(bus.getStats().rejectedMessages, 0);
  assert.equal(bus.getStats().payloadRejectedBytes, 0);
});

test('invalid configuration throws RangeError', () => {
  const bus = new EventBus();
  for (const bad of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
    assert.throws(() => bus.setTopicMaxPayloadBytes('t', bad), RangeError);
  }
  assert.throws(() => bus.setTopicMaxPayloadBytes('', 10), RangeError);
  assert.equal(bus.getStats().rejectedMessages, 0);
});

test('clearTopicMaxPayloadBytes removes the rule', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.setTopicMaxPayloadBytes('t', 10);
  assert.equal(bus.clearTopicMaxPayloadBytes('t'), true);
  assert.equal(bus.clearTopicMaxPayloadBytes('t'), false);
  assert.equal(bus.publish('t', { a: 'xyz' }), 1);
  assert.equal(bus.getStats().rejectedMessages, 0);
});

test('exact-topic rule wins over patterns; earliest pattern wins', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.subscribe('t.x', () => {});
  bus.setTopicMaxPayloadBytes('t.*', 100);
  bus.setTopicMaxPayloadBytes('t', 10);
  const eleven = { a: 'xyz' }; // 11 bytes
  const fifty = { data: 'x'.repeat(41) }; // ~52 bytes
  assert.ok(bytesOf(eleven) === 11);
  assert.ok(bytesOf(fifty) > 10 && bytesOf(fifty) < 100);
  // Exact rule on 't' wins over the 't.*' pattern: 11 > 10 rejects.
  assert.equal(bus.publish('t', eleven), 0);
  // 't.x' only matches the pattern: 52 < 100 admits.
  assert.equal(bus.publish('t.x', fifty), 1);
  bus.clearTopicMaxPayloadBytes('t');
  assert.equal(bus.publish('t', eleven), 1);
  assert.equal(bus.getStats().rejectedMessages, 1);
});

test('message-size cap is evaluated first when both caps configured', () => {
  const { events, hook } = makeSink();
  const bus = new EventBus({ onAdmissionRejected: hook });
  bus.setTopicMaxMessageBytes('t', 5);
  bus.setTopicMaxPayloadBytes('t', 10);
  assert.equal(bus.publish('t', { a: 'xyz' }), 0); // 11 bytes
  assert.equal(events[0].reason, 'message-too-big');
  bus.clearTopicMaxMessageBytes('t');
  assert.equal(bus.publish('t', { a: 'xyz' }), 0);
  assert.equal(events[1].reason, 'payload-too-large');
});

test('rejection does not burn rate-limit budget', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.setTopicRateLimit('t', 1, { burst: 1 });
  bus.setTopicMaxPayloadBytes('t', 10);
  const big = { a: 'xyz' };
  const small = { a: 'x' };
  assert.equal(bus.publish('t', big), 0);
  // The single token is still intact: this publish is admitted, proving
  // the rejection burned no budget.
  assert.equal(bus.publish('t', small), 1);
  // Now the bucket is genuinely empty: the next small publish sheds.
  assert.equal(bus.publish('t', small), 0);
  assert.equal(bus.getStats().rateLimitedMessages, 1);
  assert.equal(bus.getStats().rejectedMessages, 1);
  assert.equal(bus.getStats().payloadRejectedBytes, bytesOf(big));
});

test('rejection writes nothing to the durable log', () => {
  const dir = mkdtempSync(join(tmpdir(), 'eb120-durable-'));
  const bus = new EventBus({ durableLogDir: dir });
  bus.setTopicMaxPayloadBytes('t', 10);
  assert.equal(bus.publish('t', { a: 'xyz' }), 0);
  const durable = bus.getStats().durableLog;
  assert.ok(durable !== undefined);
  assert.equal(durable.entries, 0);
  assert.equal(durable.topics, 0);
  assert.equal(bus.getStats().rejectedMessages, 1);
});

test('publishDelayed fails fast at schedule time', async () => {
  const bus = new EventBus();
  bus.setTopicMaxPayloadBytes('t', 10);
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  const big = { a: 'xyz' };
  const small = { a: 'x' };
  assert.equal(bus.publishDelayed('t', big, { delayMs: 20 }), undefined);
  assert.equal(bus.getStats().pendingDelayed, 0);
  assert.equal(bus.getStats().rejectedMessages, 1);
  assert.equal(bus.getStats().payloadRejectedBytes, bytesOf(big));
  const id = bus.publishDelayed('t', small, { delayMs: 20 });
  assert.ok(typeof id === 'string');
  await sleep(60);
  await flush();
  // Only the admitted schedule fanned out — the rejected payload never
  // became a delivery.
  assert.deepEqual(received, [small]);
  assert.equal(bus.getStats().pendingDelayed, 0);
});

test('publishAtomic rejects the whole batch when one entry is oversized', async () => {
  const { events, hook } = makeSink();
  const bus = new EventBus({ onAdmissionRejected: hook });
  bus.setTopicMaxPayloadBytes('t', 10);
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  const small = { a: 'x' };
  const big = { a: 'xyz' };
  const result = bus.publishAtomic([
    { topic: 't', payload: small },
    { topic: 't', payload: big },
    { topic: 't', payload: small },
  ]);
  assert.equal(result.published, 0);
  assert.ok(result.rejected !== undefined);
  assert.equal(result.rejected.index, 1);
  assert.equal(result.rejected.topic, 't');
  assert.equal(result.rejected.reason, 'payload-too-large');
  await flush();
  assert.deepEqual(received, []);
  // Zero side effects: no sequence number, no durable record, no
  // rate-limit budget — counted once against the failing entry's topic.
  assert.equal(bus.getStats().topics.find((t) => t.topic === 't')?.lastSeq ?? 0, 0);
  assert.equal(bus.getStats().rejectedMessages, 1);
  assert.equal(bus.getStats().payloadRejectedBytes, bytesOf(big));
  assert.equal(events.length, 1);
  assert.equal(events[0].reason, 'payload-too-large');
  assert.equal(events[0].payloadBytes, bytesOf(big));
});

test('publishBatch routes each entry through the same gate', async () => {
  const bus = new EventBus();
  bus.setTopicMaxPayloadBytes('t', 10);
  const seqs: number[] = [];
  bus.subscribe('t', (msg) => seqs.push(msg.seq));
  const small = { a: 'x' };
  const big = { a: 'xyz' };
  // Non-atomic: admitted entries still publish; the oversized entry is
  // rejected in the shared fanOut admission path (a batch envelope never
  // bypasses the gate).
  assert.equal(bus.publishBatch([
    { topic: 't', payload: small },
    { topic: 't', payload: big },
    { topic: 't', payload: small },
  ]), 2);
  await flush();
  assert.deepEqual(seqs, [1, 2]);
  assert.equal(bus.getStats().rejectedMessages, 1);
  assert.equal(bus.getStats().payloadRejectedBytes, bytesOf(big));
});

test('previewPublish reports the gate without side effects', () => {
  const bus = new EventBus();
  bus.setTopicMaxPayloadBytes('t', 10);
  const verdict = bus.previewPublish('t', { a: 'xyz' });
  assert.equal(verdict.admitted, false);
  assert.equal(verdict.reason, 'payload-too-large');
  assert.equal(verdict.failedGate, 'max-payload-bytes');
  assert.equal(bus.getStats().rejectedMessages, 0);
  assert.equal(bus.getStats().payloadRejectedBytes, 0);
  const ok = bus.previewPublish('t', { a: 'x' });
  assert.equal(ok.admitted, true);
});

test('hook events reconcile exactly against stats counters', () => {
  const { events, hook } = makeSink();
  const bus = new EventBus({ onAdmissionRejected: hook });
  bus.setTopicMaxPayloadBytes('a', 10);
  bus.setTopicMaxPayloadBytes('b', 5);
  const payloads = [
    ['a', { v: 'x'.repeat(20) }],
    ['a', { v: 'x'.repeat(30) }],
    ['b', { v: 'x'.repeat(10) }],
  ] as Array<[string, unknown]>;
  for (const [topic, payload] of payloads) {
    assert.equal(bus.publish(topic, payload), 0);
  }
  const tooLarge = events.filter((e) => e.reason === 'payload-too-large');
  // One hook event per rejection (EB-39 one-to-one invariant).
  assert.equal(tooLarge.length, bus.getStats().rejectedMessages);
  const sumBytes = tooLarge.reduce((sum, e) => sum + e.payloadBytes, 0);
  // Hook payloadBytes reconcile exactly against the byte counter.
  assert.equal(sumBytes, bus.getStats().payloadRejectedBytes);
  const expected = payloads.reduce((sum, [, p]) => sum + bytesOf(p), 0);
  assert.equal(bus.getStats().payloadRejectedBytes, expected);
  const byTopic = new Map<string, number>();
  for (const e of tooLarge) {
    byTopic.set(e.topic, (byTopic.get(e.topic) ?? 0) + e.payloadBytes);
  }
  for (const row of bus.getStats().topics) {
    assert.equal(row.payloadRejectedBytes, byTopic.get(row.topic) ?? 0);
  }
  // A throwing hook never disturbs the publish path.
  const noisy = new EventBus({
    onAdmissionRejected: () => {
      throw new Error('observer blew up');
    },
  });
  noisy.setTopicMaxPayloadBytes('t', 10);
  assert.equal(noisy.publish('t', { a: 'xyz' }), 0);
  assert.equal(noisy.getStats().rejectedMessages, 1);
});

test('rule replacement takes effect immediately', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  const big = { a: 'xyz' }; // 11 bytes
  bus.setTopicMaxPayloadBytes('t', 20);
  assert.equal(bus.publish('t', big), 1);
  bus.setTopicMaxPayloadBytes('t', 10);
  assert.equal(bus.publish('t', big), 0);
  assert.equal(bus.getStats().rejectedMessages, 1);
});

test('per-topic stats row carries the byte counter', () => {
  const bus = new EventBus();
  bus.setTopicMaxPayloadBytes('t', 10);
  bus.publish('t', { a: 'xyz' });
  const row = bus.getStats().topics.find((t) => t.topic === 't');
  assert.ok(row !== undefined);
  assert.equal(row.rejectedMessages, 1);
  assert.equal(row.payloadRejectedBytes, 11);
  assert.equal(typeof row.payloadRejectedBytes, 'number');
});
