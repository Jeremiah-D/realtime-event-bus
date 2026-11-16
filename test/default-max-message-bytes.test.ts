/**
 * Bus-level default per-message byte cap live revision (EB-524).
 *
 * `setDefaultMaxMessageBytes` decides the admission cap for publishes
 * to topics with no EB-64 rule — `1024` to bound every rule-less
 * topic's single-message size fleet-wide with one call, `null` to
 * clear the default back to no cap. The priority chain is exact-topic
 * rule > earliest-registered matching pattern rule > bus default >
 * no cap; an explicit rule always wins, even when it is larger than
 * the default. The default is inherited wherever admission runs
 * (`publish`, `publishBatch`, `publishAtomic` via the shared `fanOut`
 * path, `publishDelayed` at schedule time, and the shadow admission
 * behind `previewPublish`) because all three call sites resolve
 * through `maxMessageBytesForTopic`. It is the first volume gate:
 * on a double violation the message-size reason wins over the
 * payload reason (EB-520). A revision only affects publishes
 * admitted after it, and a revision identical to the current default
 * is a no-op with no audit. Audited as
 * `default_max_message_bytes_revised`
 * (`{ previous, maxMessageBytes }`).
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  type AdmissionRejectionEvent,
} from '../src/bus.ts';

const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const defaultAudits = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'default_max_message_bytes_revised' });

/** UTF-8 JSON byte size of a payload, as the admission gate measures it. */
const bytesOf = (payload: unknown): number => Buffer.byteLength(JSON.stringify(payload), 'utf8');

/** A payload whose JSON UTF-8 encoding is exactly `size` bytes. */
const sizedPayload = (size: number) => ({ data: 'x'.repeat(Math.max(0, size - 11)) });

/** Collects admission-rejection hook events in order. */
function makeSink() {
  const events: AdmissionRejectionEvent[] = [];
  const hook = (event: AdmissionRejectionEvent) => {
    events.push(event);
  };
  return { events, hook };
}

test('EB-524: factory default is unset — rule-less topics have no message-size cap', async () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultMaxMessageBytes(), undefined);
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  const big = sizedPayload(10_000);
  assert.equal(bus.publish('t', big), 1);
  await flush();
  assert.deepEqual(received, [big]);
  assert.equal(bus.getStats().rejectedMessages, 0);
});

test('EB-524: a bus default rejects an oversize publish on a rule-less topic with zero side effects', async () => {
  const { events, hook } = makeSink();
  const bus = new EventBus({ onAdmissionRejected: hook });
  const seqs: number[] = [];
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => {
    seqs.push(msg.seq);
    received.push(msg.payload);
  });
  bus.setDefaultMaxMessageBytes(1024);
  assert.equal(bus.getDefaultMaxMessageBytes(), 1024);
  const small = sizedPayload(512);
  const big = sizedPayload(1500);
  assert.equal(bus.publish('t', small), 1);
  assert.equal(bus.publish('t', big), 0);
  assert.equal(bus.publish('t', small), 1);
  await flush();
  assert.deepEqual(seqs, [1, 2], 'the rejection consumed no sequence number');
  assert.equal(received.length, 2);
  assert.equal(bus.getStats().rejectedMessages, 1);
  assert.equal(events.length, 1);
  assert.equal(events[0].reason, 'message-too-big');
  assert.equal(events[0].payloadBytes, bytesOf(big));
});

test('EB-524: an explicit topic rule always beats the bus default, in both directions', async () => {
  const bus = new EventBus();
  bus.setDefaultMaxMessageBytes(1024);
  const received: unknown[] = [];
  bus.subscribe('wide', (msg) => received.push(msg.payload));
  bus.subscribe('narrow', (msg) => received.push(msg.payload));
  // A 2048 rule admits what the 1024 default would reject.
  bus.setTopicMaxMessageBytes('wide', 2048);
  assert.equal(bus.publish('wide', sizedPayload(1500)), 1);
  // A 512 rule rejects what the 1024 default would admit.
  bus.setTopicMaxMessageBytes('narrow', 512);
  assert.equal(bus.publish('narrow', sizedPayload(600)), 0);
  await flush();
  assert.equal(received.length, 1);
  assert.equal(bus.getStats().rejectedMessages, 1);
});

test('EB-524: a pattern rule beats the bus default and earliest-registered pattern semantics are unchanged', async () => {
  const bus = new EventBus();
  bus.setDefaultMaxMessageBytes(1024);
  const received: unknown[] = [];
  bus.subscribe('orders.new', (msg) => received.push(msg.payload));
  // A pattern rule rejects what the 1024 default would admit.
  bus.setTopicMaxMessageBytes('orders.*', 512);
  assert.equal(bus.publish('orders.new', sizedPayload(600)), 0);
  // The earliest-registered matching pattern still wins over a later
  // pattern — and over the default: 1500 bytes pass the 2048 pattern.
  const patterned = new EventBus();
  patterned.setDefaultMaxMessageBytes(1024);
  const patternedReceived: unknown[] = [];
  patterned.subscribe('orders.new', (msg) => patternedReceived.push(msg.payload));
  patterned.setTopicMaxMessageBytes('orders.*', 2048);
  patterned.setTopicMaxMessageBytes('*.new', 512);
  assert.equal(patterned.publish('orders.new', sizedPayload(1500)), 1);
  await flush();
  assert.equal(received.length, 0);
  assert.equal(patternedReceived.length, 1);
  assert.equal(bus.getStats().rejectedMessages, 1);
});

test('EB-524: clearing to null restores no cap for rule-less topics', async () => {
  const bus = new EventBus();
  bus.setDefaultMaxMessageBytes(1024);
  assert.equal(bus.publish('t', sizedPayload(1500)), 0);
  bus.setDefaultMaxMessageBytes(null);
  assert.equal(bus.getDefaultMaxMessageBytes(), undefined);
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  const big = sizedPayload(10_000);
  assert.equal(bus.publish('t', big), 1);
  await flush();
  assert.deepEqual(received, [big]);
});

test('EB-524: invalid values throw RangeError and leave the live default untouched', () => {
  const bus = new EventBus();
  bus.setDefaultMaxMessageBytes(1024);
  for (const bad of [0, -1, 1.5, Number.NaN, '1024', undefined] as unknown[]) {
    assert.throws(
      () => bus.setDefaultMaxMessageBytes(bad as number),
      RangeError,
      `value ${String(bad)} must throw`,
    );
    assert.equal(bus.getDefaultMaxMessageBytes(), 1024, 'live default untouched');
  }
  assert.throws(
    () => bus.setDefaultMaxMessageBytes(512, ''),
    RangeError,
    'an illegal actor throws',
  );
  assert.equal(bus.getDefaultMaxMessageBytes(), 1024);
  assert.equal(defaultAudits(bus).length, 1, 'failed revisions must not audit');
});

test('EB-524: revising to the current default is a no-op with no audit, including clearing an already-unset default', () => {
  const bus = new EventBus();
  bus.setDefaultMaxMessageBytes(null);
  assert.equal(
    defaultAudits(bus).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setDefaultMaxMessageBytes(1024);
  bus.setDefaultMaxMessageBytes(1024);
  const audit = defaultAudits(bus);
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { previous: null, maxMessageBytes: 1024 });
  bus.setDefaultMaxMessageBytes(null);
  bus.setDefaultMaxMessageBytes(null);
  assert.equal(defaultAudits(bus).length, 2, 're-clearing audits nothing');
});

test('EB-524: the default is also visible in shadow admission and in publishDelayed', async () => {
  const bus = new EventBus();
  bus.setDefaultMaxMessageBytes(1024);
  const verdict = bus.previewPublish('t', sizedPayload(1500));
  assert.equal(verdict.admitted, false);
  assert.equal(verdict.reason, 'message-too-big');
  assert.equal(verdict.failedGate, 'max-message-bytes');
  assert.equal(bus.getStats().rejectedMessages, 0, 'shadow admission has no side effects');
  const ok = bus.previewPublish('t', sizedPayload(512));
  assert.equal(ok.admitted, true);

  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  assert.equal(bus.publishDelayed('t', sizedPayload(1500), { delayMs: 20 }), undefined);
  assert.equal(bus.getStats().pendingDelayed, 0);
  assert.equal(bus.getStats().rejectedMessages, 1);
  const id = bus.publishDelayed('t', sizedPayload(512), { delayMs: 20 });
  assert.ok(typeof id === 'string');
  await sleep(60);
  await flush();
  assert.equal(received.length, 1, 'only the admitted schedule fanned out');
});

test('EB-524: on a double violation the message-size reason wins over the payload reason', async () => {
  const bus = new EventBus();
  bus.setDefaultMaxMessageBytes(1024);
  bus.setDefaultMaxPayloadBytes(512);
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  // 1500 bytes trips both defaults; the first gate's reason wins.
  assert.equal(bus.publish('t', sizedPayload(1500)), 0);
  assert.equal(bus.getStats().rejectedMessages, 1);
  const verdict = bus.previewPublish('t', sizedPayload(1500));
  assert.equal(verdict.admitted, false);
  assert.equal(verdict.reason, 'message-too-big');
  assert.equal(verdict.failedGate, 'max-message-bytes');
  // 700 bytes trips only the payload default — the payload reason
  // still surfaces when the message-size gate passes.
  const payloadOnly = bus.previewPublish('t', sizedPayload(700));
  assert.equal(payloadOnly.admitted, false);
  assert.equal(payloadOnly.reason, 'payload-too-large');
  assert.equal(payloadOnly.failedGate, 'max-payload-bytes');
  await flush();
  assert.equal(received.length, 0);
});

test('EB-524: a revision does not move already-queued or already-admitted messages or registered rules', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  // An explicit subscriber cap beats the topic leg, so the delivery-side
  // skip cannot interfere with what this test measures: admission.
  bus.subscribe('t', (msg) => received.push(msg.payload), { subscriberMaxPayloadBytes: 2048 });
  // Admitted before the revision: delivered even though it exceeds
  // the default the revision installs.
  assert.equal(bus.publish('t', sizedPayload(1500)), 1);
  // Scheduled before the revision: keeps its schedule-time verdict —
  // it fans out at its due time (a sequence number is consumed, no
  // admission rejection is counted) instead of being re-admitted
  // against the new default.
  const delayedId = bus.publishDelayed('t', sizedPayload(1500), { delayMs: 30 });
  assert.ok(typeof delayedId === 'string');
  bus.setDefaultMaxMessageBytes(1024);
  // A registered rule is untouched by the revision: its 2048 still
  // admits what the 1024 default would reject.
  bus.setTopicMaxMessageBytes('wide', 2048);
  bus.subscribe('wide', (msg) => received.push(msg.payload));
  assert.equal(bus.publish('wide', sizedPayload(1500)), 1);
  // And the new default now governs fresh publishes to rule-less topics.
  assert.equal(bus.publish('t', sizedPayload(1500)), 0);
  await sleep(80);
  await flush();
  assert.equal(received.length, 3, 'the pre-revision publish and schedule both delivered');
  assert.equal(bus.getStats().rejectedMessages, 1, 'only the fresh post-revision publish was rejected');
});

test('EB-524: audits carry the exact previous and new caps and the actor, and the getter follows the live value', () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultMaxMessageBytes(), undefined);
  bus.setDefaultMaxMessageBytes(1024, 'ops');
  assert.equal(bus.getDefaultMaxMessageBytes(), 1024);
  bus.setDefaultMaxMessageBytes(2048, 'ops');
  assert.equal(bus.getDefaultMaxMessageBytes(), 2048);
  bus.setDefaultMaxMessageBytes(null, 'ops');
  assert.equal(bus.getDefaultMaxMessageBytes(), undefined);
  const audit = defaultAudits(bus);
  assert.equal(audit.length, 3);
  assert.deepEqual(audit[0].detail, { previous: null, maxMessageBytes: 1024 });
  assert.deepEqual(audit[1].detail, { previous: 1024, maxMessageBytes: 2048 });
  assert.deepEqual(audit[2].detail, { previous: 2048, maxMessageBytes: null });
  assert.ok(audit.every((event) => event.actor === 'ops'));
});
