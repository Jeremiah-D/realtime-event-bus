/**
 * Bus-level default topic payload cap live revision (EB-520).
 *
 * `setDefaultMaxPayloadBytes` decides the admission cap for publishes
 * to topics with no EB-120 rule — `1024` to bound every rule-less
 * topic's single-message payload fleet-wide with one call, `null` to
 * clear the default back to no cap. The priority chain is exact-topic
 * rule > earliest-registered matching pattern rule > bus default >
 * no cap; an explicit rule always wins, even when it is larger than
 * the default. The default is inherited wherever admission runs
 * (`publish`, `publishBatch`, `publishAtomic`, `publishDelayed` at
 * schedule time, and the shadow admission behind `previewPublish`)
 * because all four call sites resolve through
 * `maxPayloadBytesForTopic`. It also flows into the topic leg of
 * `subscriberPayloadCapFor`, below the EB-477 subscriber-side
 * default. A revision only affects publishes admitted after it, and a
 * revision identical to the current default is a no-op with no audit.
 * Audited as `default_max_payload_bytes_revised`
 * (`{ previous, maxPayloadBytes }`).
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  type AdmissionRejectionEvent,
  type BusMessage,
} from '../src/bus.ts';

const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const defaultAudits = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'default_max_payload_bytes_revised' });

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

test('EB-520: factory default is unset — rule-less topics have no payload cap', async () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultMaxPayloadBytes(), undefined);
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  const big = sizedPayload(10_000);
  assert.equal(bus.publish('t', big), 1);
  await flush();
  assert.deepEqual(received, [big]);
  assert.equal(bus.getStats().rejectedMessages, 0);
});

test('EB-520: a bus default rejects an oversize publish on a rule-less topic with zero side effects', async () => {
  const { events, hook } = makeSink();
  const bus = new EventBus({ onAdmissionRejected: hook });
  const seqs: number[] = [];
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => {
    seqs.push(msg.seq);
    received.push(msg.payload);
  });
  bus.setDefaultMaxPayloadBytes(1024);
  assert.equal(bus.getDefaultMaxPayloadBytes(), 1024);
  const small = sizedPayload(512);
  const big = sizedPayload(1500);
  assert.equal(bus.publish('t', small), 1);
  assert.equal(bus.publish('t', big), 0);
  assert.equal(bus.publish('t', small), 1);
  await flush();
  assert.deepEqual(seqs, [1, 2], 'the rejection consumed no sequence number');
  assert.equal(received.length, 2);
  assert.equal(bus.getStats().rejectedMessages, 1);
  assert.equal(bus.getStats().payloadRejectedBytes, bytesOf(big));
  assert.equal(events.length, 1);
  assert.equal(events[0].reason, 'payload-too-large');
  assert.equal(events[0].payloadBytes, bytesOf(big));
});

test('EB-520: an explicit topic rule always beats the bus default, in both directions', async () => {
  const bus = new EventBus();
  bus.setDefaultMaxPayloadBytes(1024);
  const received: unknown[] = [];
  bus.subscribe('wide', (msg) => received.push(msg.payload));
  bus.subscribe('narrow', (msg) => received.push(msg.payload));
  // A 2048 rule admits what the 1024 default would reject.
  bus.setTopicMaxPayloadBytes('wide', 2048);
  assert.equal(bus.publish('wide', sizedPayload(1500)), 1);
  // A 512 rule rejects what the 1024 default would admit.
  bus.setTopicMaxPayloadBytes('narrow', 512);
  assert.equal(bus.publish('narrow', sizedPayload(600)), 0);
  await flush();
  assert.equal(received.length, 1);
  assert.equal(bus.getStats().rejectedMessages, 1);
});

test('EB-520: a pattern rule beats the bus default and earliest-registered pattern semantics are unchanged', async () => {
  const bus = new EventBus();
  bus.setDefaultMaxPayloadBytes(1024);
  const received: unknown[] = [];
  bus.subscribe('orders.new', (msg) => received.push(msg.payload));
  // A pattern rule rejects what the 1024 default would admit.
  bus.setTopicMaxPayloadBytes('orders.*', 512);
  assert.equal(bus.publish('orders.new', sizedPayload(600)), 0);
  // The earliest-registered matching pattern still wins over a later
  // pattern — and over the default: 1500 bytes pass the 2048 pattern.
  const patterned = new EventBus();
  patterned.setDefaultMaxPayloadBytes(1024);
  const patternedReceived: unknown[] = [];
  patterned.subscribe('orders.new', (msg) => patternedReceived.push(msg.payload));
  patterned.setTopicMaxPayloadBytes('orders.*', 2048);
  patterned.setTopicMaxPayloadBytes('*.new', 512);
  assert.equal(patterned.publish('orders.new', sizedPayload(1500)), 1);
  await flush();
  assert.equal(received.length, 0);
  assert.equal(patternedReceived.length, 1);
  assert.equal(bus.getStats().rejectedMessages, 1);
});

test('EB-520: clearing to null restores no cap for rule-less topics', async () => {
  const bus = new EventBus();
  bus.setDefaultMaxPayloadBytes(1024);
  assert.equal(bus.publish('t', sizedPayload(1500)), 0);
  bus.setDefaultMaxPayloadBytes(null);
  assert.equal(bus.getDefaultMaxPayloadBytes(), undefined);
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  const big = sizedPayload(10_000);
  assert.equal(bus.publish('t', big), 1);
  await flush();
  assert.deepEqual(received, [big]);
});

test('EB-520: invalid values throw RangeError and leave the live default untouched', () => {
  const bus = new EventBus();
  bus.setDefaultMaxPayloadBytes(1024);
  for (const bad of [0, -1, 1.5, Number.NaN, '1024', undefined] as unknown[]) {
    assert.throws(
      () => bus.setDefaultMaxPayloadBytes(bad as number),
      RangeError,
      `value ${String(bad)} must throw`,
    );
    assert.equal(bus.getDefaultMaxPayloadBytes(), 1024, 'live default untouched');
  }
  assert.throws(
    () => bus.setDefaultMaxPayloadBytes(512, ''),
    RangeError,
    'an illegal actor throws',
  );
  assert.equal(bus.getDefaultMaxPayloadBytes(), 1024);
  assert.equal(defaultAudits(bus).length, 1, 'failed revisions must not audit');
});

test('EB-520: revising to the current default is a no-op with no audit, including clearing an already-unset default', () => {
  const bus = new EventBus();
  bus.setDefaultMaxPayloadBytes(null);
  assert.equal(
    defaultAudits(bus).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setDefaultMaxPayloadBytes(1024);
  bus.setDefaultMaxPayloadBytes(1024);
  const audit = defaultAudits(bus);
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { previous: null, maxPayloadBytes: 1024 });
  bus.setDefaultMaxPayloadBytes(null);
  bus.setDefaultMaxPayloadBytes(null);
  assert.equal(defaultAudits(bus).length, 2, 're-clearing audits nothing');
});

test('EB-520: the default is also visible in shadow admission and in publishDelayed', async () => {
  const bus = new EventBus();
  bus.setDefaultMaxPayloadBytes(1024);
  const verdict = bus.previewPublish('t', sizedPayload(1500));
  assert.equal(verdict.admitted, false);
  assert.equal(verdict.reason, 'payload-too-large');
  assert.equal(verdict.failedGate, 'max-payload-bytes');
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

test('EB-520: a revision does not move already-queued or already-admitted messages or registered rules', async () => {
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
  bus.setDefaultMaxPayloadBytes(1024);
  // A registered rule is untouched by the revision: its 2048 still
  // admits what the 1024 default would reject.
  bus.setTopicMaxPayloadBytes('wide', 2048);
  bus.subscribe('wide', (msg) => received.push(msg.payload));
  assert.equal(bus.publish('wide', sizedPayload(1500)), 1);
  // And the new default now governs fresh publishes to rule-less topics.
  assert.equal(bus.publish('t', sizedPayload(1500)), 0);
  await sleep(80);
  await flush();
  assert.equal(received.length, 3, 'the pre-revision publish and schedule both delivered');
  assert.equal(bus.getStats().rejectedMessages, 1, 'only the fresh post-revision publish was rejected');
});

test('EB-520: the subscriber chain is unchanged — the EB-477 default still takes priority over the topic leg', async () => {
  // The subscriber default (64) skips at delivery what the bus
  // payload default (1024) admitted at publish time.
  const bus = new EventBus();
  bus.setDefaultMaxPayloadBytes(1024);
  bus.setDefaultSubscriberMaxPayloadBytes(64);
  const received: BusMessage[] = [];
  bus.subscribe('t', (msg) => received.push(msg));
  // `publish` returns the accepted-subscriber count, which is 0 here
  // because the only subscriber skips the message — so admission is
  // asserted through `tryPublish`, which fans the message out itself.
  const verdict = bus.tryPublish('t', sizedPayload(128));
  assert.equal(verdict.admitted, true, 'admitted against the 1024 bus default');
  await flush();
  assert.equal(received.length, 0, 'skipped at delivery against the 64 subscriber default');
  assert.equal(bus.getStats().subscriberOversizeDropped, 1);
  assert.equal(bus.getStats().rejectedMessages, 0);

  // The other way round: the bus payload default (64) rejects at
  // admission before the subscriber default (2048) ever matters.
  const loud = new EventBus();
  loud.setDefaultMaxPayloadBytes(64);
  loud.setDefaultSubscriberMaxPayloadBytes(2048);
  const loudReceived: BusMessage[] = [];
  loud.subscribe('t', (msg) => loudReceived.push(msg));
  assert.equal(loud.publish('t', sizedPayload(128)), 0, 'rejected at admission');
  await flush();
  assert.equal(loudReceived.length, 0);
  assert.equal(loud.getStats().rejectedMessages, 1);
  assert.equal(loud.getStats().subscriberOversizeDropped, 0);
});

test('EB-520: audits carry the exact previous and new caps and the actor, and the getter follows the live value', () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultMaxPayloadBytes(), undefined);
  bus.setDefaultMaxPayloadBytes(1024, 'ops');
  assert.equal(bus.getDefaultMaxPayloadBytes(), 1024);
  bus.setDefaultMaxPayloadBytes(2048, 'ops');
  assert.equal(bus.getDefaultMaxPayloadBytes(), 2048);
  bus.setDefaultMaxPayloadBytes(null, 'ops');
  assert.equal(bus.getDefaultMaxPayloadBytes(), undefined);
  const audit = defaultAudits(bus);
  assert.equal(audit.length, 3);
  assert.deepEqual(audit[0].detail, { previous: null, maxPayloadBytes: 1024 });
  assert.deepEqual(audit[1].detail, { previous: 1024, maxPayloadBytes: 2048 });
  assert.deepEqual(audit[2].detail, { previous: 2048, maxPayloadBytes: null });
  assert.ok(audit.every((event) => event.actor === 'ops'));
});
