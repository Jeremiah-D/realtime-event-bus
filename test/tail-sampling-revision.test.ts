/**
 * Bus-level tail-sampling revision (EB-296).
 *
 * `setTailSampling` retunes the sampler live — the sampling rate, the
 * open-trace budget, the per-trace span cap, and the ring capacity —
 * without restarting the bus. Only a bus that enabled `tailSampling`
 * can be revised; illegal values fail closed with the live tuning and
 * the audit log untouched; an identical (or empty) revision is a
 * no-op with no audit.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

const flush = () => new Promise((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

test('setTailSampling fails closed when tailSampling is not enabled', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setTailSampling({ successSampleRate: 0.5 }), RangeError);
  assert.throws(() => bus.setTailSampling({ maxOpenTraces: 10 }), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'tail_sampling_revised' }).length, 0);
});

test('setTailSampling rejects illegal values and leaves the live tuning untouched', async () => {
  const bus = new EventBus({ tailSampling: { successSampleRate: 0 } });
  bus.subscribe('t', () => {});
  const bad: Array<unknown> = [
    { successSampleRate: -0.1 },
    { successSampleRate: 1.1 },
    { successSampleRate: Number.NaN },
    { successSampleRate: Infinity },
    { maxOpenTraces: 0 },
    { maxOpenTraces: 1.5 },
    { maxSpansPerTrace: 0 },
    { maxSpansPerTrace: -1 },
    { bufferSize: 0 },
    { bufferSize: 2.5 },
    'nope',
    42,
  ];
  for (const revision of bad) {
    assert.throws(
      () => bus.setTailSampling(revision as never),
      RangeError,
      `expected RangeError for ${JSON.stringify(revision)}`,
    );
  }
  // The failed calls changed nothing: the rate is still 0, so a publish
  // still keeps no successful trace.
  bus.publish('t', 1);
  await flush();
  assert.deepEqual(bus.getStats().tailSampledSpans, []);
  assert.equal(bus.getSubscriptionAudit({ action: 'tail_sampling_revised' }).length, 0);
});

test('setTailSampling with identical values or an empty revision is a no-op with no audit', async () => {
  const bus = new EventBus({ tailSampling: { successSampleRate: 0.5, maxOpenTraces: 100 } });
  bus.setTailSampling({ successSampleRate: 0.5 });
  bus.setTailSampling({ maxOpenTraces: 100 });
  bus.setTailSampling({});
  bus.setTailSampling(null as never);
  assert.equal(bus.getSubscriptionAudit({ action: 'tail_sampling_revised' }).length, 0);
  // The untouched tuning still governs: rate 0.5 is probabilistic, so
  // assert the audit silence only — behaviour is covered by the tests
  // below.
  bus.subscribe('t', () => {});
  bus.publish('t', 1);
  await flush();
});

test('rate revision 0 -> 1: new traces are kept, audit carries old and new', async () => {
  const bus = new EventBus({ tailSampling: { successSampleRate: 0 } });
  bus.subscribe('t', () => {});
  bus.publish('t', 1);
  await flush();
  assert.deepEqual(bus.getStats().tailSampledSpans, []);
  bus.setTailSampling({ successSampleRate: 1 });
  bus.publish('t', 2);
  await flush();
  const spans = bus.getStats().tailSampledSpans;
  assert.ok(spans.length > 0, 'expected the post-revision trace to be kept');
  const publishSpan = spans.find((s) => s.name === 'bus.publish')!;
  assert.equal(publishSpan.attrs.tailKeepReason, 'sampled');
  const audit = bus.getSubscriptionAudit({ action: 'tail_sampling_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    successSampleRate: 1,
    oldSuccessSampleRate: 0,
    maxOpenTraces: 4096,
    oldMaxOpenTraces: 4096,
    maxSpansPerTrace: 512,
    oldMaxSpansPerTrace: 512,
    bufferSize: 1024,
    oldBufferSize: 1024,
  });
});

test('partial revision moves only the provided field', () => {
  const bus = new EventBus({ tailSampling: { successSampleRate: 0.25, maxOpenTraces: 50 } });
  bus.setTailSampling({ maxOpenTraces: 10 });
  const audit = bus.getSubscriptionAudit({ action: 'tail_sampling_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    successSampleRate: 0.25,
    oldSuccessSampleRate: 0.25,
    maxOpenTraces: 10,
    oldMaxOpenTraces: 50,
    maxSpansPerTrace: 512,
    oldMaxSpansPerTrace: 512,
    bufferSize: 1024,
    oldBufferSize: 1024,
  });
});

test('bufferSize shrink trims the ring to the newest spans at once', async () => {
  const bus = new EventBus({ tailSampling: true });
  const seqs: number[] = [];
  bus.subscribe('t', (msg) => seqs.push(msg.seq));
  bus.publish('t', 1);
  bus.publish('t', 2);
  bus.publish('t', 3);
  await flush();
  assert.equal(bus.getStats().tailSampledSpans.length, 15);
  bus.setTailSampling({ bufferSize: 2 });
  const trimmed = bus.getStats().tailSampledSpans;
  assert.equal(trimmed.length, 2);
  // The survivors are the newest spans: the last publish's deliver and
  // publish-root... in completion order the ring holds the tail of the
  // third trace.
  assert.ok(trimmed.every((s) => (s.attrs.seq as number) === seqs[2]));
  // The next append evicts the oldest under the new bound.
  bus.publish('t', 4);
  await flush();
  const after = bus.getStats().tailSampledSpans;
  assert.equal(after.length, 2);
  assert.ok(after.every((s) => (s.attrs.seq as number) === seqs[3]));
});

test('maxSpansPerTrace revision truncates the next trace at the live cap', async () => {
  const bus = new EventBus({ tailSampling: true });
  const seqs: number[] = [];
  bus.subscribe('t', (msg) => seqs.push(msg.seq));
  bus.publish('t', 1);
  await flush();
  assert.equal(bus.getStats().tailSampledSpans.length, 5);
  bus.setTailSampling({ maxSpansPerTrace: 1 });
  bus.publish('t', 2);
  await flush();
  // The second trace contributes exactly one span — the first buffered
  // (admission); the rest were dropped past the live cap.
  assert.equal(bus.getStats().tailSampledSpans.length, 6);
  const second = bus.getStats().tailSampledSpans.filter((s) => (s.attrs.seq as number) === seqs[1]);
  assert.equal(second.length, 1);
  assert.equal(second[0].name, 'bus.admission');
});

test('clean ack decides the tail trace without head tracing (EB-181 fix, EB-296)', async () => {
  // Regression: the uninstrumented reliable-delivery branch (no head
  // tracing, no ack-latency, no SLO) previously never closed the tail
  // ack span, so a clean ack left the trace open forever — decided
  // only when the open-trace budget force-decided it.
  const bus = new EventBus({ tailSampling: { successSampleRate: 1 } });
  const deliveries: Array<{ ack: () => void }> = [];
  bus.subscribeReliable(
    't',
    (delivery) => {
      deliveries.push(delivery);
    },
    { ackTimeoutMs: 60_000 },
  );
  bus.publish('t', 1);
  await flush();
  assert.deepEqual(bus.getStats().tailSampledSpans, []);
  deliveries[0].ack();
  await flush();
  const spans = bus.getStats().tailSampledSpans;
  assert.ok(spans.length > 0, 'expected the acked trace to be decided');
  assert.deepEqual(
    spans.map((s) => s.name),
    ['bus.admission', 'bus.enqueue', 'bus.fanout', 'bus.publish', 'bus.ack'],
  );
  assert.equal(spans.find((s) => s.name === 'bus.publish')!.attrs.tailKeepReason, 'sampled');
});

test('maxOpenTraces tightening force-decides the oldest open traces on the next open', async () => {
  const bus = new EventBus({ tailSampling: { successSampleRate: 1 } });
  const deliveries: Array<{ ack: () => void }> = [];
  const seqs: number[] = [];
  bus.subscribeReliable(
    't',
    (delivery) => {
      deliveries.push(delivery);
    },
    { ackTimeoutMs: 60_000 },
  );
  // A plain subscriber records the publish-order sequence numbers (the
  // reliable leg alone keeps each trace open until acked).
  bus.subscribe('t', (msg) => seqs.push(msg.seq));
  bus.publish('t', 1);
  await flush();
  bus.publish('t', 2);
  await flush();
  // Both traces are still open (neither acked): nothing decided yet.
  assert.deepEqual(bus.getStats().tailSampledSpans, []);
  bus.setTailSampling({ maxOpenTraces: 1 });
  // Tightening alone decides nothing — the next open enforces it.
  assert.deepEqual(bus.getStats().tailSampledSpans, []);
  bus.publish('t', 3);
  await flush();
  // The open-trace budget is enforced before the new trace opens, so
  // both older open traces (seqs[0], seqs[1]) were force-decided to
  // make room for the third under the new bound of 1.
  const publishSpans = bus.getStats().tailSampledSpans.filter((s) => s.name === 'bus.publish');
  assert.equal(publishSpans.length, 2);
  assert.deepEqual(
    publishSpans.map((s) => s.attrs.seq),
    [seqs[0], seqs[1]],
  );
  // The third trace is still open; acking it decides it normally (the
  // first two acks are no-ops — those traces were already decided).
  assert.equal(deliveries.length, 3);
  deliveries[2].ack();
  await sleep(50);
  await flush();
  const decided = bus.getStats().tailSampledSpans.filter((s) => s.name === 'bus.publish');
  assert.deepEqual(
    decided.map((s) => s.attrs.seq),
    [seqs[0], seqs[1], seqs[2]],
  );
});

test('onTraceSpan callback is untouched by a revision', async () => {
  const seen: string[] = [];
  const bus = new EventBus({
    tailSampling: { successSampleRate: 1, onTraceSpan: (span) => seen.push(span.name) },
  });
  bus.subscribe('t', () => {});
  bus.setTailSampling({ maxOpenTraces: 10 });
  bus.publish('t', 1);
  await flush();
  // The callback still receives the full kept trace after the revision.
  assert.deepEqual(seen, ['bus.admission', 'bus.enqueue', 'bus.fanout', 'bus.publish', 'bus.deliver']);
  assert.equal(bus.getSubscriptionAudit({ action: 'tail_sampling_revised' }).length, 1);
});
