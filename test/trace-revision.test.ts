/**
 * Bus-level head-based trace revision (EB-311).
 *
 * `setTrace` retunes the sampling rate and the ring capacity live —
 * without restarting the bus. Only a bus that enabled `trace` can be
 * revised; illegal values fail closed with the live tuning and the
 * audit log untouched; an identical (or empty) revision is a no-op
 * with no audit. Shrinking `bufferSize` keeps the buffered spans
 * until the next span covers the excess down to the live bound (unlike
 * EB-296, which trims at once); growing keeps every buffered span in
 * chronological order.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

const flush = () => new Promise((resolve) => setImmediate(resolve));

test('setTrace fails closed when trace is not enabled', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setTrace({ sampleRate: 0.5 }), RangeError);
  assert.throws(() => bus.setTrace({ bufferSize: 10 }), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_revised' }).length, 0);
});

test('setTrace rejects illegal values and leaves the live tuning untouched', async () => {
  const bus = new EventBus({ trace: { sampleRate: 0, bufferSize: 6 } });
  const bad: Array<unknown> = [
    { sampleRate: -0.1 },
    { sampleRate: 1.1 },
    { sampleRate: Number.NaN },
    { sampleRate: Infinity },
    { sampleRate: '0.5' },
    { bufferSize: 0 },
    { bufferSize: -1 },
    { bufferSize: 2.5 },
    { bufferSize: '4' },
    // One good field does not save a bad sibling: the whole call fails.
    { sampleRate: 0.5, bufferSize: 0 },
    'nope',
    42,
    [],
  ];
  for (const revision of bad) {
    assert.throws(
      () => bus.setTrace(revision as never),
      RangeError,
      `expected RangeError for ${JSON.stringify(revision)}`,
    );
  }
  // The failed calls changed nothing: the rate is still 0, so a publish
  // still starts no trace, and the audit log is untouched.
  bus.publish('t', 1);
  await flush();
  assert.deepEqual(bus.getStats().traceSpans, []);
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_revised' }).length, 0);
  // A valid revision afterwards still works, and the surviving buffer
  // bound (6) still governs the ring: three publishes attempt 9 spans.
  bus.setTrace({ sampleRate: 1 });
  bus.publish('t', 2);
  bus.publish('t', 3);
  bus.publish('t', 4);
  await flush();
  assert.equal(bus.getStats().traceSpans.length, 6);
});

test('setTrace with identical values or an empty revision is a no-op with no audit', () => {
  const bus = new EventBus({ trace: { sampleRate: 0.5, bufferSize: 100 } });
  bus.setTrace({ sampleRate: 0.5 });
  bus.setTrace({ bufferSize: 100 });
  bus.setTrace({ sampleRate: 0.5, bufferSize: 100 });
  bus.setTrace({});
  bus.setTrace(null as never);
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_revised' }).length, 0);
});

test('partial revision moves only the provided field', () => {
  const bus = new EventBus({ trace: { sampleRate: 0.25, bufferSize: 100 } });
  bus.setTrace({ bufferSize: 10 });
  let audit = bus.getSubscriptionAudit({ action: 'trace_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    sampleRate: 0.25,
    oldSampleRate: 0.25,
    bufferSize: 10,
    oldBufferSize: 100,
  });
  bus.setTrace({ sampleRate: 0.5 });
  audit = bus.getSubscriptionAudit({ action: 'trace_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, {
    sampleRate: 0.5,
    oldSampleRate: 0.25,
    bufferSize: 10,
    oldBufferSize: 10,
  });
});

test('rate revision 0 -> 1: new publishes are traced, audit carries old and new', async () => {
  const bus = new EventBus({ trace: { sampleRate: 0 } });
  bus.publish('t', 1);
  await flush();
  assert.deepEqual(bus.getStats().traceSpans, []);
  bus.setTrace({ sampleRate: 1 });
  bus.publish('t', 2);
  await flush();
  const spans = bus.getStats().traceSpans;
  assert.equal(spans.length, 3);
  assert.ok(spans.every((s) => (s.attrs.seq as number) === 2));
  const audit = bus.getSubscriptionAudit({ action: 'trace_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    sampleRate: 1,
    oldSampleRate: 0,
    bufferSize: 1024,
    oldBufferSize: 1024,
  });
});

test('rate revision 1 -> 0: new publishes stop tracing at the live rate', async () => {
  const bus = new EventBus({ trace: true });
  bus.publish('t', 1);
  await flush();
  assert.equal(bus.getStats().traceSpans.length, 3);
  bus.setTrace({ sampleRate: 0 });
  bus.publish('t', 2);
  await flush();
  // Still exactly the first publish's spans: the second publish was
  // sampled at the live rate 0, not the construction-time rate 1.
  const spans = bus.getStats().traceSpans;
  assert.equal(spans.length, 3);
  assert.ok(spans.every((s) => (s.attrs.seq as number) === 1));
});

test('bufferSize shrink retains the buffered spans until the next span covers them', async () => {
  const bus = new EventBus({ trace: { sampleRate: 1, bufferSize: 6 } });
  bus.publish('t', 1);
  bus.publish('t', 2);
  await flush();
  assert.equal(bus.getStats().traceSpans.length, 6);
  bus.setTrace({ bufferSize: 2 });
  // Unlike EB-296's at-once trim, the shrink itself evicts nothing:
  // all six spans are still buffered.
  const retained = bus.getStats().traceSpans;
  assert.equal(retained.length, 6);
  assert.deepEqual(
    retained.map((s) => s.attrs.seq as number),
    [1, 1, 1, 2, 2, 2],
  );
  // The next publish's spans cover the excess down to the live bound:
  // the ring lands on the two newest spans, oldest evicted first.
  bus.publish('t', 3);
  await flush();
  const after = bus.getStats().traceSpans;
  assert.equal(after.length, 2);
  assert.deepEqual(
    after.map((s) => [s.name, s.attrs.seq as number]),
    [
      ['bus.fanout', 3],
      ['bus.publish', 3],
    ],
  );
});

test('shrink wrap boundary: later publishes wrap at the new capacity, in order', async () => {
  const bus = new EventBus({ trace: { sampleRate: 1, bufferSize: 6 } });
  bus.publish('t', 1);
  bus.publish('t', 2);
  await flush();
  bus.setTrace({ bufferSize: 2 });
  bus.publish('t', 3);
  await flush();
  assert.deepEqual(
    bus.getStats().traceSpans.map((s) => [s.name, s.attrs.seq as number]),
    [
      ['bus.fanout', 3],
      ['bus.publish', 3],
    ],
  );
  // The head has wrapped at the new capacity-2 boundary: one more
  // publish overwrites the oldest live span first, and the snapshot
  // stays the newest two spans in chronological order.
  bus.publish('t', 4);
  await flush();
  assert.deepEqual(
    bus.getStats().traceSpans.map((s) => [s.name, s.attrs.seq as number]),
    [
      ['bus.fanout', 4],
      ['bus.publish', 4],
    ],
  );
  bus.publish('t', 5);
  await flush();
  assert.deepEqual(
    bus.getStats().traceSpans.map((s) => [s.name, s.attrs.seq as number]),
    [
      ['bus.fanout', 5],
      ['bus.publish', 5],
    ],
  );
});

test('bufferSize grow after a wrap keeps the buffered spans in chronological order', async () => {
  const bus = new EventBus({ trace: { sampleRate: 1, bufferSize: 2 } });
  bus.publish('t', 1);
  await flush();
  // The ring wrapped under capacity 2: only the newest two spans
  // survive, oldest first.
  assert.deepEqual(
    bus.getStats().traceSpans.map((s) => [s.name, s.attrs.seq as number]),
    [
      ['bus.fanout', 1],
      ['bus.publish', 1],
    ],
  );
  bus.setTrace({ bufferSize: 6 });
  bus.publish('t', 2);
  await flush();
  // Without the retune re-lay the grown ring would read the pre-grow
  // newest span before the older one; the snapshot must stay
  // chronological across the grow boundary.
  assert.deepEqual(
    bus.getStats().traceSpans.map((s) => [s.name, s.attrs.seq as number]),
    [
      ['bus.fanout', 1],
      ['bus.publish', 1],
      ['bus.admission', 2],
      ['bus.fanout', 2],
      ['bus.publish', 2],
    ],
  );
});

test('sampler and onTraceSpan survive a revision untouched', async () => {
  const seen: Array<{ name: string; seq: number }> = [];
  const bus = new EventBus({
    trace: {
      sampleRate: 1,
      sampler: () => true,
      onTraceSpan: (span) => seen.push({ name: span.name, seq: span.attrs.seq as number }),
      bufferSize: 1024,
    },
  });
  bus.publish('t', 1);
  await flush();
  assert.equal(seen.length, 3);
  // Revising the rate to 0 must not stop tracing: the custom sampler
  // still decides, and the callback still receives every span.
  bus.setTrace({ sampleRate: 0, bufferSize: 10 });
  bus.publish('t', 2);
  await flush();
  assert.equal(seen.length, 6);
  assert.deepEqual(
    seen.map((s) => s.seq),
    [1, 1, 1, 2, 2, 2],
  );
  assert.equal(bus.getStats().traceSpans.length, 6);
  const audit = bus.getSubscriptionAudit({ action: 'trace_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    sampleRate: 0,
    oldSampleRate: 1,
    bufferSize: 10,
    oldBufferSize: 1024,
  });
});
