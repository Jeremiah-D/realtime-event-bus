/**
 * Tail-sampling live enable (EB-545):
 * `setTailSampling` on a bus that never enabled `tailSampling`
 * installs the sampler live — no restart, no lost subscriptions —
 * with the revision resolved through the construction-time rule
 * (`resolveTailSamplingOptions`: `true` takes every default, a
 * partial object falls back per field). Enabling installs no callback
 * of its own — when `trace` is enabled the fresh sampler falls back
 * to the shared head channel exactly as construction does, and when
 * it is not the tail keeps spans in the ring only until
 * `setTailSpanCallback` (EB-418) installs a callback of its own — and
 * it is live-only: a restart restores the unconfigured state. The
 * enable is audited like any revision, with every old value `null`.
 * Spans published before the enable were never traced, so nothing is
 * decided retroactively.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import type { TraceSpan } from '../src/trace.ts';

const flush = () => new Promise((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

const DEFAULTS_DETAIL = {
  successSampleRate: 1,
  oldSuccessSampleRate: null,
  maxOpenTraces: 4096,
  oldMaxOpenTraces: null,
  maxSpansPerTrace: 512,
  oldMaxSpansPerTrace: null,
  bufferSize: 1024,
  oldBufferSize: null,
};

test('the first enable with {} takes the four construction defaults', () => {
  const bus = new EventBus();
  bus.setTailSampling({});
  const audit = bus.getSubscriptionAudit({ action: 'tail_sampling_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].actor, 'system');
  assert.deepEqual(audit[0].detail, DEFAULTS_DETAIL);
});

test('setTailSampling(true) enables with the defaults', () => {
  const bus = new EventBus();
  bus.setTailSampling(true);
  const audit = bus.getSubscriptionAudit({ action: 'tail_sampling_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, DEFAULTS_DETAIL);
});

test('a partial enable falls back to the defaults per field', () => {
  const bus = new EventBus();
  bus.setTailSampling({ successSampleRate: 0.5 });
  const audit = bus.getSubscriptionAudit({ action: 'tail_sampling_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    successSampleRate: 0.5,
    oldSuccessSampleRate: null,
    maxOpenTraces: 4096,
    oldMaxOpenTraces: null,
    maxSpansPerTrace: 512,
    oldMaxSpansPerTrace: null,
    bufferSize: 1024,
    oldBufferSize: null,
  });
  // A post-enable call revises from the live enabled values — the
  // enable is not one-shot, and the old values are now real.
  bus.setTailSampling({ maxOpenTraces: 10 });
  const revised = bus.getSubscriptionAudit({ action: 'tail_sampling_revised' });
  assert.equal(revised.length, 2);
  assert.deepEqual(revised[1].detail, {
    successSampleRate: 0.5,
    oldSuccessSampleRate: 0.5,
    maxOpenTraces: 10,
    oldMaxOpenTraces: 4096,
    maxSpansPerTrace: 512,
    oldMaxSpansPerTrace: 512,
    bufferSize: 1024,
    oldBufferSize: 1024,
  });
});

test('null on a never-enabled bus is a silent no-op with no audit', () => {
  const bus = new EventBus();
  bus.setTailSampling(null);
  assert.equal(bus.getSubscriptionAudit({ action: 'tail_sampling_revised' }).length, 0);
  // Still unconfigured: the tail callback gate still refuses.
  assert.throws(() => bus.setTailSpanCallback(() => {}), RangeError);
  assert.deepEqual(bus.getStats().tailSampledSpans, []);
  // A valid enable afterwards still works.
  bus.setTailSampling({});
  assert.equal(bus.getSubscriptionAudit({ action: 'tail_sampling_revised' }).length, 1);
});

test('an illegal value is rejected and the bus stays unconfigured', () => {
  const bus = new EventBus();
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
    [],
  ];
  for (const revision of bad) {
    assert.throws(
      () => bus.setTailSampling(revision as never),
      RangeError,
      `expected RangeError for ${JSON.stringify(revision)}`,
    );
  }
  assert.equal(bus.getSubscriptionAudit({ action: 'tail_sampling_revised' }).length, 0);
  // Still unconfigured: the tail callback gate still refuses.
  assert.throws(() => bus.setTailSpanCallback(() => {}), RangeError);
  // A valid enable afterwards still works.
  bus.setTailSampling({ successSampleRate: 1 });
  assert.equal(bus.getSubscriptionAudit({ action: 'tail_sampling_revised' }).length, 1);
});

test('after enabling, the next publish is judged at the live rate', async () => {
  const bus = new EventBus();
  // A health-probed subscriber swallows the throw (so the drain
  // survives) and reports the true failure outcome to the tail sampler.
  bus.subscribe(
    'err',
    () => {
      throw new Error('boom');
    },
    { healthProbe: true },
  );
  bus.subscribe('ok', () => {});
  bus.setTailSampling({ successSampleRate: 0 });
  // A successful trace is dropped at rate 0 ...
  bus.publish('ok', 1);
  await flush();
  assert.deepEqual(bus.getStats().tailSampledSpans, []);
  // ... while an error trace is always kept even at rate 0.
  bus.publish('err', 1);
  await flush();
  await flush();
  const errored = bus.getStats().tailSampledSpans;
  assert.ok(errored.length > 0, 'expected the error trace to be kept');
  assert.equal(
    errored.find((s) => s.name === 'bus.publish')!.attrs.tailKeepReason,
    'error',
  );
  // Now raise the rate and a successful trace is kept too.
  bus.setTailSampling({ successSampleRate: 1 });
  bus.publish('ok', 2);
  await flush();
  const spans = bus.getStats().tailSampledSpans;
  const sampled = spans.filter(
    (s) => s.name === 'bus.publish' && s.attrs.tailKeepReason === 'sampled',
  );
  assert.equal(sampled.length, 1);
});

test('ack-timeout and dead-letter traces are kept at rate 0 after the enable', async () => {
  const bus = new EventBus();
  bus.setTailSampling({ successSampleRate: 0 });
  let deliveries = 0;
  bus.subscribeReliable(
    't',
    (delivery) => {
      deliveries += 1;
      // Ack the redelivery so the trace can close.
      if (deliveries > 1) delivery.ack();
    },
    { ackTimeoutMs: 30 },
  );
  bus.publish('t', 1);
  await sleep(150);
  await flush();
  assert.ok(deliveries >= 2, 'expected a timeout redelivery');
  const timeoutSpans = bus.getStats().tailSampledSpans;
  assert.ok(timeoutSpans.length > 0, 'expected the timeout trace to be kept');
  assert.equal(
    timeoutSpans.find((s) => s.name === 'bus.publish')!.attrs.tailKeepReason,
    'ack-timeout',
  );

  const dlqBus = new EventBus();
  dlqBus.setTailSampling({ successSampleRate: 0 });
  const sub = dlqBus.subscribeReliable(
    't',
    (delivery) => delivery.nack(),
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0 } },
  );
  dlqBus.publish('t', 1);
  await flush();
  await flush();
  const dlqSpans = dlqBus.getStats().tailSampledSpans;
  assert.ok(dlqSpans.length > 0, 'expected the dead-letter trace to be kept');
  assert.equal(
    dlqSpans.find((s) => s.name === 'bus.publish')!.attrs.tailKeepReason,
    'dead-letter',
  );
  assert.equal(dlqBus.getDeadLetterMessages(sub.id).length, 1);
});

test('spans published before the enable are not traced back', async () => {
  const bus = new EventBus();
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
  // The trace was never opened — there was no sampler at publish time.
  assert.deepEqual(bus.getStats().tailSampledSpans, []);
  bus.setTailSampling({ successSampleRate: 1 });
  deliveries[0].ack();
  await flush();
  assert.deepEqual(
    bus.getStats().tailSampledSpans,
    [],
    'acking a pre-enable publish must decide nothing',
  );
  // The next publish after the enable is traced normally — once its
  // reliable leg acks and the trace is decided.
  bus.publish('t', 2);
  await flush();
  deliveries[1].ack();
  await flush();
  const spans = bus.getStats().tailSampledSpans;
  assert.ok(spans.length > 0, 'expected the post-enable trace to be kept');
  assert.equal(spans.find((s) => s.name === 'bus.publish')!.attrs.tailKeepReason, 'sampled');
});

test('the enable is live-only: a fresh bus is unconfigured again', () => {
  const bus = new EventBus();
  bus.setTailSampling({ successSampleRate: 1 });
  assert.equal(bus.getSubscriptionAudit({ action: 'tail_sampling_revised' }).length, 1);
  // A restart restores the unconfigured state: a fresh bus built from
  // the same construction options holds no sampler.
  const fresh = new EventBus();
  assert.deepEqual(fresh.getStats().tailSampledSpans, []);
  assert.throws(() => fresh.setTailSpanCallback(() => {}), RangeError);
  assert.equal(fresh.getSubscriptionAudit({ action: 'tail_sampling_revised' }).length, 0);
  // The fresh bus can still be enabled the same way.
  fresh.setTailSampling(true);
  assert.equal(fresh.getSubscriptionAudit({ action: 'tail_sampling_revised' }).length, 1);
});

test('a closed bus refuses the enable and the revision with RangeError', async () => {
  const unconfigured = new EventBus();
  await unconfigured.close();
  assert.throws(
    () => unconfigured.setTailSampling({ successSampleRate: 1 }),
    (e: unknown) => e instanceof RangeError && /bus is closed/.test((e as Error).message),
  );
  assert.equal(unconfigured.getSubscriptionAudit({ action: 'tail_sampling_revised' }).length, 0);

  const configured = new EventBus({ tailSampling: true });
  await configured.close();
  assert.throws(
    () => configured.setTailSampling({ successSampleRate: 0.5 }),
    (e: unknown) => e instanceof RangeError && /bus is closed/.test((e as Error).message),
  );
  assert.equal(configured.getSubscriptionAudit({ action: 'tail_sampling_revised' }).length, 0);
});

test('the enable shares the head channel when trace is enabled', async () => {
  const seen: TraceSpan[] = [];
  const bus = new EventBus({
    trace: { sampleRate: 0, onTraceSpan: (span) => seen.push(span) },
  });
  bus.subscribe(
    't',
    () => {
      throw new Error('boom');
    },
    { healthProbe: true },
  );
  bus.setTailSampling({ successSampleRate: 0 });
  // No callback of its own: the tail shares the head channel.
  assert.equal(bus.getStats().tailHasSpanCallback, false);
  bus.publish('t', 1);
  await flush();
  await flush();
  // Head-based sampling is off (rate 0), so every span here is tail-kept.
  assert.ok(seen.length > 0, 'expected tail spans on the shared channel');
  assert.equal(
    seen.find((s) => s.name === 'bus.publish')!.attrs.tailKeepReason,
    'error',
  );
  // A later head swap retargets the live-enabled tail too.
  const reseen: TraceSpan[] = [];
  bus.setTraceSpanCallback((span) => reseen.push(span));
  bus.publish('t', 2);
  await flush();
  await flush();
  assert.ok(reseen.length > 0, 'expected tail spans on the new head callback');
  assert.equal(
    reseen.find((s) => s.name === 'bus.publish')!.attrs.tailKeepReason,
    'error',
  );
});

test('the enable without trace keeps spans in the ring only, and a tail callback is installable afterwards', async () => {
  const bus = new EventBus();
  // Before the enable the tail callback gate still refuses.
  assert.throws(() => bus.setTailSpanCallback(() => {}), RangeError);
  bus.setTailSampling(true);
  bus.subscribe('t', () => {});
  bus.publish('t', 1);
  await flush();
  assert.ok(bus.getStats().tailSampledSpans.length > 0);
  // No head channel to share: the tail gains a callback of its own.
  const seen: TraceSpan[] = [];
  bus.setTailSpanCallback((span) => seen.push(span));
  assert.equal(bus.getStats().tailHasSpanCallback, true);
  bus.publish('t', 2);
  await flush();
  assert.ok(seen.length > 0, 'expected spans on the tail callback');
  assert.equal(
    seen.find((s) => s.name === 'bus.publish')!.attrs.tailKeepReason,
    'sampled',
  );
});
