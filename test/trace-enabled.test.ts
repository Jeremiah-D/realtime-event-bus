/**
 * Bus-level head-based trace master switch (EB-473).
 *
 * `setTraceEnabled` switches `EventBusOptions.trace` on or off at
 * runtime — without restarting the bus. A bus that never enabled
 * tracing gains a recorder live on the first enable (`opts` takes
 * only `sampleRate` / `bufferSize`, defaulting to 1 / 1024), and the
 * next admitted publish starts sampling at once. Disabling pauses
 * sampling only: the recorder and its ring stay in place, so
 * `getStats().traceSpans` keeps reporting the frozen spans, a
 * publish already sampled finishes its spans, and an attached
 * archive journal stays attached. Re-enabling resumes sampling
 * with the retained tuning, or with `opts` when given. While
 * paused, `setTrace` / `setTraceSampler` / `setTraceSpanCallback`
 * still retune the retained recorder. A same-state enable or
 * disable is a no-op with no audit. Audited as
 * `trace_enabled_revised` (`{ enabled, oldEnabled, sampleRate?,
 * bufferSize? }`).
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

function tmpLogDir(): string {
  return mkdtempSync(join(tmpdir(), 'eb473-'));
}

test('enabling a never-enabled bus starts sampling on the next publish', async () => {
  const bus = new EventBus();
  assert.equal(bus.getTraceEnabled(), false);
  assert.deepEqual(bus.getStats().traceSpans, []);
  bus.subscribe('t', () => {});
  bus.setTraceEnabled(true);
  assert.equal(bus.getTraceEnabled(), true);
  bus.publish('t', { n: 1 });
  await flush();
  const spans = bus.getStats().traceSpans;
  assert.equal(spans.length, 5);
  assert.ok(spans.some((s) => s.name === 'bus.publish'));
  assert.ok(spans.every((s) => (s.attrs.seq as number) === 1));
});

test('enable with opts tuning takes effect: rate 0 samples nothing, bufferSize caps the ring', async () => {
  const silent = new EventBus();
  silent.subscribe('t', () => {});
  silent.setTraceEnabled(true, { sampleRate: 0 });
  silent.publish('t', { n: 1 });
  await flush();
  assert.deepEqual(silent.getStats().traceSpans, []);

  const small = new EventBus();
  small.subscribe('t', () => {});
  small.setTraceEnabled(true, { bufferSize: 2 });
  small.publish('t', { n: 1 });
  await flush();
  const spans = small.getStats().traceSpans;
  assert.equal(spans.length, 2);
  assert.ok(spans.every((s) => (s.attrs.seq as number) === 1));
});

test('a repeat enable is a no-op with no audit, and opts are ignored', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.setTraceEnabled(true, { sampleRate: 1 });
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_enabled_revised' }).length, 1);
  // A repeat enable moves nothing — not even the rate in `opts`.
  bus.setTraceEnabled(true, { sampleRate: 0 });
  bus.setTraceEnabled(true);
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_enabled_revised' }).length, 1);
  assert.equal(bus.getTraceEnabled(), true);
  bus.publish('t', { n: 1 });
  await flush();
  assert.equal(bus.getStats().traceSpans.length, 5);
  // Illegal opts are still refused even on a repeat enable: validation
  // runs before the same-state no-op check.
  assert.throws(() => bus.setTraceEnabled(true, { sampleRate: 2 }), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_enabled_revised' }).length, 1);
});

test('disabling pauses sampling and freezes the ring', async () => {
  const bus = new EventBus({ trace: { sampleRate: 1 } });
  bus.subscribe('t', () => {});
  bus.publish('t', { n: 1 });
  await flush();
  assert.equal(bus.getStats().traceSpans.length, 5);
  bus.setTraceEnabled(false);
  assert.equal(bus.getTraceEnabled(), false);
  bus.publish('t', { n: 2 });
  await flush();
  // No new trace started: the ring is frozen at the seq-1 spans.
  const spans = bus.getStats().traceSpans;
  assert.equal(spans.length, 5);
  assert.ok(spans.every((s) => (s.attrs.seq as number) === 1));
  // A repeat disable is a no-op with no audit.
  bus.setTraceEnabled(false);
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_enabled_revised' }).length, 1);
});

test('re-enabling resumes sampling with the retained tuning, or the given opts', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.setTraceEnabled(true, { sampleRate: 0 });
  bus.setTraceEnabled(false);
  bus.setTraceEnabled(true);
  // The retained rate 0 survives the pause: still nothing sampled.
  bus.publish('t', { n: 1 });
  await flush();
  assert.deepEqual(bus.getStats().traceSpans, []);
  // A re-enable with opts overrides the retained tuning.
  bus.setTraceEnabled(false);
  bus.setTraceEnabled(true, { sampleRate: 1 });
  bus.publish('t', { n: 2 });
  await flush();
  assert.equal(bus.getStats().traceSpans.length, 5);
  assert.ok(bus.getStats().traceSpans.every((s) => (s.attrs.seq as number) === 2));
});

test('a sampled publish crossing disable still completes its spans', async () => {
  const bus = new EventBus({ trace: { sampleRate: 1 } });
  bus.subscribe('t', () => {});
  bus.publish('t', { n: 1 });
  // Disable before the delivery flush: the in-flight trace must finish.
  bus.setTraceEnabled(false);
  await flush();
  const spans = bus.getStats().traceSpans;
  assert.equal(spans.length, 5);
  assert.ok(spans.some((s) => s.name === 'bus.deliver'));
  assert.ok(spans.every((s) => (s.attrs.seq as number) === 1));
  // ...while a publish after the disable starts nothing.
  bus.publish('t', { n: 2 });
  await flush();
  assert.equal(bus.getStats().traceSpans.length, 5);
});

test('disabling a never-enabled bus throws RangeError', () => {
  const plain = new EventBus();
  assert.throws(() => plain.setTraceEnabled(false), RangeError);
  assert.equal(plain.getSubscriptionAudit({ action: 'trace_enabled_revised' }).length, 0);
  // A durable log directory alone is not enough: the gate is trace.
  const logged = new EventBus({ durableLogDir: tmpLogDir() });
  assert.throws(() => logged.setTraceEnabled(false), RangeError);
  assert.equal(logged.getSubscriptionAudit({ action: 'trace_enabled_revised' }).length, 0);
});

test('non-boolean enabled and illegal opts throw RangeError and leave the live state untouched', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  const badEnabled: Array<unknown> = ['yes', 1, 0, {}, [], null, undefined];
  for (const enabled of badEnabled) {
    assert.throws(() => bus.setTraceEnabled(enabled as never), RangeError);
  }
  const badOpts: Array<unknown> = [
    { sampleRate: 2 },
    { sampleRate: -0.5 },
    { sampleRate: Number.NaN },
    { sampleRate: '1' },
    { bufferSize: 0 },
    { bufferSize: -1 },
    { bufferSize: 1.5 },
    { bufferSize: '1024' },
    // One good field does not save a bad sibling.
    { sampleRate: 1, bufferSize: 0 },
    'nope',
    42,
    [],
  ];
  for (const opts of badOpts) {
    assert.throws(() => bus.setTraceEnabled(true, opts as never), RangeError);
  }
  assert.equal(bus.getTraceEnabled(), false);
  assert.deepEqual(bus.getStats().traceSpans, []);
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_enabled_revised' }).length, 0);
  // A valid enable afterwards still works.
  bus.setTraceEnabled(true);
  bus.publish('t', { n: 1 });
  return flush().then(() => {
    assert.equal(bus.getStats().traceSpans.length, 5);
  });
});

test('setTraceEnabled rejects an illegal actor', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setTraceEnabled(true, undefined, ''), RangeError);
  assert.equal(bus.getTraceEnabled(), false);
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_enabled_revised' }).length, 0);
});

test('a closed bus refuses enable', async () => {
  const bus = new EventBus();
  await bus.close();
  assert.throws(() => bus.setTraceEnabled(true), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_enabled_revised' }).length, 0);
});

test('setTrace, setTraceSampler and setTraceSpanCallback still retune a paused recorder', async () => {
  const seen: string[] = [];
  const bus = new EventBus({ trace: { sampleRate: 0 } });
  bus.subscribe('t', () => {});
  bus.setTraceEnabled(false);
  // All three retune the retained recorder while paused.
  bus.setTrace({ sampleRate: 1 });
  bus.setTraceSampler(() => true);
  bus.setTraceSpanCallback((span) => seen.push(span.name));
  bus.setTraceEnabled(true);
  bus.publish('t', { n: 1 });
  await flush();
  assert.equal(bus.getStats().traceSpans.length, 5);
  assert.ok(seen.includes('bus.publish'));
});

test('the archive journal survives a pause and resumes archiving on re-enable', async () => {
  const dir = tmpLogDir();
  const bus = new EventBus({
    durableLogDir: dir,
    trace: { sampleRate: 1, archive: true },
  });
  bus.subscribe('t', () => {});
  bus.publish('t', { n: 1 });
  await flush();
  assert.equal(bus.getTraceArchive().length, 5);
  assert.equal(bus.getStats().traceArchiveEnabled, true);
  bus.setTraceEnabled(false);
  bus.publish('t', { n: 2 });
  await flush();
  // The journal stays mounted, but no new span means no new record.
  assert.equal(bus.getStats().traceArchiveEnabled, true);
  assert.equal(bus.getTraceArchive().length, 5);
  assert.ok(bus.getTraceArchive().every((s) => (s.attrs.seq as number) === 1));
  bus.setTraceEnabled(true);
  bus.publish('t', { n: 3 });
  await flush();
  assert.equal(bus.getTraceArchive().length, 10);
  assert.equal(bus.getTraceArchive().filter((s) => (s.attrs.seq as number) === 2).length, 0);
  assert.equal(bus.getTraceArchive().filter((s) => (s.attrs.seq as number) === 3).length, 5);
});

test('the switch audits exact flags with the actor, and the getter follows the live value', () => {
  const bus = new EventBus();
  assert.equal(bus.getTraceEnabled(), false);
  bus.setTraceEnabled(true, { sampleRate: 0 }, 'ops');
  assert.equal(bus.getTraceEnabled(), true);
  bus.setTraceEnabled(false, undefined, 'ops');
  assert.equal(bus.getTraceEnabled(), false);
  bus.setTraceEnabled(true, { bufferSize: 6 }, 'ops');
  assert.equal(bus.getTraceEnabled(), true);
  const audit = bus.getSubscriptionAudit({ action: 'trace_enabled_revised' });
  assert.equal(audit.length, 3);
  assert.equal(audit[0].actor, 'ops');
  assert.deepEqual(audit[0].detail, { enabled: true, oldEnabled: false, sampleRate: 0 });
  assert.equal(audit[1].actor, 'ops');
  assert.deepEqual(audit[1].detail, { enabled: false, oldEnabled: true });
  assert.equal(audit[2].actor, 'ops');
  assert.deepEqual(audit[2].detail, { enabled: true, oldEnabled: false, bufferSize: 6 });
});
