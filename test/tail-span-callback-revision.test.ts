/**
 * Tail sampler's own span observer channel live replacement (EB-418).
 *
 * `setTailSpanCallback` swaps the `onTraceSpan` hook every tail-kept
 * span is handed to without restarting the bus. Only a bus that
 * enabled `tailSampling` can be revised; the gate is the sampler
 * being configured, not a callback already existing — a shared tail
 * (no callback of its own) can gain one. Installing a callback of
 * the tail's own splits the tail off the shared head channel for
 * good: from the split on, `setTraceSpanCallback` retargets only the
 * head recorder, and `null` clears back to no observer without
 * rejoining the shared channel (only a restart restores it). The
 * tail's own callback is installed raw, without the OTLP exporter
 * and archive journal riders, so after the split tail-kept spans no
 * longer reach OTLP export or the archive journal while head spans
 * keep flowing to both. The new callback governs from the next kept
 * span — an open tail trace already buffered is not re-judged, and
 * its spans are delivered to the new callback when the trace is
 * decided. The callback function itself is never written to the
 * audit log: only whether one is installed is recorded
 * (`{ hasHandler, oldHasHandler }`). Re-installing the live callback
 * is a no-op with no audit, and a `null` against a shared tail
 * leaves the shared channel untouched.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, type Server, type IncomingMessage, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type TraceSpan } from '../src/bus.ts';

const flush = () => new Promise((resolve) => setImmediate(resolve));

/** Polls `cond` until true or `timeoutMs` elapses. */
async function waitFor(cond: () => boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now();
  for (;;) {
    if (cond()) return;
    if (Date.now() - start > timeoutMs) throw new Error('waitFor timed out');
    await new Promise((r) => setTimeout(r, 10));
  }
}

interface CapturedRequest {
  url: string;
  body: string;
}

/**
 * Stub OTLP/HTTP collector on 127.0.0.1. Records every POST body. The
 * exporter uses `agent: false`, so connections close after each
 * response and `close()` never hangs on pooled sockets.
 */
function startCollector(): {
  requests: CapturedRequest[];
  endpoint: Promise<string>;
  close: () => Promise<void>;
} {
  const requests: CapturedRequest[] = [];
  const server: Server = createServer((req: IncomingMessage, res: ServerResponse) => {
    let body = '';
    req.on('data', (chunk: Buffer) => {
      body += chunk.toString('utf8');
    });
    req.on('end', () => {
      requests.push({ url: req.url ?? '/', body });
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end('{}');
    });
  });
  const endpoint = new Promise<string>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const addr = server.address() as AddressInfo;
      resolve(`http://127.0.0.1:${addr.port}/v1/traces`);
    });
  });
  return {
    requests,
    endpoint,
    close: () =>
      new Promise<void>((resolve, reject) => {
        server.close((err) => (err == null ? resolve() : reject(err)));
      }),
  };
}

function tmpLogDir(): string {
  return mkdtempSync(join(tmpdir(), 'eb418-'));
}

const tailKept = (spans: TraceSpan[]): TraceSpan[] =>
  spans.filter((s) => s.attrs.tailKeepReason !== undefined);

test('setTailSpanCallback fails closed when tailSampling is not enabled', () => {
  const handler = (_span: TraceSpan): void => {};
  const plain = new EventBus();
  assert.throws(() => plain.setTailSpanCallback(handler), RangeError);
  assert.throws(() => plain.setTailSpanCallback(null), RangeError);
  assert.equal(plain.getSubscriptionAudit({ action: 'tail_span_callback_revised' }).length, 0);
  // Trace alone is not enough: the gate is the tail sampler.
  const traced = new EventBus({ trace: { sampleRate: 1 } });
  assert.throws(() => traced.setTailSpanCallback(handler), RangeError);
  assert.throws(() => traced.setTailSpanCallback(null), RangeError);
  assert.equal(traced.getSubscriptionAudit({ action: 'tail_span_callback_revised' }).length, 0);
});

test('setTailSpanCallback rejects an illegal actor', () => {
  const bus = new EventBus({ tailSampling: true });
  const handler = (_span: TraceSpan): void => {};
  assert.throws(() => bus.setTailSpanCallback(handler, ''), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'tail_span_callback_revised' }).length, 0);
});

test('a closed bus refuses the swap', async () => {
  const bus = new EventBus({ tailSampling: true });
  await bus.close();
  const handler = (_span: TraceSpan): void => {};
  assert.throws(() => bus.setTailSpanCallback(handler), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'tail_span_callback_revised' }).length, 0);
});

test('setTailSpanCallback rejects non-functions and leaves the live channel untouched', async () => {
  const live: TraceSpan[] = [];
  const bus = new EventBus({ tailSampling: { onTraceSpan: (span) => live.push(span) } });
  bus.subscribe('t', () => {});
  const bad: Array<unknown> = ['nope', 42, {}, [], undefined];
  for (const handler of bad) {
    assert.throws(() => bus.setTailSpanCallback(handler as never), RangeError);
  }
  assert.equal(bus.getSubscriptionAudit({ action: 'tail_span_callback_revised' }).length, 0);
  bus.publish('t', 1);
  await flush();
  // The live channel survived every rejected call.
  assert.equal(tailKept(live).length, 1);
  assert.equal(tailKept(live)[0].attrs.tailKeepReason, 'sampled');
  assert.equal(bus.getStats().tailHasSpanCallback, true);
});

test('re-installing the live callback is a no-op with no audit', async () => {
  const live: TraceSpan[] = [];
  const handler = (span: TraceSpan): void => {
    live.push(span);
  };
  const bus = new EventBus({ tailSampling: { onTraceSpan: handler } });
  bus.subscribe('t', () => {});
  bus.setTailSpanCallback(handler);
  assert.equal(bus.getSubscriptionAudit({ action: 'tail_span_callback_revised' }).length, 0);
  bus.publish('t', 1);
  await flush();
  assert.equal(tailKept(live).length, 1);
  assert.equal(bus.getStats().tailHasSpanCallback, true);
});

test('clearing a shared tail is a no-op and the shared channel survives', async () => {
  const headOld: TraceSpan[] = [];
  const headNew: TraceSpan[] = [];
  const bus = new EventBus({
    trace: { sampleRate: 1, onTraceSpan: (span) => headOld.push(span) },
    tailSampling: true,
  });
  bus.subscribe('t', () => {});
  // `null` against a tail with no callback of its own: no split, no audit.
  bus.setTailSpanCallback(null);
  assert.equal(bus.getSubscriptionAudit({ action: 'tail_span_callback_revised' }).length, 0);
  assert.equal(bus.getStats().tailHasSpanCallback, false);
  // The shared channel is intact: a head swap still retargets the tail.
  bus.setTraceSpanCallback((span) => headNew.push(span));
  bus.publish('t', 1);
  await flush();
  assert.equal(tailKept(headNew).length, 1);
  assert.equal(tailKept(headNew)[0].attrs.tailKeepReason, 'sampled');
  assert.equal(tailKept(headOld).length, 0);
});

test('a shared tail gains its own callback: the next kept span leaves the head channel', async () => {
  const headSeen: TraceSpan[] = [];
  const tailSeen: TraceSpan[] = [];
  const bus = new EventBus({
    trace: { sampleRate: 1, onTraceSpan: (span) => headSeen.push(span) },
    tailSampling: true,
  });
  bus.subscribe('t', () => {});
  bus.publish('t', 1);
  await flush();
  // Shared channel: the head callback saw head spans and tail-kept spans.
  assert.equal(bus.getStats().tailSampledSpans.length, 5);
  assert.equal(tailKept(headSeen).length, 1);
  assert.equal(tailKept(headSeen)[0].attrs.tailKeepReason, 'sampled');
  const headCount = headSeen.length;
  bus.setTailSpanCallback((span) => tailSeen.push(span));
  bus.publish('t', 2);
  await flush();
  // The tail's kept spans now flow only to its own callback, while the
  // head spans keep flowing to the head callback.
  assert.equal(bus.getStats().tailSampledSpans.length, 10);
  assert.equal(tailSeen.length, 5);
  assert.equal(tailKept(tailSeen).length, 1);
  assert.equal(tailKept(tailSeen)[0].attrs.tailKeepReason, 'sampled');
  assert.equal(tailKept(tailSeen)[0].attrs.seq, 2);
  assert.equal(headSeen.length - headCount, 5);
  assert.equal(tailKept(headSeen).length, 1);
});

test('after the split, a head swap no longer retargets the tail', async () => {
  const headOld: TraceSpan[] = [];
  const headNew: TraceSpan[] = [];
  const tailSeen: TraceSpan[] = [];
  const bus = new EventBus({
    trace: { sampleRate: 1, onTraceSpan: (span) => headOld.push(span) },
    tailSampling: true,
  });
  bus.subscribe('t', () => {});
  bus.setTailSpanCallback((span) => tailSeen.push(span));
  bus.publish('t', 1);
  await flush();
  assert.equal(tailKept(tailSeen).length, 1);
  assert.equal(tailKept(headOld).length, 0);
  const tailCount = tailSeen.length;
  const headOldCount = headOld.length;
  bus.setTraceSpanCallback((span) => headNew.push(span));
  bus.publish('t', 2);
  await flush();
  // The head swap retargeted only the head channel: the tail kept
  // flowing to its own callback, the old head callback is retired, and
  // the new head callback sees only head spans.
  assert.ok(tailSeen.length > tailCount);
  assert.equal(tailKept(tailSeen).length, 2);
  assert.equal(tailKept(tailSeen)[1].attrs.seq, 2);
  assert.equal(headOld.length, headOldCount);
  assert.ok(headNew.length > 0);
  assert.equal(tailKept(headNew).length, 0);
});

test('after the split, tail-kept spans no longer reach OTLP export or the archive journal', async () => {
  const collector = startCollector();
  const endpoint = await collector.endpoint;
  const dir = tmpLogDir();
  const headSeen: TraceSpan[] = [];
  const tailSeen: TraceSpan[] = [];
  const bus = new EventBus({
    durableLogDir: dir,
    trace: { sampleRate: 1, archive: true, onTraceSpan: (span) => headSeen.push(span) },
    otlp: { endpoint, batchSize: 2, flushIntervalMs: 60_000 },
    tailSampling: true,
  });
  try {
    bus.subscribe('t', () => {});
    bus.publish('t', 1);
    await flush();
    // Shared channel: head spans and tail-kept spans both ride the
    // composed closure, so the head callback, the journal, and the
    // exporter all saw the same total.
    const sharedTotal = headSeen.length;
    assert.equal(bus.getStats().tailSampledSpans.length, 5);
    assert.ok(sharedTotal > 5);
    await waitFor(() => (bus.getStats().otlp?.exportedSpans ?? 0) === sharedTotal);
    assert.equal(bus.getTraceArchive().length, sharedTotal);
    const exportedBefore = bus.getStats().otlp?.exportedSpans ?? 0;
    bus.setTailSpanCallback((span) => tailSeen.push(span));
    bus.publish('t', 2);
    await flush();
    // The tail's kept spans now flow only to its own callback.
    assert.equal(tailSeen.length, 5);
    assert.equal(tailKept(tailSeen).length, 1);
    assert.equal(tailKept(tailSeen)[0].attrs.seq, 2);
    const headDelta = headSeen.length - sharedTotal;
    assert.ok(headDelta > 0);
    assert.ok(headDelta < sharedTotal);
    // The journal and the exporter grew by head spans only — the tail
    // portion stopped flowing the moment the channels split.
    assert.equal(bus.getTraceArchive().length - sharedTotal, headDelta);
    await waitFor(() => (bus.getStats().otlp?.exportedSpans ?? 0) === exportedBefore + headDelta);
  } finally {
    await collector.close();
  }
});

test('clearing the tail callback keeps spans in the ring only and never rejoins the shared channel', async () => {
  const headOld: TraceSpan[] = [];
  const headNew: TraceSpan[] = [];
  const tailSeen: TraceSpan[] = [];
  const bus = new EventBus({
    trace: { sampleRate: 1, onTraceSpan: (span) => headOld.push(span) },
    tailSampling: { onTraceSpan: (span) => tailSeen.push(span) },
  });
  bus.subscribe('t', () => {});
  bus.publish('t', 1);
  await flush();
  assert.equal(tailKept(tailSeen).length, 1);
  const tailCount = tailSeen.length;
  bus.setTailSpanCallback(null);
  assert.equal(bus.getStats().tailHasSpanCallback, false);
  bus.publish('t', 2);
  await flush();
  // Nothing new reached the tail callback, but the ring kept recording.
  assert.equal(tailSeen.length, tailCount);
  assert.equal(bus.getStats().tailSampledSpans.length, 10);
  // No auto-rejoin: a later head swap still does not retarget the tail.
  bus.setTraceSpanCallback((span) => headNew.push(span));
  bus.publish('t', 3);
  await flush();
  assert.equal(tailSeen.length, tailCount);
  assert.equal(bus.getStats().tailSampledSpans.length, 15);
  assert.ok(headNew.length > 0);
  assert.equal(tailKept(headNew).length, 0);
});

test('a buffered open trace is decided under the new callback, not re-judged', async () => {
  const oldSeen: TraceSpan[] = [];
  const newSeen: TraceSpan[] = [];
  const bus = new EventBus({
    tailSampling: { onTraceSpan: (span) => oldSeen.push(span) },
  });
  bus.subscribe('t', () => {});
  bus.publish('t', 1);
  // Swap before the trace is decided: the buffered spans must go to
  // the new callback, never to the old one, and the trace is decided
  // exactly once.
  bus.setTailSpanCallback((span) => newSeen.push(span));
  await flush();
  assert.equal(oldSeen.length, 0);
  assert.equal(newSeen.length, 5);
  assert.equal(tailKept(newSeen).length, 1);
  assert.equal(tailKept(newSeen)[0].attrs.tailKeepReason, 'sampled');
  assert.equal(bus.getStats().tailSampledSpans.length, 5);
});

test('a throwing tail callback is swallowed and publish stays intact', async () => {
  const received: unknown[] = [];
  const good: TraceSpan[] = [];
  const bus = new EventBus({ tailSampling: true });
  bus.subscribe('t', (msg) => received.push(msg.payload));
  const boom = (_span: TraceSpan): void => {
    throw new Error('observer exploded');
  };
  bus.setTailSpanCallback(boom);
  bus.publish('t', 1);
  await flush();
  // The throw never disturbed the publish/deliver path, and the ring
  // kept recording through it.
  assert.deepEqual(received, [1]);
  assert.equal(bus.getStats().tailSampledSpans.length, 5);
  // Swapping back to a legal callback restores observation at once.
  bus.setTailSpanCallback((span) => good.push(span));
  bus.publish('t', 2);
  await flush();
  assert.deepEqual(received, [1, 2]);
  assert.equal(good.length, 5);
  assert.ok(good.every((s) => (s.attrs.seq as number) === 2));
});

test('the swap audits exact presence flags with the actor', () => {
  const seen: TraceSpan[] = [];
  const firstHandler = (span: TraceSpan): void => {
    seen.push(span);
  };
  const bus = new EventBus({ tailSampling: true });
  bus.setTailSpanCallback(firstHandler, 'ops');
  bus.setTailSpanCallback(null, 'ops');
  const audit = bus.getSubscriptionAudit({ action: 'tail_span_callback_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[0].detail, { hasHandler: true, oldHasHandler: false });
  assert.equal(audit[0].actor, 'ops');
  assert.deepEqual(audit[1].detail, { hasHandler: false, oldHasHandler: true });
  assert.equal(audit[1].actor, 'ops');
  // The function itself is never recorded — only its presence.
  assert.ok(!JSON.stringify(audit).includes('firstHandler'));
  assert.equal(seen.length, 0);
});

test('getStats().tailHasSpanCallback follows the live value', () => {
  const handler = (_span: TraceSpan): void => {};
  const bus = new EventBus({ tailSampling: true });
  assert.equal(bus.getStats().tailHasSpanCallback, false);
  bus.setTailSpanCallback(handler);
  assert.equal(bus.getStats().tailHasSpanCallback, true);
  bus.setTailSpanCallback(null);
  assert.equal(bus.getStats().tailHasSpanCallback, false);
  // A bus with a callback at construction reports true; a shared tail
  // reports false; a bus without tail sampling reports false, not
  // undefined.
  const withHandler = new EventBus({ tailSampling: { onTraceSpan: handler } });
  assert.equal(withHandler.getStats().tailHasSpanCallback, true);
  const shared = new EventBus({ trace: { sampleRate: 1 }, tailSampling: true });
  assert.equal(shared.getStats().tailHasSpanCallback, false);
  const plain = new EventBus();
  assert.equal(plain.getStats().tailHasSpanCallback, false);
});

test('swapping the callback leaves the four numeric bounds and the ring untouched', async () => {
  const seen: TraceSpan[] = [];
  const bus = new EventBus({ tailSampling: { onTraceSpan: (span) => seen.push(span) } });
  bus.subscribe('t', () => {});
  bus.publish('t', 1);
  await flush();
  const ringBefore = bus
    .getStats()
    .tailSampledSpans.map((s) => [s.name, s.attrs.seq as number]);
  assert.equal(ringBefore.length, 5);
  bus.setTailSpanCallback((span) => seen.push(span));
  assert.deepEqual(
    bus
      .getStats()
      .tailSampledSpans.map((s) => [s.name, s.attrs.seq as number]),
    ringBefore,
  );
  bus.publish('t', 2);
  await flush();
  assert.equal(bus.getStats().tailSampledSpans.length, 10);
  assert.ok(bus.getStats().tailSampledSpans.every((s) => [1, 2].includes(s.attrs.seq as number)));

  // The rate and buffer bounds survive the swap: rate 0 still drops
  // successful traces, and a kept error trace is still trimmed to the
  // live bufferSize of 3.
  const quiet: TraceSpan[] = [];
  const bounded = new EventBus({
    tailSampling: { successSampleRate: 0, bufferSize: 3, onTraceSpan: (span) => quiet.push(span) },
  });
  bounded.subscribe('t', () => {});
  bounded.setTailSpanCallback((span) => quiet.push(span));
  bounded.publish('t', 1);
  await flush();
  assert.deepEqual(bounded.getStats().tailSampledSpans, []);
  // A health-probed subscriber swallows the throw (so the drain
  // survives) and reports the true failure outcome to the tail sampler.
  bounded.subscribe(
    't',
    () => {
      throw new Error('handler blew up');
    },
    { healthProbe: true },
  );
  bounded.publish('t', 2);
  await flush();
  await flush();
  // An error trace is always kept, even at rate 0 — trimmed to the
  // live ring bound of 3.
  assert.equal(bounded.getStats().tailSampledSpans.length, 3);
});
