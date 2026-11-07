/**
 * Bus-level trace span observer channel live replacement (EB-416).
 *
 * `setTraceSpanCallback` swaps the `onTraceSpan` hook every sampled span
 * is handed to without restarting the bus. Only a bus that enabled
 * `trace` can be revised; the new callback governs from the next span
 * — a span already emitted is never re-delivered — and `null` clears
 * back to no observer while the ring keeps recording. The replacement
 * re-composes the single shared closure (user hook, then the OTLP
 * exporter, then the archive journal) and installs it in both emitters
 * that share the channel — the head recorder and, when the tail sampler
 * fell back to the head channel at construction, the tail sampler — so
 * neither rider is dropped and the two channels never split, while a
 * tail sampler with its own callback is a separate channel and is
 * untouched. The callback function itself is never written to the audit
 * log: only whether one is installed is recorded (`{ hasHandler,
 * oldHasHandler }`). Re-installing the live callback is a no-op with no
 * audit.
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
  return mkdtempSync(join(tmpdir(), 'eb416-'));
}

const tailKept = (spans: TraceSpan[]): TraceSpan[] =>
  spans.filter((s) => s.attrs.tailKeepReason !== undefined);

test('setTraceSpanCallback fails closed when trace is not enabled', () => {
  const bus = new EventBus();
  const handler = (_span: TraceSpan): void => {};
  assert.throws(() => bus.setTraceSpanCallback(handler), RangeError);
  assert.throws(() => bus.setTraceSpanCallback(null), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_span_callback_revised' }).length, 0);
});

test('setTraceSpanCallback rejects an illegal actor', () => {
  const bus = new EventBus({ trace: true });
  const handler = (_span: TraceSpan): void => {};
  assert.throws(() => bus.setTraceSpanCallback(handler, ''), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_span_callback_revised' }).length, 0);
});

test('a closed bus refuses the swap', async () => {
  const bus = new EventBus({ trace: true });
  await bus.close();
  const handler = (_span: TraceSpan): void => {};
  assert.throws(() => bus.setTraceSpanCallback(handler), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_span_callback_revised' }).length, 0);
});

test('setTraceSpanCallback rejects non-functions and leaves the live channel untouched', async () => {
  const live: TraceSpan[] = [];
  const bus = new EventBus({ trace: { sampleRate: 1, onTraceSpan: (span) => live.push(span) } });
  const bad: Array<unknown> = ['nope', 42, {}, [], undefined];
  for (const handler of bad) {
    assert.throws(
      () => bus.setTraceSpanCallback(handler as never),
      RangeError,
      `expected RangeError for ${String(handler)}`,
    );
  }
  // The failed calls changed nothing: the live callback still observes
  // every sampled span (one publish, no subscriber: 3 spans).
  bus.publish('orders', 1);
  await flush();
  assert.equal(live.length, 3);
  assert.ok(live.every((s) => (s.attrs.seq as number) === 1));
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_span_callback_revised' }).length, 0);
});

test('re-installing the live callback is a no-op with no audit', () => {
  const handler = (_span: TraceSpan): void => {};
  const bus = new EventBus({ trace: { sampleRate: 1, onTraceSpan: handler } });
  bus.setTraceSpanCallback(handler);
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_span_callback_revised' }).length, 0);
  assert.equal(bus.getStats().traceHasSpanHandler, true);
});

test('clearing a bus with no callback is a no-op with no audit', () => {
  const bus = new EventBus({ trace: { sampleRate: 1 } });
  bus.setTraceSpanCallback(null);
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_span_callback_revised' }).length, 0);
  assert.equal(bus.getStats().traceHasSpanHandler, false);
});

test('installing a callback governs the next span: replacing retires the old one', async () => {
  const oldSeen: TraceSpan[] = [];
  const newSeen: TraceSpan[] = [];
  const bus = new EventBus({ trace: { sampleRate: 1 } });
  bus.publish('orders', 1);
  await flush();
  assert.equal(bus.getStats().traceSpans.length, 3);
  assert.deepEqual(oldSeen, []);
  bus.setTraceSpanCallback((span) => oldSeen.push(span));
  bus.publish('orders', 2);
  await flush();
  assert.equal(oldSeen.length, 3);
  assert.ok(oldSeen.every((s) => (s.attrs.seq as number) === 2));
  bus.setTraceSpanCallback((span) => newSeen.push(span));
  bus.publish('orders', 3);
  await flush();
  // The old callback is retired: it saw nothing of seq 3, and the new
  // callback saw exactly the seq-3 spans.
  assert.equal(oldSeen.length, 3);
  assert.equal(newSeen.length, 3);
  assert.ok(newSeen.every((s) => (s.attrs.seq as number) === 3));
  assert.equal(bus.getStats().traceSpans.length, 9);
});

test('clearing sends spans to the ring only', async () => {
  const seen: TraceSpan[] = [];
  const bus = new EventBus({ trace: { sampleRate: 1, onTraceSpan: (span) => seen.push(span) } });
  bus.publish('orders', 1);
  await flush();
  assert.equal(seen.length, 3);
  bus.setTraceSpanCallback(null);
  bus.publish('orders', 2);
  await flush();
  // Nothing new reached the callback, but the ring kept recording.
  assert.equal(seen.length, 3);
  assert.equal(bus.getStats().traceSpans.length, 6);
  assert.equal(bus.getStats().traceHasSpanHandler, false);
});

test('the swap keeps OTLP export flowing', async () => {
  const collector = startCollector();
  const endpoint = await collector.endpoint;
  const oldSeen: TraceSpan[] = [];
  const newSeen: TraceSpan[] = [];
  const bus = new EventBus({
    trace: { sampleRate: 1, onTraceSpan: (span) => oldSeen.push(span) },
    otlp: { endpoint, batchSize: 2, flushIntervalMs: 60_000 },
  });
  try {
    bus.subscribe('t', () => {});
    bus.publish('t', 'a');
    await flush();
    await waitFor(() => (bus.getStats().otlp?.exportedSpans ?? 0) > 0);
    const exportedBefore = bus.getStats().otlp?.exportedSpans ?? 0;
    assert.ok(exportedBefore > 0);
    assert.ok(oldSeen.length > 0);
    // Swap the observer: the exporter rides the same composed closure,
    // so a naive field swap would strand it — exports must continue.
    bus.setTraceSpanCallback((span) => newSeen.push(span));
    bus.publish('t', 'b');
    await flush();
    await waitFor(() => (bus.getStats().otlp?.exportedSpans ?? 0) > exportedBefore);
    assert.equal(oldSeen.length, 5);
    assert.ok(newSeen.length > 0);
    assert.ok(newSeen.every((s) => (s.attrs.seq as number) === 2));
  } finally {
    await collector.close();
  }
});

test('the swap keeps the archive journal recording', async () => {
  const dir = tmpLogDir();
  const oldSeen: TraceSpan[] = [];
  const newSeen: TraceSpan[] = [];
  const bus = new EventBus({
    durableLogDir: dir,
    trace: { sampleRate: 1, archive: true, onTraceSpan: (span) => oldSeen.push(span) },
  });
  bus.subscribe('orders.*', () => {});
  bus.publish('orders.new', { id: 1 });
  await flush();
  // The journal sees exactly what the callback saw.
  assert.equal(bus.getTraceArchive().length, oldSeen.length);
  assert.ok(oldSeen.length > 0);
  bus.setTraceSpanCallback((span) => newSeen.push(span));
  bus.publish('orders.cancel', { id: 2 });
  await flush();
  // The journal kept recording under the new callback, and the old
  // callback saw nothing of the second publish.
  assert.equal(bus.getTraceArchive().length, oldSeen.length + newSeen.length);
  assert.ok(newSeen.length > 0);
  assert.ok(newSeen.every((s) => s.attrs.topic === 'orders.cancel'));
  assert.ok(oldSeen.every((s) => s.attrs.topic === 'orders.new'));
});

test('a shared tail follows the new callback', async () => {
  const oldSeen: TraceSpan[] = [];
  const newSeen: TraceSpan[] = [];
  const bus = new EventBus({
    trace: { sampleRate: 1, onTraceSpan: (span) => oldSeen.push(span) },
    tailSampling: true,
  });
  bus.subscribe('t', () => {});
  bus.publish('t', 1);
  await flush();
  // One kept tail trace: 5 spans, its publish root stamped.
  assert.equal(bus.getStats().tailSampledSpans.length, 5);
  assert.equal(tailKept(oldSeen).length, 1);
  assert.equal(tailKept(oldSeen)[0].attrs.tailKeepReason, 'sampled');
  const oldCount = oldSeen.length;
  bus.setTraceSpanCallback((span) => newSeen.push(span));
  bus.publish('t', 2);
  await flush();
  // The old callback is retired everywhere, and the tail-kept spans of
  // the second publish followed the new callback.
  assert.equal(oldSeen.length, oldCount);
  assert.equal(bus.getStats().tailSampledSpans.length, 10);
  assert.equal(tailKept(newSeen).length, 1);
  assert.equal(tailKept(newSeen)[0].attrs.tailKeepReason, 'sampled');
  assert.equal(tailKept(newSeen)[0].attrs.seq, 2);
});

test('a tail with its own callback is untouched by a head swap', async () => {
  const headOld: TraceSpan[] = [];
  const headNew: TraceSpan[] = [];
  const tailOwn: TraceSpan[] = [];
  const bus = new EventBus({
    trace: { sampleRate: 1, onTraceSpan: (span) => headOld.push(span) },
    tailSampling: { onTraceSpan: (span) => tailOwn.push(span) },
  });
  bus.subscribe('t', () => {});
  bus.publish('t', 1);
  await flush();
  // Separate channels from the start: the tail's own callback sees only
  // tail-kept spans, the head callback only head spans.
  assert.equal(tailKept(tailOwn).length, 1);
  assert.equal(tailKept(headOld).length, 0);
  assert.ok(headOld.length > 0);
  const tailCount = tailOwn.length;
  const headOldCount = headOld.length;
  bus.setTraceSpanCallback((span) => headNew.push(span));
  bus.publish('t', 2);
  await flush();
  // The head swap retargeted only the head channel: the tail kept
  // flowing to its own callback, the old head callback is retired, and
  // the new head callback sees only head spans.
  assert.ok(tailOwn.length > tailCount);
  assert.equal(tailKept(tailOwn).length, 2);
  assert.equal(headOld.length, headOldCount);
  assert.ok(headNew.length > 0);
  assert.equal(tailKept(headNew).length, 0);
});

test('a throwing callback is swallowed and publish stays intact', async () => {
  const received: unknown[] = [];
  const good: TraceSpan[] = [];
  const bus = new EventBus({ trace: { sampleRate: 1 } });
  bus.subscribe('orders', (msg) => received.push(msg.payload));
  const boom = (_span: TraceSpan): void => {
    throw new Error('observer exploded');
  };
  bus.setTraceSpanCallback(boom);
  bus.publish('orders', 1);
  await flush();
  // The throw never disturbed the publish/deliver path, and the ring
  // kept recording through it.
  assert.deepEqual(received, [1]);
  assert.equal(bus.getStats().traceSpans.length, 5);
  // Swapping back to a legal callback restores observation at once.
  bus.setTraceSpanCallback((span) => good.push(span));
  bus.publish('orders', 2);
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
  const bus = new EventBus({ trace: { sampleRate: 1 } });
  bus.setTraceSpanCallback(firstHandler, 'ops');
  bus.setTraceSpanCallback(null, 'ops');
  const audit = bus.getSubscriptionAudit({ action: 'trace_span_callback_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[0].detail, { hasHandler: true, oldHasHandler: false });
  assert.equal(audit[0].actor, 'ops');
  assert.deepEqual(audit[1].detail, { hasHandler: false, oldHasHandler: true });
  assert.equal(audit[1].actor, 'ops');
  // The function itself is never recorded — only its presence.
  assert.ok(!JSON.stringify(audit).includes('firstHandler'));
  assert.equal(seen.length, 0);
});

test('getStats().traceHasSpanHandler follows the live value', () => {
  const handler = (_span: TraceSpan): void => {};
  const bus = new EventBus({ trace: { sampleRate: 1 } });
  assert.equal(bus.getStats().traceHasSpanHandler, false);
  bus.setTraceSpanCallback(handler);
  assert.equal(bus.getStats().traceHasSpanHandler, true);
  bus.setTraceSpanCallback(null);
  assert.equal(bus.getStats().traceHasSpanHandler, false);
  // A bus with a callback at construction reports true, and a bus
  // without tracing reports false, not undefined.
  const withHandler = new EventBus({ trace: { sampleRate: 1, onTraceSpan: handler } });
  assert.equal(withHandler.getStats().traceHasSpanHandler, true);
  const plain = new EventBus();
  assert.equal(plain.getStats().traceHasSpanHandler, false);
});

test('swapping the callback leaves sampleRate, bufferSize, sampler and the ring untouched', async () => {
  // No-sampler bus: a live rate of 1 keeps tracing after the swap.
  const plain = new EventBus({ trace: { sampleRate: 1, bufferSize: 100 } });
  plain.publish('orders', 1);
  await flush();
  const plainBefore = plain.getStats().traceSpans.map((s) => [s.name, s.attrs.seq as number]);
  assert.equal(plainBefore.length, 3);
  plain.setTraceSpanCallback((_span) => {});
  assert.deepEqual(
    plain.getStats().traceSpans.map((s) => [s.name, s.attrs.seq as number]),
    plainBefore,
  );
  plain.publish('orders', 2);
  await flush();
  assert.equal(plain.getStats().traceSpans.length, 6);
  assert.ok(plain.getStats().traceSpans.every((s) => [1, 2].includes(s.attrs.seq as number)));

  // Sampler bus: rate 0 with an allow-all sampler, buffer 6 — the swap
  // must keep the sampler deciding and the buffer capping the ring.
  const allowAll = () => true;
  const sampled = new EventBus({
    trace: { sampleRate: 0, bufferSize: 6, sampler: allowAll },
  });
  sampled.publish('orders', 1);
  await flush();
  const sampledBefore = sampled.getStats().traceSpans.map((s) => [s.name, s.attrs.seq as number]);
  assert.equal(sampledBefore.length, 3);
  sampled.setTraceSpanCallback((_span) => {});
  assert.deepEqual(
    sampled.getStats().traceSpans.map((s) => [s.name, s.attrs.seq as number]),
    sampledBefore,
  );
  assert.equal(sampled.getStats().traceHasCustomSampler, true);
  sampled.publish('orders', 2);
  await flush();
  // Still traced (the sampler survived the swap despite rate 0), and
  // the ring is still capped at the live bufferSize of 6.
  assert.equal(sampled.getStats().traceSpans.length, 6);
  assert.ok(sampled.getStats().traceSpans.some((s) => (s.attrs.seq as number) === 2));
  assert.equal(sampled.getStats().traceHasCustomSampler, true);
});
