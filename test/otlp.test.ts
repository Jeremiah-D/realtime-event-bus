import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, type Server, type IncomingMessage, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import { EventBus } from '../src/bus.ts';
import { OtlpSpanExporter, resolveOtlpOptions } from '../src/otlp.ts';
import type { TraceSpan } from '../src/trace.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

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
 * Stub OTLP/HTTP collector on 127.0.0.1. Records every POST body; the
 * response status is configurable per test (default 200). The exporter
 * uses `agent: false`, so connections close after each response and
 * `close()` never hangs on pooled sockets.
 */
function startCollector(statusCode = 200): {
  requests: CapturedRequest[];
  setStatus: (code: number) => void;
  endpoint: Promise<string>;
  close: () => Promise<void>;
} {
  const requests: CapturedRequest[] = [];
  let status = statusCode;
  const server: Server = createServer((req: IncomingMessage, res: ServerResponse) => {
    let body = '';
    req.on('data', (chunk: Buffer) => {
      body += chunk.toString('utf8');
    });
    req.on('end', () => {
      requests.push({ url: req.url ?? '/', body });
      res.writeHead(status, { 'content-type': 'application/json' });
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
    setStatus: (code: number) => {
      status = code;
    },
    endpoint,
    close: () =>
      new Promise<void>((resolve, reject) => {
        server.close((err) => (err == null ? resolve() : reject(err)));
      }),
  };
}

function makeSpan(overrides: Partial<TraceSpan> = {}): TraceSpan {
  return {
    traceId: 'a'.repeat(32),
    spanId: 'b'.repeat(16),
    name: 'bus.publish',
    at: 1_700_000_000_000,
    durationMs: 5,
    attrs: { topic: 't.1', seq: 7 },
    ...overrides,
  };
}

function decodeSingleSpan(body: string, serviceName = 'realtime-event-bus'): Record<string, unknown> {
  const parsed = JSON.parse(body) as {
    resourceSpans: Array<{
      resource: { attributes: Array<{ key: string; value: { stringValue: string } }> };
      scopeSpans: Array<{ scope: { name: string }; spans: Array<Record<string, unknown>> }>;
    }>;
  };
  assert.equal(parsed.resourceSpans.length, 1);
  const [rs] = parsed.resourceSpans;
  assert.deepEqual(rs.resource.attributes, [{ key: 'service.name', value: { stringValue: serviceName } }]);
  assert.equal(rs.scopeSpans.length, 1);
  const [scope] = rs.scopeSpans;
  assert.equal(scope.scope.name, 'realtime-event-bus');
  assert.equal(scope.spans.length, 1);
  return scope.spans[0];
}

// --- resolveOtlpOptions ---

test('resolveOtlpOptions: absent/false/null disables export', () => {
  assert.equal(resolveOtlpOptions(undefined), undefined);
  assert.equal(resolveOtlpOptions(null), undefined);
  assert.equal(resolveOtlpOptions(false), undefined);
});

test('resolveOtlpOptions: rejects invalid configs with RangeError', () => {
  assert.throws(() => resolveOtlpOptions('x' as never), RangeError);
  assert.throws(() => resolveOtlpOptions({} as never), RangeError); // no endpoint
  assert.throws(() => resolveOtlpOptions({ endpoint: '' }), RangeError);
  assert.throws(() => resolveOtlpOptions({ endpoint: '::not a url::' }), RangeError);
  assert.throws(() => resolveOtlpOptions({ endpoint: 'ftp://x/traces' }), RangeError);
  assert.throws(() => resolveOtlpOptions({ endpoint: 'http://x', sampleRate: 2 }), RangeError);
  assert.throws(() => resolveOtlpOptions({ endpoint: 'http://x', sampleRate: -0.1 }), RangeError);
  assert.throws(() => resolveOtlpOptions({ endpoint: 'http://x', batchSize: 0 }), RangeError);
  assert.throws(() => resolveOtlpOptions({ endpoint: 'http://x', batchSize: 1.5 }), RangeError);
  assert.throws(() => resolveOtlpOptions({ endpoint: 'http://x', flushIntervalMs: 0 }), RangeError);
  assert.throws(() => resolveOtlpOptions({ endpoint: 'http://x', timeoutMs: -1 }), RangeError);
  assert.throws(() => resolveOtlpOptions({ endpoint: 'http://x', headers: { a: 1 } as never }), RangeError);
  assert.throws(() => resolveOtlpOptions({ endpoint: 'http://x', serviceName: '' }), RangeError);
  assert.throws(() => resolveOtlpOptions({ endpoint: 'http://x', maxBuffered: 0 }), RangeError);
});

test('resolveOtlpOptions: applies defaults', () => {
  const r = resolveOtlpOptions({ endpoint: 'http://collector:4318/v1/traces' })!;
  assert.equal(r.endpoint.toString(), 'http://collector:4318/v1/traces');
  assert.equal(r.sampleRate, 1);
  assert.equal(r.batchSize, 512);
  assert.equal(r.flushIntervalMs, 5000);
  assert.equal(r.timeoutMs, 5000);
  assert.deepEqual(r.headers, {});
  assert.equal(r.serviceName, 'realtime-event-bus');
  assert.equal(r.maxBuffered, 10000);
});

// --- exporter unit tests ---

test('exporter posts OTLP/HTTP JSON with the resourceSpans shape', async () => {
  const collector = startCollector();
  const endpoint = await collector.endpoint;
  const exporter = new OtlpSpanExporter(resolveOtlpOptions({ endpoint, serviceName: 'test-svc' })!);
  try {
    exporter.exportSpan(
      makeSpan({
        traceId: 'c'.repeat(32),
        spanId: 'd'.repeat(16),
        parentId: 'e'.repeat(16),
        name: 'bus.deliver',
        at: 1_700_000_000_123,
        durationMs: 7,
        attrs: { topic: 't.1', seq: 7, dwellMs: 2.5, ok: true },
      }),
    );
    await exporter.flush();
    assert.equal(collector.requests.length, 1);
    const span = decodeSingleSpan(collector.requests[0].body, 'test-svc');
    assert.equal(span.traceId, 'c'.repeat(32));
    assert.equal(span.spanId, 'd'.repeat(16));
    assert.equal(span.parentSpanId, 'e'.repeat(16));
    assert.equal(span.name, 'bus.deliver');
    assert.equal(span.kind, 1);
    assert.equal(span.startTimeUnixNano, '1700000000123000000');
    assert.equal(span.endTimeUnixNano, '1700000000130000000');
    assert.deepEqual(span.attributes, [
      { key: 'topic', value: { stringValue: 't.1' } },
      { key: 'seq', value: { intValue: '7' } },
      { key: 'dwellMs', value: { doubleValue: 2.5 } },
      { key: 'ok', value: { boolValue: true } },
    ]);
    assert.deepEqual(span.status, {});
    const stats = exporter.getStats();
    assert.equal(stats.exportedSpans, 1);
    assert.equal(stats.failedSpans, 0);
    assert.equal(stats.droppedSpans, 0);
    assert.equal(stats.bufferedSpans, 0);
  } finally {
    await exporter.shutdown();
    await collector.close();
  }
});

test('exporter omits parentSpanId on root spans', async () => {
  const collector = startCollector();
  const endpoint = await collector.endpoint;
  const exporter = new OtlpSpanExporter(resolveOtlpOptions({ endpoint })!);
  try {
    exporter.exportSpan(makeSpan());
    await exporter.flush();
    const span = decodeSingleSpan(collector.requests[0].body);
    assert.ok(!('parentSpanId' in span));
  } finally {
    await exporter.shutdown();
    await collector.close();
  }
});

test('exporter honors the export-side sampleRate', async () => {
  const collector = startCollector();
  const endpoint = await collector.endpoint;
  const exporter = new OtlpSpanExporter(resolveOtlpOptions({ endpoint, sampleRate: 0 })!);
  try {
    for (let i = 0; i < 10; i++) exporter.exportSpan(makeSpan());
    await exporter.flush();
    assert.equal(collector.requests.length, 0);
    assert.equal(exporter.getStats().exportedSpans, 0);
  } finally {
    await exporter.shutdown();
    await collector.close();
  }
});

test('exporter counts failed batches and never throws from flush', async () => {
  const collector = startCollector(500);
  const endpoint = await collector.endpoint;
  const exporter = new OtlpSpanExporter(resolveOtlpOptions({ endpoint })!);
  try {
    exporter.exportSpan(makeSpan());
    exporter.exportSpan(makeSpan());
    await exporter.flush(); // must not throw
    assert.equal(collector.requests.length, 1);
    const stats = exporter.getStats();
    assert.equal(stats.failedSpans, 2);
    assert.equal(stats.exportedSpans, 0);
    assert.equal(stats.bufferedSpans, 0);
    // Recovery: the collector comes back, later spans export fine.
    collector.setStatus(200);
    exporter.exportSpan(makeSpan());
    await exporter.flush();
    assert.equal(exporter.getStats().exportedSpans, 1);
  } finally {
    await exporter.shutdown();
    await collector.close();
  }
});

test('exporter drops the oldest spans past maxBuffered', async () => {
  const collector = startCollector();
  const endpoint = await collector.endpoint;
  const exporter = new OtlpSpanExporter(
    resolveOtlpOptions({ endpoint, maxBuffered: 2, flushIntervalMs: 60_000 })!,
  );
  try {
    for (let i = 0; i < 5; i++) {
      exporter.exportSpan(makeSpan({ spanId: String(i).padStart(16, '0') }));
    }
    assert.equal(exporter.getStats().droppedSpans, 3);
    await exporter.flush();
    assert.equal(collector.requests.length, 1);
    const parsed = JSON.parse(collector.requests[0].body) as {
      resourceSpans: Array<{ scopeSpans: Array<{ spans: Array<{ spanId: string }> }> }>;
    };
    const ids = parsed.resourceSpans[0].scopeSpans[0].spans.map((s) => s.spanId);
    // Drop-oldest: only the last two survive.
    assert.deepEqual(ids, ['0000000000000003', '0000000000000004']);
  } finally {
    await exporter.shutdown();
    await collector.close();
  }
});

test('exporter never throws from exportSpan', async () => {
  const collector = startCollector();
  const endpoint = await collector.endpoint;
  const exporter = new OtlpSpanExporter(resolveOtlpOptions({ endpoint })!);
  try {
    assert.doesNotThrow(() => exporter.exportSpan(undefined as never));
    assert.doesNotThrow(() => exporter.exportSpan(null as never));
    await exporter.flush();
  } finally {
    await exporter.shutdown();
    await collector.close();
  }
});

// --- bus integration ---

test('bus: otlp without trace throws RangeError at construction', async () => {
  const collector = startCollector();
  const endpoint = await collector.endpoint;
  try {
    assert.throws(() => new EventBus({ otlp: { endpoint } }), RangeError);
  } finally {
    await collector.close();
  }
});

test('bus: sampled spans are exported end-to-end', async () => {
  const collector = startCollector();
  const endpoint = await collector.endpoint;
  const bus = new EventBus({
    trace: { sampleRate: 1 },
    otlp: { endpoint, flushIntervalMs: 60_000 },
  });
  try {
    const seen: string[] = [];
    bus.subscribe('t.*', () => {
      seen.push('handler');
    });
    bus.publish('t.1', { n: 1 });
    await flush();
    await bus.flushOtlp();
    await waitFor(() => collector.requests.length > 0);
    const parsed = JSON.parse(collector.requests[0].body) as {
      resourceSpans: Array<{ scopeSpans: Array<{ spans: Array<{ name: string }> }> }>;
    };
    const names = parsed.resourceSpans[0].scopeSpans[0].spans.map((s) => s.name).sort();
    assert.ok(names.includes('bus.publish'));
    assert.ok(names.includes('bus.deliver'));
    assert.equal(seen.length, 1);
    const stats = bus.getStats().otlp!;
    assert.ok(stats.exportedSpans > 0);
    assert.equal(stats.failedSpans, 0);
    assert.equal(stats.endpoint, endpoint);
  } finally {
    await collector.close();
  }
});

test('bus: user onTraceSpan still fires alongside the exporter', async () => {
  const collector = startCollector();
  const endpoint = await collector.endpoint;
  const hooked: TraceSpan[] = [];
  const bus = new EventBus({
    trace: { sampleRate: 1, onTraceSpan: (s) => hooked.push(s) },
    otlp: { endpoint, flushIntervalMs: 60_000 },
  });
  try {
    bus.subscribe('t.*', () => {});
    bus.publish('t.1', {});
    await flush();
    await bus.flushOtlp();
    await waitFor(() => collector.requests.length > 0);
    assert.ok(hooked.length > 0);
  } finally {
    await collector.close();
  }
});

test('bus: getStats().otlp is absent when export is not configured', () => {
  const bus = new EventBus({ trace: true });
  assert.equal(bus.getStats().otlp, undefined);
});

test('bus: flushOtlp is a no-op without configuration', async () => {
  const bus = new EventBus();
  await bus.flushOtlp();
});
