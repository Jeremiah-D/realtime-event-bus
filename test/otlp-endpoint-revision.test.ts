/**
 * OTLP span export endpoint repoint (EB-406).
 *
 * `setOtlpEndpoint` repoints the collector destination live — the
 * next export POST goes to the new collector, with no restart and no
 * loss of live bus state. A POST already in flight keeps the
 * destination it started with; buffered spans stay buffered, are
 * never re-sent and never dropped by the repoint; the flush timer is
 * not rebuilt; the five numeric bounds, `headers`, and `serviceName`
 * are untouched. Only a bus that already enabled `otlp` can be
 * repointed; a closed bus refuses; illegal endpoints fail closed with
 * the live endpoint untouched; a repoint identical to the current
 * endpoint (string equality) is a no-op with no audit.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { createServer, type Server, type IncomingMessage, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import { EventBus } from '../src/bus.ts';
import { OtlpSpanExporter, resolveOtlpOptions } from '../src/otlp.ts';

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

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** Counts the spans in one OTLP/HTTP JSON request body. */
function spanCount(body: string): number {
  const parsed = JSON.parse(body) as {
    resourceSpans: Array<{ scopeSpans: Array<{ spans: unknown[] }> }>;
  };
  return parsed.resourceSpans[0].scopeSpans[0].spans.length;
}

/**
 * Stub OTLP/HTTP collector on 127.0.0.1. Records every POST body and
 * its `x-collector-key` header; `started` counts requests as they
 * arrive (before the delayed response), so a test can tell an
 * in-flight POST from a completed one. The exporter uses
 * `agent: false`, so connections close after each response and
 * `close()` never hangs on pooled sockets.
 */
function startCollector(options: { status?: number; delayMs?: number } = {}): {
  requests: Array<{ body: string; collectorKey: string | string[] | undefined }>;
  started: () => number;
  endpoint: Promise<string>;
  close: () => Promise<void>;
} {
  const { status = 200, delayMs = 0 } = options;
  const requests: Array<{ body: string; collectorKey: string | string[] | undefined }> = [];
  let started = 0;
  const server: Server = createServer((req: IncomingMessage, res: ServerResponse) => {
    started += 1;
    let body = '';
    req.on('data', (chunk: Buffer) => {
      body += chunk.toString('utf8');
    });
    req.on('end', () => {
      const collectorKey = req.headers['x-collector-key'];
      setTimeout(() => {
        requests.push({ body, collectorKey });
        res.writeHead(status, { 'content-type': 'application/json' });
        res.end('{}');
      }, delayMs);
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
    started: () => started,
    endpoint,
    close: () =>
      new Promise<void>((resolve, reject) => {
        server.close((err) => (err == null ? resolve() : reject(err)));
      }),
  };
}

describe('OTLP span export endpoint repoint (EB-406)', () => {
  it('repoint sends the next flush to the new collector', async () => {
    const oldCollector = startCollector();
    const newCollector = startCollector();
    const oldEndpoint = await oldCollector.endpoint;
    const newEndpoint = await newCollector.endpoint;
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: { endpoint: oldEndpoint, flushIntervalMs: 60_000 },
    });
    try {
      bus.subscribe('t', () => {});
      bus.publish('t', 'a');
      await flush();
      await bus.flushOtlp();
      assert.equal(oldCollector.requests.length, 1);
      assert.equal(newCollector.requests.length, 0);

      bus.setOtlpEndpoint(newEndpoint);
      assert.equal(bus.getStats().otlp?.endpoint, newEndpoint);
      bus.publish('t', 'b');
      await flush();
      await bus.flushOtlp();
      assert.equal(oldCollector.requests.length, 1, 'no further POST may go to the old collector');
      assert.equal(newCollector.requests.length, 1, 'the next flush must POST to the new collector');
    } finally {
      await oldCollector.close();
      await newCollector.close();
    }
  });

  it('an in-flight POST keeps the old destination', async () => {
    const oldCollector = startCollector({ delayMs: 500 });
    const newCollector = startCollector();
    const oldEndpoint = await oldCollector.endpoint;
    const newEndpoint = await newCollector.endpoint;
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: { endpoint: oldEndpoint, flushIntervalMs: 60_000 },
    });
    try {
      bus.subscribe('t', () => {});
      bus.publish('t', 'a');
      await flush();
      // Start the POST, wait until the old collector has the request,
      // then repoint while the (slow) response is still outstanding.
      const flushing = bus.flushOtlp();
      await waitFor(() => oldCollector.started() > 0);
      bus.setOtlpEndpoint(newEndpoint);
      await flushing;
      assert.equal(oldCollector.requests.length, 1, 'the in-flight POST must complete to the old collector');
      assert.equal(newCollector.requests.length, 0, 'the in-flight POST must not leak to the new collector');
      // The next flush goes to the new destination.
      bus.publish('t', 'b');
      await flush();
      await bus.flushOtlp();
      assert.equal(newCollector.requests.length, 1);
    } finally {
      await oldCollector.close();
      await newCollector.close();
    }
  });

  it('buffered spans survive a repoint: not lost, not re-sent', async () => {
    const oldCollector = startCollector();
    const newCollector = startCollector();
    const oldEndpoint = await oldCollector.endpoint;
    const newEndpoint = await newCollector.endpoint;
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: { endpoint: oldEndpoint, flushIntervalMs: 60_000 },
    });
    try {
      bus.subscribe('t', () => {});
      bus.publish('t', 'a');
      await flush();
      const buffered = bus.getStats().otlp?.bufferedSpans ?? 0;
      assert.ok(buffered > 0, 'expected spans buffered before the repoint');
      // The repoint itself keeps the buffer.
      bus.setOtlpEndpoint(newEndpoint);
      assert.equal(bus.getStats().otlp?.bufferedSpans, buffered);
      await bus.flushOtlp();
      assert.equal(bus.getStats().otlp?.bufferedSpans, 0);
      assert.equal(oldCollector.requests.length, 0);
      assert.equal(newCollector.requests.length, 1);
      const received = newCollector.requests.reduce((n, r) => n + spanCount(r.body), 0);
      assert.equal(received, buffered, 'every buffered span must arrive exactly once, at the new collector');
      assert.equal(bus.getStats().otlp?.exportedSpans, buffered);
    } finally {
      await oldCollector.close();
      await newCollector.close();
    }
  });

  it('setOtlpEndpoint fails closed when otlp is not enabled', async () => {
    const plain = new EventBus();
    assert.throws(() => plain.setOtlpEndpoint('http://127.0.0.1:9/v1/traces'), RangeError);
    assert.equal(plain.getSubscriptionAudit({ action: 'otlp_endpoint_revised' }).length, 0);

    // Trace alone is not export: the exporter was never built.
    const traced = new EventBus({ trace: { sampleRate: 1 } });
    assert.throws(() => traced.setOtlpEndpoint('http://127.0.0.1:9/v1/traces'), RangeError);
    assert.equal(traced.getSubscriptionAudit({ action: 'otlp_endpoint_revised' }).length, 0);
  });

  it('setOtlpEndpoint rejects illegal endpoints and leaves the live endpoint untouched', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: { endpoint, flushIntervalMs: 60_000 },
    });
    try {
      for (const bad of ['', 'not a url', 'ftp://example.com/v1/traces', 'http://', 42]) {
        assert.throws(
          () => bus.setOtlpEndpoint(bad as never),
          RangeError,
          `expected RangeError for ${JSON.stringify(bad)}`,
        );
      }
      assert.throws(() => bus.setOtlpEndpoint(endpoint, ''), RangeError, 'an illegal actor throws');
      assert.equal(bus.getStats().otlp?.endpoint, endpoint);
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_endpoint_revised' }).length, 0);
      // And the bus still repoints afterwards.
      const other = startCollector();
      try {
        const otherEndpoint = await other.endpoint;
        bus.setOtlpEndpoint(otherEndpoint);
        assert.equal(bus.getStats().otlp?.endpoint, otherEndpoint);
      } finally {
        await other.close();
      }
    } finally {
      await collector.close();
    }
  });

  it('repointing to the current endpoint is a no-op with no audit', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: { endpoint, flushIntervalMs: 60_000 },
    });
    try {
      bus.setOtlpEndpoint(endpoint);
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_endpoint_revised' }).length, 0);
      // The untouched endpoint still governs the next flush.
      bus.subscribe('t', () => {});
      bus.publish('t', 'a');
      await flush();
      await bus.flushOtlp();
      assert.equal(collector.requests.length, 1);
    } finally {
      await collector.close();
    }
  });

  it('setOtlpEndpoint audits the repoint with old and new endpoint', async () => {
    const oldCollector = startCollector();
    const newCollector = startCollector();
    const oldEndpoint = await oldCollector.endpoint;
    const newEndpoint = await newCollector.endpoint;
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: { endpoint: oldEndpoint, flushIntervalMs: 60_000 },
    });
    try {
      bus.setOtlpEndpoint(newEndpoint, 'ops');
      const audit = bus.getSubscriptionAudit({ action: 'otlp_endpoint_revised' });
      assert.equal(audit.length, 1);
      assert.equal(audit[0].actor, 'ops');
      assert.deepEqual(audit[0].detail, { endpoint: newEndpoint, oldEndpoint });
    } finally {
      await oldCollector.close();
      await newCollector.close();
    }
  });

  it('getStats().otlp.endpoint follows the live destination', async () => {
    const oldCollector = startCollector();
    const newCollector = startCollector();
    const oldEndpoint = await oldCollector.endpoint;
    const newEndpoint = await newCollector.endpoint;
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: { endpoint: oldEndpoint, flushIntervalMs: 60_000 },
    });
    try {
      assert.equal(bus.getStats().otlp?.endpoint, oldEndpoint);
      bus.setOtlpEndpoint(newEndpoint);
      assert.equal(bus.getStats().otlp?.endpoint, newEndpoint);
      bus.setOtlpEndpoint(oldEndpoint);
      assert.equal(bus.getStats().otlp?.endpoint, oldEndpoint);
    } finally {
      await oldCollector.close();
      await newCollector.close();
    }
  });

  it('a repoint leaves the five numeric bounds, headers, and serviceName untouched', async () => {
    const oldCollector = startCollector();
    const newCollector = startCollector();
    const oldEndpoint = await oldCollector.endpoint;
    const newEndpoint = await newCollector.endpoint;
    // Exporter level: current() reports every live value.
    const exporter = new OtlpSpanExporter(
      resolveOtlpOptions({
        endpoint: oldEndpoint,
        sampleRate: 0.5,
        batchSize: 10,
        flushIntervalMs: 60_000,
        timeoutMs: 2500,
        headers: { Authorization: 'Bearer x' },
        serviceName: 'svc',
        maxBuffered: 100,
      })!,
    );
    try {
      const before = exporter.current();
      exporter.setEndpoint(new URL(newEndpoint));
      const after = exporter.current();
      assert.equal(after.endpoint.toString(), newEndpoint);
      assert.equal(after.sampleRate, before.sampleRate);
      assert.equal(after.batchSize, before.batchSize);
      assert.equal(after.flushIntervalMs, before.flushIntervalMs);
      assert.equal(after.timeoutMs, before.timeoutMs);
      assert.equal(after.maxBuffered, before.maxBuffered);
      assert.deepEqual(after.headers, before.headers);
      assert.equal(after.serviceName, before.serviceName);
    } finally {
      await exporter.shutdown();
    }
    // Bus level: the new collector still receives the same identity
    // and credentials.
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: {
        endpoint: oldEndpoint,
        flushIntervalMs: 60_000,
        headers: { 'x-collector-key': 'k-123' },
        serviceName: 'svc-name',
      },
    });
    try {
      bus.subscribe('t', () => {});
      bus.setOtlpEndpoint(newEndpoint);
      bus.publish('t', 'a');
      await flush();
      await bus.flushOtlp();
      assert.equal(newCollector.requests.length, 1);
      assert.equal(newCollector.requests[0].collectorKey, 'k-123');
      const body = JSON.parse(newCollector.requests[0].body) as {
        resourceSpans: Array<{
          resource: { attributes: Array<{ key: string; value: { stringValue?: string } }> };
        }>;
      };
      const serviceName = body.resourceSpans[0].resource.attributes.find(
        (a) => a.key === 'service.name',
      )?.value.stringValue;
      assert.equal(serviceName, 'svc-name');
    } finally {
      await bus.close();
      await oldCollector.close();
      await newCollector.close();
    }
  });

  it('a repoint does not restart the flush timer', async () => {
    const oldCollector = startCollector();
    const newCollector = startCollector();
    const oldEndpoint = await oldCollector.endpoint;
    const newEndpoint = await newCollector.endpoint;
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: { endpoint: oldEndpoint, flushIntervalMs: 500 },
    });
    try {
      bus.subscribe('t', () => {});
      bus.publish('t', 'a');
      await flush();
      // 400ms in, the 500ms timer has not fired yet.
      await sleep(400);
      assert.equal(oldCollector.requests.length, 0);
      // Repoint now: if the timer were rebuilt, the next flush would
      // land ~500ms later; with the timer untouched it fires ~100ms
      // later, at the original phase — and to the new collector.
      bus.setOtlpEndpoint(newEndpoint);
      await waitFor(() => newCollector.requests.length > 0, 400);
      assert.equal(oldCollector.requests.length, 0, 'the scheduled flush must go to the new collector');
    } finally {
      await oldCollector.close();
      await newCollector.close();
    }
  });

  it('a failed POST after a repoint still drops the batch and counts failedSpans', async () => {
    const oldCollector = startCollector();
    const failingCollector = startCollector({ status: 500 });
    const oldEndpoint = await oldCollector.endpoint;
    const failingEndpoint = await failingCollector.endpoint;
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: { endpoint: oldEndpoint, flushIntervalMs: 60_000 },
    });
    try {
      bus.subscribe('t', () => {});
      bus.setOtlpEndpoint(failingEndpoint);
      bus.publish('t', 'a');
      await flush();
      const buffered = bus.getStats().otlp?.bufferedSpans ?? 0;
      assert.ok(buffered > 0, 'expected spans buffered before the failing flush');
      await bus.flushOtlp();
      assert.equal(bus.getStats().otlp?.bufferedSpans, 0);
      assert.equal(bus.getStats().otlp?.failedSpans, buffered);
      assert.equal(bus.getStats().otlp?.exportedSpans, 0);
      assert.equal(failingCollector.requests.length, 1);
      assert.equal(oldCollector.requests.length, 0);
    } finally {
      await oldCollector.close();
      await failingCollector.close();
    }
  });

  it('a closed bus refuses the repoint', async () => {
    const collector = startCollector();
    const other = startCollector();
    const endpoint = await collector.endpoint;
    const otherEndpoint = await other.endpoint;
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: { endpoint, flushIntervalMs: 60_000 },
    });
    try {
      await bus.close();
      assert.throws(() => bus.setOtlpEndpoint(otherEndpoint), RangeError);
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_endpoint_revised' }).length, 0);
    } finally {
      await collector.close();
      await other.close();
    }
  });
});
