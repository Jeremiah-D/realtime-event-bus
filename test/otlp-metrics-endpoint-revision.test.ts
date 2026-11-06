/**
 * OTLP metrics export endpoint repoint (EB-407).
 *
 * `setOtlpMetricsEndpoint` repoints the collector destination live —
 * the next export POST goes to the new collector, with no restart and
 * no loss of live bus state. A POST already in flight keeps the
 * destination it started with; the counters (`exportedMetrics` /
 * `failedExports` / `lastExportAtMs`) and the Sum `startTimeUnixNano`
 * are retained, so the collector-side rate computation is
 * uninterrupted; the flush timer is not rebuilt; the three numeric
 * bounds, `headers`, and `serviceName` are untouched. Only a bus that
 * already enabled `otlpMetrics` can be repointed; a closed bus
 * refuses; illegal endpoints fail closed with the live endpoint
 * untouched; a repoint identical to the current endpoint (string
 * equality) is a no-op with no audit.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { createServer, type Server, type IncomingMessage, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import { EventBus } from '../src/bus.ts';
import { OtlpMetricsExporter, resolveOtlpMetricsOptions } from '../src/otlp-metrics.ts';

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

/**
 * Stub OTLP/HTTP collector on 127.0.0.1. Records every POST body and
 * its headers; `started` counts requests as they arrive (before the
 * delayed response), so a test can tell an in-flight POST from a
 * completed one. The exporter uses `agent: false`, so connections
 * close after each response and `close()` never hangs on pooled
 * sockets.
 */
function startCollector(options: { status?: number; delayMs?: number } = {}): {
  requests: Array<{ body: string; headers: Record<string, string | string[] | undefined> }>;
  started: () => number;
  endpoint: Promise<string>;
  close: () => Promise<void>;
} {
  const { status = 200, delayMs = 0 } = options;
  const requests: Array<{ body: string; headers: Record<string, string | string[] | undefined> }> = [];
  let started = 0;
  const server: Server = createServer((req: IncomingMessage, res: ServerResponse) => {
    started += 1;
    let body = '';
    req.on('data', (chunk: Buffer) => {
      body += chunk.toString('utf8');
    });
    req.on('end', () => {
      setTimeout(() => {
        requests.push({ body, headers: { ...req.headers } });
        res.writeHead(status, { 'content-type': 'application/json' });
        res.end('{}');
      }, delayMs);
    });
  });
  const endpoint = new Promise<string>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const addr = server.address() as AddressInfo;
      resolve(`http://127.0.0.1:${addr.port}/v1/metrics`);
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

/** The `service.name` resource attribute of one metrics POST body. */
function serviceNameOf(body: string): string | undefined {
  const parsed = JSON.parse(body) as {
    resourceMetrics: Array<{
      resource: { attributes: Array<{ key: string; value: { stringValue?: string } }> };
    }>;
  };
  return parsed.resourceMetrics[0].resource.attributes.find((a) => a.key === 'service.name')
    ?.value.stringValue;
}

describe('OTLP metrics export endpoint repoint (EB-407)', () => {
  it('repoint sends the next export to the new collector', async () => {
    const oldCollector = startCollector();
    const newCollector = startCollector();
    const oldEndpoint = await oldCollector.endpoint;
    const newEndpoint = await newCollector.endpoint;
    const bus = new EventBus({
      otlpMetrics: { endpoint: oldEndpoint, flushIntervalMs: 60_000 },
    });
    try {
      await bus.flushOtlpMetrics();
      assert.equal(oldCollector.requests.length, 1);
      assert.equal(newCollector.requests.length, 0);

      bus.setOtlpMetricsEndpoint(newEndpoint);
      assert.equal(bus.getStats().otlpMetrics?.endpoint, newEndpoint);
      await bus.flushOtlpMetrics();
      assert.equal(oldCollector.requests.length, 1, 'no further POST may go to the old collector');
      assert.equal(newCollector.requests.length, 1, 'the next export must POST to the new collector');
    } finally {
      await bus.close();
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
      otlpMetrics: { endpoint: oldEndpoint, flushIntervalMs: 60_000 },
    });
    try {
      // Start the POST, wait until the old collector has the request,
      // then repoint while the (slow) response is still outstanding.
      const flushing = bus.flushOtlpMetrics();
      await waitFor(() => oldCollector.started() > 0);
      bus.setOtlpMetricsEndpoint(newEndpoint);
      await flushing;
      assert.equal(oldCollector.requests.length, 1, 'the in-flight POST must complete to the old collector');
      assert.equal(newCollector.requests.length, 0, 'the in-flight POST must not leak to the new collector');
      // The next export goes to the new destination.
      await bus.flushOtlpMetrics();
      assert.equal(newCollector.requests.length, 1);
    } finally {
      await bus.close();
      await oldCollector.close();
      await newCollector.close();
    }
  });

  it('setOtlpMetricsEndpoint fails closed when otlpMetrics is not enabled', async () => {
    const plain = new EventBus();
    assert.throws(() => plain.setOtlpMetricsEndpoint('http://127.0.0.1:9/v1/metrics'), RangeError);
    assert.equal(plain.getSubscriptionAudit({ action: 'otlp_metrics_endpoint_revised' }).length, 0);

    // Span export alone is not metrics export: the metrics exporter
    // was never built.
    const traced = new EventBus({
      trace: { sampleRate: 1 },
      otlp: { endpoint: 'http://127.0.0.1:9/v1/traces', flushIntervalMs: 60_000 },
    });
    assert.throws(() => traced.setOtlpMetricsEndpoint('http://127.0.0.1:9/v1/metrics'), RangeError);
    assert.equal(traced.getSubscriptionAudit({ action: 'otlp_metrics_endpoint_revised' }).length, 0);
    await plain.close();
    await traced.close();
  });

  it('setOtlpMetricsEndpoint rejects illegal endpoints and leaves the live endpoint untouched', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      otlpMetrics: { endpoint, flushIntervalMs: 60_000 },
    });
    try {
      for (const bad of ['', 'not a url', 'ftp://example.com/v1/metrics', 'http://', 42]) {
        assert.throws(
          () => bus.setOtlpMetricsEndpoint(bad as never),
          RangeError,
          `expected RangeError for ${JSON.stringify(bad)}`,
        );
      }
      assert.throws(() => bus.setOtlpMetricsEndpoint(endpoint, ''), RangeError, 'an illegal actor throws');
      assert.equal(bus.getStats().otlpMetrics?.endpoint, endpoint);
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_metrics_endpoint_revised' }).length, 0);
      // And the bus still repoints afterwards.
      const other = startCollector();
      try {
        const otherEndpoint = await other.endpoint;
        bus.setOtlpMetricsEndpoint(otherEndpoint);
        assert.equal(bus.getStats().otlpMetrics?.endpoint, otherEndpoint);
      } finally {
        await other.close();
      }
    } finally {
      await bus.close();
      await collector.close();
    }
  });

  it('repointing to the current endpoint is a no-op with no audit', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      otlpMetrics: { endpoint, flushIntervalMs: 60_000 },
    });
    try {
      bus.setOtlpMetricsEndpoint(endpoint);
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_metrics_endpoint_revised' }).length, 0);
      // The untouched endpoint still governs the next export.
      await bus.flushOtlpMetrics();
      assert.equal(collector.requests.length, 1);
    } finally {
      await bus.close();
      await collector.close();
    }
  });

  it('setOtlpMetricsEndpoint audits the repoint with old and new endpoint', async () => {
    const oldCollector = startCollector();
    const newCollector = startCollector();
    const oldEndpoint = await oldCollector.endpoint;
    const newEndpoint = await newCollector.endpoint;
    const bus = new EventBus({
      otlpMetrics: { endpoint: oldEndpoint, flushIntervalMs: 60_000 },
    });
    try {
      bus.setOtlpMetricsEndpoint(newEndpoint, 'ops');
      const audit = bus.getSubscriptionAudit({ action: 'otlp_metrics_endpoint_revised' });
      assert.equal(audit.length, 1);
      assert.equal(audit[0].actor, 'ops');
      assert.deepEqual(audit[0].detail, { endpoint: newEndpoint, oldEndpoint });
    } finally {
      await bus.close();
      await oldCollector.close();
      await newCollector.close();
    }
  });

  it('getStats().otlpMetrics.endpoint follows the live destination', async () => {
    const oldCollector = startCollector();
    const newCollector = startCollector();
    const oldEndpoint = await oldCollector.endpoint;
    const newEndpoint = await newCollector.endpoint;
    const bus = new EventBus({
      otlpMetrics: { endpoint: oldEndpoint, flushIntervalMs: 60_000 },
    });
    try {
      assert.equal(bus.getStats().otlpMetrics?.endpoint, oldEndpoint);
      bus.setOtlpMetricsEndpoint(newEndpoint);
      assert.equal(bus.getStats().otlpMetrics?.endpoint, newEndpoint);
      bus.setOtlpMetricsEndpoint(oldEndpoint);
      assert.equal(bus.getStats().otlpMetrics?.endpoint, oldEndpoint);
    } finally {
      await bus.close();
      await oldCollector.close();
      await newCollector.close();
    }
  });

  it('a repoint leaves flushIntervalMs, timeoutMs, perTopic, headers, and serviceName untouched', async () => {
    const oldCollector = startCollector();
    const newCollector = startCollector();
    const oldEndpoint = await oldCollector.endpoint;
    const newEndpoint = await newCollector.endpoint;
    // Exporter level: current() reports every live value.
    const plain = new EventBus();
    const exporter = new OtlpMetricsExporter(
      resolveOtlpMetricsOptions({
        endpoint: oldEndpoint,
        serviceName: 'svc',
        flushIntervalMs: 60_000,
        timeoutMs: 2500,
        headers: { Authorization: 'Bearer x' },
        perTopic: { maxTopics: 5 },
      })!,
      () => plain.getStats(),
    );
    try {
      const before = exporter.current();
      exporter.setEndpoint(new URL(newEndpoint));
      const after = exporter.current();
      assert.equal(after.endpoint.toString(), newEndpoint);
      assert.equal(after.flushIntervalMs, before.flushIntervalMs);
      assert.equal(after.timeoutMs, before.timeoutMs);
      assert.deepEqual(after.perTopic, before.perTopic);
      assert.deepEqual(after.headers, before.headers);
      assert.equal(after.serviceName, before.serviceName);
    } finally {
      exporter.shutdown();
      await plain.close();
    }
    // Bus level: the new collector still receives the same identity
    // and credentials, and the live tuning reports unchanged.
    const bus = new EventBus({
      otlpMetrics: {
        endpoint: oldEndpoint,
        flushIntervalMs: 60_000,
        timeoutMs: 2500,
        headers: { Authorization: 'Bearer k-123' },
        serviceName: 'svc-name',
        perTopic: { maxTopics: 5 },
      },
    });
    try {
      const before = bus.getStats().otlpMetrics;
      bus.setOtlpMetricsEndpoint(newEndpoint);
      const after = bus.getStats().otlpMetrics;
      assert.equal(after?.flushIntervalMs, before?.flushIntervalMs);
      assert.equal(after?.timeoutMs, before?.timeoutMs);
      assert.deepEqual(after?.perTopic, before?.perTopic);
      await bus.flushOtlpMetrics();
      assert.equal(newCollector.requests.length, 1);
      assert.equal(newCollector.requests[0].headers['authorization'], 'Bearer k-123');
      assert.equal(serviceNameOf(newCollector.requests[0].body), 'svc-name');
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
      otlpMetrics: { endpoint: oldEndpoint, flushIntervalMs: 500 },
    });
    try {
      // 400ms in, the 500ms timer has not fired yet.
      await sleep(400);
      assert.equal(oldCollector.requests.length, 0);
      // Repoint now: if the timer were rebuilt, the next export would
      // land ~500ms later; with the timer untouched it fires ~100ms
      // later, at the original phase — and to the new collector.
      bus.setOtlpMetricsEndpoint(newEndpoint);
      await waitFor(() => newCollector.requests.length > 0, 400);
      assert.equal(oldCollector.requests.length, 0, 'the scheduled export must go to the new collector');
    } finally {
      await bus.close();
      await oldCollector.close();
      await newCollector.close();
    }
  });

  it('a failed POST after a repoint drops the snapshot and counts failedExports', async () => {
    const oldCollector = startCollector();
    const failingCollector = startCollector({ status: 500 });
    const oldEndpoint = await oldCollector.endpoint;
    const failingEndpoint = await failingCollector.endpoint;
    const bus = new EventBus({
      otlpMetrics: { endpoint: oldEndpoint, flushIntervalMs: 60_000 },
    });
    try {
      bus.setOtlpMetricsEndpoint(failingEndpoint);
      await bus.flushOtlpMetrics();
      assert.equal(bus.getStats().otlpMetrics?.failedExports, 1);
      assert.equal(bus.getStats().otlpMetrics?.exportedMetrics, 0);
      assert.equal(failingCollector.requests.length, 1);
      assert.equal(oldCollector.requests.length, 0);
    } finally {
      await bus.close();
      await oldCollector.close();
      await failingCollector.close();
    }
  });

  it('counters are retained across a repoint and consecutive repoints both take effect', async () => {
    const firstCollector = startCollector();
    const secondCollector = startCollector();
    const firstEndpoint = await firstCollector.endpoint;
    const secondEndpoint = await secondCollector.endpoint;
    const bus = new EventBus({
      otlpMetrics: { endpoint: firstEndpoint, flushIntervalMs: 60_000 },
    });
    try {
      await bus.flushOtlpMetrics();
      const first = bus.getStats().otlpMetrics;
      assert.ok((first?.exportedMetrics ?? 0) > 0, 'expected data points exported before the repoint');
      assert.equal(first?.failedExports, 0);
      assert.ok((first?.lastExportAtMs ?? 0) > 0);

      bus.setOtlpMetricsEndpoint(secondEndpoint);
      await bus.flushOtlpMetrics();
      const second = bus.getStats().otlpMetrics;
      assert.ok(
        (second?.exportedMetrics ?? 0) > (first?.exportedMetrics ?? 0),
        'exportedMetrics must keep accumulating across the repoint, not restart',
      );
      assert.equal(second?.failedExports, 0);
      assert.ok((second?.lastExportAtMs ?? 0) >= (first?.lastExportAtMs ?? 0));

      bus.setOtlpMetricsEndpoint(firstEndpoint);
      await bus.flushOtlpMetrics();
      const third = bus.getStats().otlpMetrics;
      assert.ok((third?.exportedMetrics ?? 0) > (second?.exportedMetrics ?? 0));
      assert.equal(third?.failedExports, 0);
      assert.equal(firstCollector.requests.length, 2, 'the first collector must receive both its exports');
      assert.equal(secondCollector.requests.length, 1);
    } finally {
      await bus.close();
      await firstCollector.close();
      await secondCollector.close();
    }
  });

  it('a closed bus refuses the repoint', async () => {
    const collector = startCollector();
    const other = startCollector();
    const endpoint = await collector.endpoint;
    const otherEndpoint = await other.endpoint;
    const bus = new EventBus({
      otlpMetrics: { endpoint, flushIntervalMs: 60_000 },
    });
    try {
      await bus.close();
      assert.throws(() => bus.setOtlpMetricsEndpoint(otherEndpoint), RangeError);
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_metrics_endpoint_revised' }).length, 0);
    } finally {
      await collector.close();
      await other.close();
    }
  });
});
