/**
 * OTLP metrics export service-name rename (EB-413).
 *
 * `setOtlpMetricsServiceName` renames the export identity live — the
 * next export POST carries the new `service.name`, with no restart and
 * no loss of live bus state. A POST already in flight keeps the name
 * it started with; metrics are snapshots, not a buffered stream, so
 * unlike the span exporter (EB-412) no buffered content changes
 * identity at the rename point — the next snapshot simply carries the
 * new name. The flush timer is not rebuilt; the three numeric bounds,
 * the endpoint, and `headers` are untouched; the counters and the Sum
 * `startTimeUnixNano` are retained, so a rename never restarts the
 * cumulative series. Only a bus that already enabled `otlpMetrics`
 * can be renamed; a closed bus refuses; an illegal name fails closed
 * with the live name untouched; a rename identical to the current
 * name is a no-op with no audit. The live name is visible as
 * `getStats().otlpMetrics.serviceName`.
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

type MetricJson = {
  sum?: { dataPoints: Array<{ startTimeUnixNano: string }> };
  gauge?: { dataPoints: unknown[] };
  histogram?: { dataPoints: unknown[] };
};

function parsedMetrics(body: string): MetricJson[] {
  const parsed = JSON.parse(body) as {
    resourceMetrics: Array<{ scopeMetrics: Array<{ metrics: MetricJson[] }> }>;
  };
  return parsed.resourceMetrics[0].scopeMetrics[0].metrics;
}

/** Every Sum `startTimeUnixNano` in one metrics POST body. */
function sumStartTimes(body: string): Set<string> {
  const out = new Set<string>();
  for (const m of parsedMetrics(body)) {
    for (const dp of m.sum?.dataPoints ?? []) out.add(dp.startTimeUnixNano);
  }
  return out;
}

/** Total data points (sum + gauge + histogram) in one POST body. */
function dataPointCount(body: string): number {
  let n = 0;
  for (const m of parsedMetrics(body)) {
    n += (m.sum?.dataPoints.length ?? 0) + (m.gauge?.dataPoints.length ?? 0) +
      (m.histogram?.dataPoints.length ?? 0);
  }
  return n;
}

describe('OTLP metrics export service-name rename (EB-413)', () => {
  it('the next export carries the new service.name and the old name is gone', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      otlpMetrics: { endpoint, flushIntervalMs: 60_000, serviceName: 'checkout-bus' },
    });
    try {
      await bus.flushOtlpMetrics();
      assert.equal(collector.requests.length, 1);
      assert.equal(serviceNameOf(collector.requests[0].body), 'checkout-bus');

      bus.setOtlpMetricsServiceName('payments-bus');
      await bus.flushOtlpMetrics();
      assert.equal(collector.requests.length, 2, 'the next export must POST with the new name');
      assert.equal(serviceNameOf(collector.requests[1].body), 'payments-bus');
      assert.ok(!collector.requests[1].body.includes('checkout-bus'), 'the old name must not appear');
    } finally {
      await bus.close();
      await collector.close();
    }
  });

  it('a POST already in flight keeps the name it started with', async () => {
    const collector = startCollector({ delayMs: 500 });
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      otlpMetrics: { endpoint, flushIntervalMs: 60_000, serviceName: 'in-flight-old' },
    });
    try {
      // Start the POST, wait until the collector has the request,
      // then rename while the (slow) response is still outstanding:
      // the body was encoded at request build time, so the rename
      // cannot reach it.
      const flushing = bus.flushOtlpMetrics();
      await waitFor(() => collector.started() > 0);
      bus.setOtlpMetricsServiceName('in-flight-new');
      await flushing;
      assert.equal(collector.requests.length, 1);
      assert.equal(serviceNameOf(collector.requests[0].body), 'in-flight-old');
      // The next POST after the rename carries the new name.
      await bus.flushOtlpMetrics();
      assert.equal(collector.requests.length, 2);
      assert.equal(serviceNameOf(collector.requests[1].body), 'in-flight-new');
    } finally {
      await bus.close();
      await collector.close();
    }
  });

  it('setOtlpMetricsServiceName fails closed when otlpMetrics is not enabled', async () => {
    const plain = new EventBus();
    assert.throws(() => plain.setOtlpMetricsServiceName('svc'), RangeError);
    assert.equal(
      plain.getSubscriptionAudit({ action: 'otlp_metrics_service_name_revised' }).length,
      0,
    );

    // Span export alone is not metrics export: the metrics exporter
    // was never built.
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    try {
      const spanned = new EventBus({
        trace: { sampleRate: 1 },
        otlp: { endpoint, flushIntervalMs: 60_000 },
      });
      assert.throws(() => spanned.setOtlpMetricsServiceName('svc'), RangeError);
      assert.equal(
        spanned.getSubscriptionAudit({ action: 'otlp_metrics_service_name_revised' }).length,
        0,
      );
    } finally {
      await collector.close();
    }
  });

  it('setOtlpMetricsServiceName rejects illegal names and leaves the live name untouched', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      otlpMetrics: { endpoint, flushIntervalMs: 60_000, serviceName: 'live-name' },
    });
    try {
      for (const bad of ['', 42, null, undefined, {}, ['svc'], true]) {
        assert.throws(
          () => bus.setOtlpMetricsServiceName(bad as never),
          RangeError,
          `expected RangeError for ${JSON.stringify(bad)}`,
        );
      }
      assert.throws(
        () => bus.setOtlpMetricsServiceName('svc', ''),
        RangeError,
        'an illegal actor throws',
      );
      assert.equal(
        bus.getSubscriptionAudit({ action: 'otlp_metrics_service_name_revised' }).length,
        0,
      );
      assert.equal(bus.getStats().otlpMetrics?.serviceName, 'live-name');
      // The live name is untouched: the next POST still carries it.
      await bus.flushOtlpMetrics();
      assert.equal(collector.requests.length, 1);
      assert.equal(serviceNameOf(collector.requests[0].body), 'live-name');
      // And the bus still renames afterwards.
      bus.setOtlpMetricsServiceName('new-name');
      assert.equal(
        bus.getSubscriptionAudit({ action: 'otlp_metrics_service_name_revised' }).length,
        1,
      );
    } finally {
      await bus.close();
      await collector.close();
    }
  });

  it('renaming to the current name is a no-op with no audit', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      otlpMetrics: { endpoint, flushIntervalMs: 60_000, serviceName: 'same-name' },
    });
    try {
      bus.setOtlpMetricsServiceName('same-name');
      assert.equal(
        bus.getSubscriptionAudit({ action: 'otlp_metrics_service_name_revised' }).length,
        0,
      );
      assert.equal(bus.getStats().otlpMetrics?.serviceName, 'same-name');
      // The untouched name still governs the next export.
      await bus.flushOtlpMetrics();
      assert.equal(collector.requests.length, 1);
      assert.equal(serviceNameOf(collector.requests[0].body), 'same-name');
    } finally {
      await bus.close();
      await collector.close();
    }
  });

  it('a rename audits the exact previous and current names with the actor', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      otlpMetrics: { endpoint, flushIntervalMs: 60_000, serviceName: 'checkout-bus' },
    });
    try {
      bus.setOtlpMetricsServiceName('payments-bus', 'ops');
      const audit = bus.getSubscriptionAudit({ action: 'otlp_metrics_service_name_revised' });
      assert.equal(audit.length, 1);
      assert.equal(audit[0].actor, 'ops');
      assert.deepEqual(audit[0].detail, {
        serviceName: 'payments-bus',
        oldServiceName: 'checkout-bus',
      });
      // The default service name audits as the previous value.
      const plain = new EventBus({
        otlpMetrics: { endpoint, flushIntervalMs: 60_000 },
      });
      plain.setOtlpMetricsServiceName('renamed');
      const plainAudit = plain.getSubscriptionAudit({ action: 'otlp_metrics_service_name_revised' });
      assert.deepEqual(plainAudit[0].detail, {
        serviceName: 'renamed',
        oldServiceName: 'realtime-event-bus',
      });
    } finally {
      await bus.close();
      await collector.close();
    }
  });

  it('a rename leaves the three numeric bounds, endpoint, and headers untouched', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    try {
      // Exporter level: current() reports every live value.
      const exporter = new OtlpMetricsExporter(
        resolveOtlpMetricsOptions({
          endpoint,
          serviceName: 'svc',
          flushIntervalMs: 30_000,
          timeoutMs: 2500,
          headers: { Authorization: 'Bearer <redacted>' },
          perTopic: { maxTopics: 5 },
        })!,
        () => {
          throw new Error('must not collect');
        },
      );
      try {
        const before = exporter.current();
        exporter.setServiceName('svc-renamed');
        const after = exporter.current();
        assert.equal(after.endpoint.toString(), before.endpoint.toString());
        assert.equal(after.flushIntervalMs, before.flushIntervalMs);
        assert.equal(after.timeoutMs, before.timeoutMs);
        assert.deepEqual(after.perTopic, before.perTopic);
        assert.deepEqual(after.headers, { Authorization: 'Bearer <redacted>' });
        assert.equal(after.serviceName, 'svc-renamed');
        assert.equal(exporter.getStats().serviceName, 'svc-renamed');
      } finally {
        exporter.shutdown();
      }
      // Bus level: the renamed identity still rides the same
      // endpoint and headers.
      const bus = new EventBus({
        otlpMetrics: {
          endpoint,
          flushIntervalMs: 60_000,
          headers: { Authorization: 'Bearer <redacted>' },
          serviceName: 'svc-name',
        },
      });
      bus.setOtlpMetricsServiceName('svc-renamed');
      assert.equal(bus.getStats().otlpMetrics?.endpoint, endpoint);
      assert.equal(bus.getStats().otlpMetrics?.serviceName, 'svc-renamed');
      await bus.flushOtlpMetrics();
      assert.equal(collector.requests.length, 1);
      assert.equal(collector.requests[0].headers['authorization'], 'Bearer <redacted>');
      assert.equal(serviceNameOf(collector.requests[0].body), 'svc-renamed');
      await bus.close();
    } finally {
      await collector.close();
    }
  });

  it('getStats().otlpMetrics.serviceName follows the live name', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      otlpMetrics: { endpoint, flushIntervalMs: 60_000, serviceName: 'svc-a' },
    });
    try {
      assert.equal(bus.getStats().otlpMetrics?.serviceName, 'svc-a');
      bus.setOtlpMetricsServiceName('svc-b');
      assert.equal(bus.getStats().otlpMetrics?.serviceName, 'svc-b');
      bus.setOtlpMetricsServiceName('svc-c');
      assert.equal(bus.getStats().otlpMetrics?.serviceName, 'svc-c');
      // The default is reported when no name was configured.
      const plain = new EventBus({
        otlpMetrics: { endpoint, flushIntervalMs: 60_000 },
      });
      assert.equal(plain.getStats().otlpMetrics?.serviceName, 'realtime-event-bus');
    } finally {
      await bus.close();
      await collector.close();
    }
  });

  it('consecutive renames each govern the next POST', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      otlpMetrics: { endpoint, flushIntervalMs: 60_000, serviceName: 'first' },
    });
    try {
      bus.setOtlpMetricsServiceName('second');
      await bus.flushOtlpMetrics();
      assert.equal(serviceNameOf(collector.requests[0].body), 'second');
      bus.setOtlpMetricsServiceName('third');
      await bus.flushOtlpMetrics();
      assert.equal(collector.requests.length, 2);
      assert.equal(serviceNameOf(collector.requests[1].body), 'third');
      assert.equal(
        bus.getSubscriptionAudit({ action: 'otlp_metrics_service_name_revised' }).length,
        2,
      );
    } finally {
      await bus.close();
      await collector.close();
    }
  });

  it('a closed bus refuses the rename', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      otlpMetrics: { endpoint, flushIntervalMs: 60_000, serviceName: 'svc' },
    });
    try {
      await bus.close();
      assert.throws(() => bus.setOtlpMetricsServiceName('renamed'), RangeError);
      assert.equal(
        bus.getSubscriptionAudit({ action: 'otlp_metrics_service_name_revised' }).length,
        0,
      );
    } finally {
      await collector.close();
    }
  });

  it('a rename does not reset counters or the Sum start time', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      otlpMetrics: { endpoint, flushIntervalMs: 60_000, serviceName: 'metrics-svc' },
    });
    try {
      await bus.flushOtlpMetrics();
      assert.equal(collector.requests.length, 1);
      const before = bus.getStats().otlpMetrics!;
      const pointsBefore = dataPointCount(collector.requests[0].body);
      const startsBefore = sumStartTimes(collector.requests[0].body);
      assert.ok(startsBefore.size > 0, 'expected Sum data points with a start time');
      assert.equal(before.exportedMetrics, pointsBefore);

      bus.setOtlpMetricsServiceName('metrics-renamed');
      await bus.flushOtlpMetrics();
      assert.equal(collector.requests.length, 2);
      const after = bus.getStats().otlpMetrics!;
      const pointsAfter = dataPointCount(collector.requests[1].body);
      const startsAfter = sumStartTimes(collector.requests[1].body);
      // Counters accumulate across the rename — they are not reset.
      assert.equal(after.exportedMetrics, before.exportedMetrics + pointsAfter);
      assert.equal(after.failedExports, 0);
      assert.ok(after.lastExportAtMs >= before.lastExportAtMs);
      // The Sum start time is the exporter construction time, kept
      // across the rename: the collector-side rate computation is
      // uninterrupted.
      assert.deepEqual([...startsAfter].sort(), [...startsBefore].sort());
      assert.equal(serviceNameOf(collector.requests[1].body), 'metrics-renamed');
    } finally {
      await bus.close();
      await collector.close();
    }
  });

  it('the flush timer keeps running after a rename', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      otlpMetrics: { endpoint, flushIntervalMs: 100, serviceName: 'timer-svc' },
    });
    try {
      // The timer drives exports on its own cadence.
      await waitFor(() => collector.requests.length >= 1);
      const before = bus.getStats().otlpMetrics!;
      assert.equal(before.flushIntervalMs, 100);
      // A rename assigns only the name field — it must not stop or
      // rebuild the timer, so exports keep arriving at the live
      // cadence and carry the new name.
      bus.setOtlpMetricsServiceName('timer-renamed');
      assert.equal(bus.getStats().otlpMetrics?.flushIntervalMs, 100);
      await waitFor(() => collector.requests.length >= 2);
      const last = collector.requests[collector.requests.length - 1];
      assert.equal(serviceNameOf(last.body), 'timer-renamed');
    } finally {
      await bus.close();
      await collector.close();
    }
  });
});
