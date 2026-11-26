/**
 * Bus-level OTLP metrics export live enable (EB-556).
 *
 * A bus that never enabled `otlpMetrics` gains an exporter live on
 * the first `setOtlpMetrics` call with an `endpoint` — built exactly
 * like the construction row (`resolveOtlpMetricsOptions` then `new
 * OtlpMetricsExporter`), so the rest fall back to the construction
 * defaults, the unref flush timer arms itself, and the first snapshot
 * POSTs on the next tick with the Sum start time taken at the enable.
 * A failed enable leaves the bus unconfigured, so a later valid call
 * still enables it; `null` on a never-enabled bus is a silent no-op
 * with no audit; a closed bus refuses the enable (EB-154 fail-fast);
 * and the enable is live-only — a restart restores the unconfigured
 * state. The enable is audited as `otlp_metrics_revised` with every
 * `old*` value `null`. Once enabled, revision follows EB-374 and the
 * `endpoint` / `headers` / `serviceName` setters (EB-407 / EB-410 /
 * EB-413) act on the live exporter.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { createServer, type Server, type IncomingMessage, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import { EventBus } from '../src/bus.ts';

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
  headers: Record<string, string | string[] | undefined>;
  body: string;
}

/**
 * Stub OTLP/HTTP collector on 127.0.0.1. Records every POST body and
 * headers. The exporter uses `agent: false`, so connections close
 * after each response and `close()` never hangs on pooled sockets.
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
      requests.push({ url: req.url ?? '/', headers: { ...req.headers }, body });
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end('{}');
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
    endpoint,
    close: () =>
      new Promise<void>((resolve, reject) => {
        server.close((err) => (err == null ? resolve() : reject(err)));
      }),
  };
}

type MetricsBody = {
  resourceMetrics: Array<{
    resource: { attributes: Array<{ key: string; value: { stringValue: string } }> };
    scopeMetrics: Array<{
      scope: { name: string };
      metrics: Array<Record<string, unknown>>;
    }>;
  }>;
};

function parseBody(request: CapturedRequest): MetricsBody {
  return JSON.parse(request.body) as MetricsBody;
}

function serviceNameOf(body: MetricsBody): string | undefined {
  return body.resourceMetrics[0].resource.attributes.find((a) => a.key === 'service.name')?.value
    .stringValue;
}

describe('OTLP metrics export live enable (EB-556)', () => {
  it('enables a never-enabled bus with explicit values and audits every old value as null', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus();
    try {
      assert.equal(bus.getStats().otlpMetrics, undefined);
      bus.setOtlpMetrics({
        endpoint,
        serviceName: 'metrics-bus',
        flushIntervalMs: 1234,
        timeoutMs: 2345,
        headers: { authorization: 'Bearer x' },
        perTopic: { maxTopics: 3 },
      });
      const live = bus.getStats().otlpMetrics;
      assert.ok(live !== undefined);
      assert.equal(live.endpoint, endpoint);
      assert.equal(live.serviceName, 'metrics-bus');
      assert.equal(live.flushIntervalMs, 1234);
      assert.equal(live.timeoutMs, 2345);
      assert.deepEqual(live.perTopic, { enabled: true, maxTopics: 3 });
      assert.equal(live.exportedMetrics, 0);
      assert.equal(live.failedExports, 0);
      assert.equal(live.lastExportAtMs, 0);
      const audit = bus.getSubscriptionAudit({ action: 'otlp_metrics_revised' });
      assert.equal(audit.length, 1);
      assert.equal(audit[0].actor, 'system');
      assert.deepEqual(audit[0].detail, {
        flushIntervalMs: 1234,
        oldFlushIntervalMs: null,
        timeoutMs: 2345,
        oldTimeoutMs: null,
        perTopicEnabled: true,
        oldPerTopicEnabled: null,
        perTopicMaxTopics: 3,
        oldPerTopicMaxTopics: null,
      });
    } finally {
      await bus.close();
      await collector.close();
    }
  });

  it('enables with an endpoint alone and takes the construction defaults', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus();
    try {
      bus.setOtlpMetrics({ endpoint });
      const live = bus.getStats().otlpMetrics;
      assert.ok(live !== undefined);
      assert.equal(live.endpoint, endpoint);
      assert.equal(live.serviceName, 'realtime-event-bus');
      assert.equal(live.flushIntervalMs, 5000);
      assert.equal(live.timeoutMs, 5000);
      assert.deepEqual(live.perTopic, { enabled: true, maxTopics: 20 });
      const audit = bus.getSubscriptionAudit({ action: 'otlp_metrics_revised' });
      assert.equal(audit.length, 1);
      assert.deepEqual(audit[0].detail, {
        flushIntervalMs: 5000,
        oldFlushIntervalMs: null,
        timeoutMs: 5000,
        oldTimeoutMs: null,
        perTopicEnabled: true,
        oldPerTopicEnabled: null,
        perTopicMaxTopics: 20,
        oldPerTopicMaxTopics: null,
      });
    } finally {
      await bus.close();
      await collector.close();
    }
  });

  it('posts a snapshot on the next tick after the enable', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus();
    try {
      bus.setOtlpMetrics({ endpoint, flushIntervalMs: 50 });
      await waitFor(() => collector.requests.length > 0);
      const body = parseBody(collector.requests[0]);
      assert.equal(serviceNameOf(body), 'realtime-event-bus');
      assert.ok(body.resourceMetrics[0].scopeMetrics[0].metrics.length > 0);
      // The collector records the request when the body arrives, but
      // `exportedMetrics` moves only once the export POST completes
      // (the response is received), so poll the counter instead of
      // asserting it at request-arrival time: under load the waitFor
      // above can return in the gap between the two (CI c341901).
      await waitFor(() => (bus.getStats().otlpMetrics?.exportedMetrics ?? 0) > 0);
    } finally {
      await bus.close();
      await collector.close();
    }
  });

  it('{} and endpoint-less patches refuse and leave the bus unconfigured, then a valid call still enables', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus();
    try {
      const bad: Array<unknown> = [{}, { timeoutMs: 1000 }, { flushIntervalMs: 100 }, { perTopic: false }];
      for (const patch of bad) {
        assert.throws(() => bus.setOtlpMetrics(patch as never), RangeError);
      }
      assert.equal(bus.getStats().otlpMetrics, undefined);
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_metrics_revised' }).length, 0);
      bus.setOtlpMetrics({ endpoint });
      assert.ok(bus.getStats().otlpMetrics !== undefined);
    } finally {
      await bus.close();
      await collector.close();
    }
  });

  it('illegal endpoints refuse and leave the bus unconfigured', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus();
    try {
      const bad: Array<unknown> = ['', 'not-a-url', 'ftp://127.0.0.1:9/v1/metrics', 123];
      for (const patch of bad) {
        assert.throws(
          () => bus.setOtlpMetrics({ endpoint: patch } as never),
          RangeError,
          `expected RangeError for ${JSON.stringify(patch)}`,
        );
      }
      assert.equal(bus.getStats().otlpMetrics, undefined);
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_metrics_revised' }).length, 0);
      bus.setOtlpMetrics({ endpoint });
      assert.ok(bus.getStats().otlpMetrics !== undefined);
    } finally {
      await bus.close();
      await collector.close();
    }
  });

  it('illegal tuning values refuse and leave the bus unconfigured', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus();
    try {
      const bad: Array<unknown> = [
        { flushIntervalMs: 0 },
        { flushIntervalMs: -1 },
        { flushIntervalMs: Number.NaN },
        { flushIntervalMs: Infinity },
        { timeoutMs: 0 },
        { timeoutMs: -1 },
        { timeoutMs: Number.NaN },
        { perTopic: { maxTopics: 0 } },
        { perTopic: { maxTopics: -1 } },
        { perTopic: { maxTopics: 1.5 } },
        { perTopic: 'all' },
        { perTopic: 42 },
        { perTopic: [] },
        { serviceName: '' },
        { headers: 'x' },
        { headers: { authorization: 1 } },
      ];
      for (const patch of bad) {
        assert.throws(
          () => bus.setOtlpMetrics({ endpoint, ...(patch as Record<string, unknown>) }),
          RangeError,
          `expected RangeError for ${JSON.stringify(patch)}`,
        );
      }
      assert.equal(bus.getStats().otlpMetrics, undefined);
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_metrics_revised' }).length, 0);
      bus.setOtlpMetrics({ endpoint });
      assert.ok(bus.getStats().otlpMetrics !== undefined);
    } finally {
      await bus.close();
      await collector.close();
    }
  });

  it('null on a never-enabled bus is a silent no-op with no audit', async () => {
    const bus = new EventBus();
    try {
      bus.setOtlpMetrics(null as never);
      assert.equal(bus.getStats().otlpMetrics, undefined);
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_metrics_revised' }).length, 0);
      // And the bus still enables afterwards.
      const collector = startCollector();
      const endpoint = await collector.endpoint;
      try {
        bus.setOtlpMetrics({ endpoint });
        assert.ok(bus.getStats().otlpMetrics !== undefined);
      } finally {
        await collector.close();
      }
    } finally {
      await bus.close();
    }
  });

  it('non-object revisions refuse and leave the bus unconfigured', async () => {
    const bus = new EventBus();
    try {
      for (const patch of ['nope', 42, []] as unknown[]) {
        assert.throws(() => bus.setOtlpMetrics(patch as never), RangeError);
      }
      assert.equal(bus.getStats().otlpMetrics, undefined);
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_metrics_revised' }).length, 0);
    } finally {
      await bus.close();
    }
  });

  it('post-enable revision merges live values and a retuned interval governs the next tick', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus();
    try {
      bus.setOtlpMetrics({ endpoint, flushIntervalMs: 60_000, timeoutMs: 5000 });
      bus.setOtlpMetrics({ flushIntervalMs: 50 });
      assert.equal(bus.getStats().otlpMetrics?.flushIntervalMs, 50);
      assert.equal(bus.getStats().otlpMetrics?.timeoutMs, 5000);
      await waitFor(() => collector.requests.length > 0);
      const audit = bus.getSubscriptionAudit({ action: 'otlp_metrics_revised' });
      assert.equal(audit.length, 2);
      assert.deepEqual(audit[1].detail, {
        flushIntervalMs: 50,
        oldFlushIntervalMs: 60_000,
        timeoutMs: 5000,
        oldTimeoutMs: 5000,
        perTopicEnabled: true,
        oldPerTopicEnabled: true,
        perTopicMaxTopics: 20,
        oldPerTopicMaxTopics: 20,
      });
    } finally {
      await bus.close();
      await collector.close();
    }
  });

  it('post-enable same-value and empty revisions are no-ops with no audit', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus();
    try {
      bus.setOtlpMetrics({ endpoint, flushIntervalMs: 60_000 });
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_metrics_revised' }).length, 1);
      bus.setOtlpMetrics({ flushIntervalMs: 60_000 });
      bus.setOtlpMetrics({ timeoutMs: 5000 });
      bus.setOtlpMetrics({ perTopic: true });
      bus.setOtlpMetrics({});
      bus.setOtlpMetrics(null as never);
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_metrics_revised' }).length, 1);
      // And the bus still revises afterwards.
      bus.setOtlpMetrics({ timeoutMs: 1000 });
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_metrics_revised' }).length, 2);
    } finally {
      await bus.close();
      await collector.close();
    }
  });

  it('post-enable endpoint, headers, and serviceName setters act on the live exporter', async () => {
    const collectorA = startCollector();
    const collectorB = startCollector();
    const endpointA = await collectorA.endpoint;
    const endpointB = await collectorB.endpoint;
    const bus = new EventBus();
    try {
      bus.setOtlpMetrics({ endpoint: endpointA, flushIntervalMs: 60_000 });
      bus.setOtlpMetricsServiceName('renamed');
      bus.setOtlpMetricsHeaders({ authorization: 'Bearer test-token' });
      bus.setOtlpMetricsEndpoint(endpointB);
      const live = bus.getStats().otlpMetrics;
      assert.ok(live !== undefined);
      assert.equal(live.endpoint, endpointB);
      assert.equal(live.serviceName, 'renamed');
      await bus.flushOtlpMetrics();
      await waitFor(() => collectorB.requests.length > 0);
      assert.equal(collectorA.requests.length, 0);
      const request = collectorB.requests[0];
      assert.equal(request.headers['authorization'], 'Bearer test-token');
      assert.equal(serviceNameOf(parseBody(request)), 'renamed');
    } finally {
      await bus.close();
      await collectorA.close();
      await collectorB.close();
    }
  });

  it('a closed bus refuses the enable', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus();
    try {
      await bus.close();
      assert.throws(() => bus.setOtlpMetrics({ endpoint }), /bus is closed/);
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_metrics_revised' }).length, 0);
    } finally {
      await collector.close();
    }
  });

  it('a restart restores the unconfigured state', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus();
    try {
      bus.setOtlpMetrics({ endpoint });
      assert.ok(bus.getStats().otlpMetrics !== undefined);
      await bus.close();
      const restarted = new EventBus();
      try {
        assert.equal(restarted.getStats().otlpMetrics, undefined);
        assert.equal(
          restarted.getSubscriptionAudit({ action: 'otlp_metrics_revised' }).length,
          0,
        );
      } finally {
        await restarted.close();
      }
    } finally {
      await collector.close();
    }
  });
});
