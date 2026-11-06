/**
 * OTLP metrics export headers rotation (EB-410).
 *
 * `setOtlpMetricsHeaders` rotates the collector request headers live
 * — the next export POST carries the new header set, with no restart
 * and no loss of live bus state. Full replacement, never a per-header
 * merge: a header absent from the new map stops being sent, and `{}`
 * clears the headers back to the construction-time default. Metrics
 * are snapshots, not a buffered stream, so there is nothing to
 * re-send or drop, and the counters (`exportedMetrics` /
 * `failedExports` / `lastExportAtMs`) and the Sum `startTimeUnixNano`
 * are retained — a rotation never restarts the cumulative series. A
 * POST already in flight keeps the headers it started with; the flush
 * timer is not rebuilt; the three numeric bounds, the endpoint, and
 * `serviceName` are untouched. Only a bus that already enabled
 * `otlpMetrics` can be rotated; a closed bus refuses; illegal headers
 * fail closed with the live headers untouched; a rotation identical
 * to the current header set (deep equality, key order-insensitive)
 * is a no-op with no audit. The audit records header names only —
 * values are never written to the audit record.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { createServer, type Server, type IncomingMessage, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import { EventBus } from '../src/bus.ts';
import { OtlpMetricsExporter, resolveOtlpMetricsOptions } from '../src/otlp-metrics.ts';

/**
 * Stub OTLP/HTTP collector on 127.0.0.1. Records every POST body and
 * its headers. The exporter uses `agent: false`, so connections close
 * after each response and `close()` never hangs on pooled sockets.
 */
function startCollector(): {
  requests: Array<{ body: string; headers: Record<string, string | string[] | undefined> }>;
  endpoint: Promise<string>;
  close: () => Promise<void>;
} {
  const requests: Array<{ body: string; headers: Record<string, string | string[] | undefined> }> =
    [];
  const server: Server = createServer((req: IncomingMessage, res: ServerResponse) => {
    let body = '';
    req.on('data', (chunk: Buffer) => {
      body += chunk.toString('utf8');
    });
    req.on('end', () => {
      requests.push({ body, headers: { ...req.headers } });
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

describe('OTLP metrics export headers rotation (EB-410)', () => {
  it('rotation sends the next export with the new Authorization', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      otlpMetrics: {
        endpoint,
        flushIntervalMs: 60_000,
        headers: { Authorization: 'Bearer old', 'x-tenant': 'acme' },
      },
    });
    try {
      await bus.flushOtlpMetrics();
      assert.equal(collector.requests.length, 1);
      assert.equal(collector.requests[0].headers['authorization'], 'Bearer old');
      assert.equal(collector.requests[0].headers['x-tenant'], 'acme');

      bus.setOtlpMetricsHeaders({ Authorization: 'Bearer new' });
      await bus.flushOtlpMetrics();
      assert.equal(collector.requests.length, 2, 'the next export must POST with the new headers');
      assert.equal(collector.requests[1].headers['authorization'], 'Bearer new');
    } finally {
      await bus.close();
      await collector.close();
    }
  });

  it('the old credential is gone from the next POST: no per-header merge', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      otlpMetrics: {
        endpoint,
        flushIntervalMs: 60_000,
        headers: { Authorization: 'Bearer old', 'x-tenant': 'acme', 'x-extra': 'e' },
      },
    });
    try {
      // A single-field rotation replaces the whole set: the old
      // Authorization and the untouched-by-the-call x-extra must not
      // linger beside the new tenant header.
      bus.setOtlpMetricsHeaders({ 'x-tenant': 'other' });
      await bus.flushOtlpMetrics();
      assert.equal(collector.requests.length, 1);
      assert.equal(collector.requests[0].headers['authorization'], undefined);
      assert.equal(collector.requests[0].headers['x-tenant'], 'other');
      assert.equal(collector.requests[0].headers['x-extra'], undefined);
    } finally {
      await bus.close();
      await collector.close();
    }
  });

  it('two consecutive rotations both take effect', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      otlpMetrics: {
        endpoint,
        flushIntervalMs: 60_000,
        headers: { Authorization: 'Bearer one' },
      },
    });
    try {
      bus.setOtlpMetricsHeaders({ Authorization: 'Bearer two' });
      await bus.flushOtlpMetrics();
      assert.equal(collector.requests[0].headers['authorization'], 'Bearer two');
      bus.setOtlpMetricsHeaders({ Authorization: 'Bearer three', 'x-tenant': 't3' });
      await bus.flushOtlpMetrics();
      assert.equal(collector.requests.length, 2);
      assert.equal(collector.requests[1].headers['authorization'], 'Bearer three');
      assert.equal(collector.requests[1].headers['x-tenant'], 't3');
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_metrics_headers_revised' }).length, 2);
    } finally {
      await bus.close();
      await collector.close();
    }
  });

  it('setOtlpMetricsHeaders fails closed when otlpMetrics is not enabled', async () => {
    const plain = new EventBus();
    assert.throws(
      () => plain.setOtlpMetricsHeaders({ Authorization: 'Bearer x' }),
      RangeError,
    );
    assert.equal(plain.getSubscriptionAudit({ action: 'otlp_metrics_headers_revised' }).length, 0);

    // Span export alone is not metrics export: the metrics exporter
    // was never built.
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const spanned = new EventBus({
      trace: { sampleRate: 1 },
      otlp: { endpoint, flushIntervalMs: 60_000 },
    });
    try {
      assert.throws(
        () => spanned.setOtlpMetricsHeaders({ Authorization: 'Bearer x' }),
        RangeError,
      );
      assert.equal(
        spanned.getSubscriptionAudit({ action: 'otlp_metrics_headers_revised' }).length,
        0,
      );
    } finally {
      await collector.close();
    }
  });

  it('setOtlpMetricsHeaders rejects illegal headers and leaves the live headers untouched', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      otlpMetrics: {
        endpoint,
        flushIntervalMs: 60_000,
        headers: { Authorization: 'Bearer live' },
      },
    });
    try {
      for (const bad of [
        'Bearer x',
        42,
        ['Authorization'],
        { Authorization: 42 },
        { Authorization: null },
        { 'x-tenant': {} },
      ]) {
        assert.throws(
          () => bus.setOtlpMetricsHeaders(bad as never),
          RangeError,
          `expected RangeError for ${JSON.stringify(bad)}`,
        );
      }
      assert.throws(
        () => bus.setOtlpMetricsHeaders({ Authorization: 'Bearer x' }, ''),
        RangeError,
        'an illegal actor throws',
      );
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_metrics_headers_revised' }).length, 0);
      // The live headers are untouched: the next export still carries
      // the original credential.
      await bus.flushOtlpMetrics();
      assert.equal(collector.requests.length, 1);
      assert.equal(collector.requests[0].headers['authorization'], 'Bearer live');
      // And the bus still rotates afterwards.
      bus.setOtlpMetricsHeaders({ Authorization: 'Bearer new' });
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_metrics_headers_revised' }).length, 1);
    } finally {
      await bus.close();
      await collector.close();
    }
  });

  it('rotating to the current headers is a no-op with no audit', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      otlpMetrics: {
        endpoint,
        flushIntervalMs: 60_000,
        headers: { Authorization: 'Bearer x', 'x-tenant': 'acme' },
      },
    });
    try {
      bus.setOtlpMetricsHeaders({ Authorization: 'Bearer x', 'x-tenant': 'acme' });
      // Key order does not matter: a reordered copy is the same set.
      bus.setOtlpMetricsHeaders({ 'x-tenant': 'acme', Authorization: 'Bearer x' });
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_metrics_headers_revised' }).length, 0);
      // The untouched headers still govern the next export.
      await bus.flushOtlpMetrics();
      assert.equal(collector.requests.length, 1);
      assert.equal(collector.requests[0].headers['authorization'], 'Bearer x');
      assert.equal(collector.requests[0].headers['x-tenant'], 'acme');
    } finally {
      await bus.close();
      await collector.close();
    }
  });

  it('a rotation audits header names only, never credential values', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      otlpMetrics: {
        endpoint,
        flushIntervalMs: 60_000,
        headers: { Authorization: 'Bearer old' },
      },
    });
    try {
      bus.setOtlpMetricsHeaders({ Authorization: 'Bearer new', 'x-tenant': 't-1' }, 'ops');
      const audit = bus.getSubscriptionAudit({ action: 'otlp_metrics_headers_revised' });
      assert.equal(audit.length, 1);
      assert.equal(audit[0].actor, 'ops');
      assert.deepEqual(audit[0].detail, {
        headers: ['Authorization', 'x-tenant'],
        oldHeaders: ['Authorization'],
      });
      const detailJson = JSON.stringify(audit[0].detail);
      assert.ok(!detailJson.includes('Bearer new'), 'the new credential must not reach the audit');
      assert.ok(!detailJson.includes('Bearer old'), 'the old credential must not reach the audit');
      assert.ok(!detailJson.includes('t-1'), 'a header value must not reach the audit');
    } finally {
      await bus.close();
      await collector.close();
    }
  });

  it('an empty object clears the headers', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      otlpMetrics: {
        endpoint,
        flushIntervalMs: 60_000,
        headers: { Authorization: 'Bearer old' },
      },
    });
    try {
      await bus.flushOtlpMetrics();
      assert.equal(collector.requests[0].headers['authorization'], 'Bearer old');

      bus.setOtlpMetricsHeaders({});
      const audit = bus.getSubscriptionAudit({ action: 'otlp_metrics_headers_revised' });
      assert.deepEqual(audit[0].detail, { headers: [], oldHeaders: ['Authorization'] });
      await bus.flushOtlpMetrics();
      assert.equal(collector.requests.length, 2);
      assert.equal(collector.requests[1].headers['authorization'], undefined);
    } finally {
      await bus.close();
      await collector.close();
    }
  });

  it('a rotation leaves the numeric bounds, endpoint, and serviceName untouched', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    // The collector close owns the outer finally: an early throw in
    // either half must not leak the listening server (a leaked
    // listener keeps the test worker alive).
    try {
      // Exporter level: current() reports every live value. The collect
      // callback is a plain bus's stats, as in the EB-407 test — the
      // exporter is shut down before its 60s timer can fire.
      const plain = new EventBus();
      const exporter = new OtlpMetricsExporter(
        resolveOtlpMetricsOptions({
          endpoint,
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
        const rotated = { Authorization: 'Bearer new' };
        exporter.setHeaders(rotated);
        // Mutating the caller's object afterwards must not move the
        // live headers: setHeaders stores a defensive copy.
        rotated.Authorization = 'Bearer tampered';
        const after = exporter.current();
        assert.equal(after.endpoint.toString(), before.endpoint.toString());
        assert.equal(after.flushIntervalMs, before.flushIntervalMs);
        assert.equal(after.timeoutMs, before.timeoutMs);
        assert.deepEqual(after.perTopic, before.perTopic);
        assert.deepEqual(after.headers, { Authorization: 'Bearer new' });
        assert.equal(after.serviceName, before.serviceName);
      } finally {
        await exporter.shutdown();
      }
      // Bus level: the new headers still ride the same identity, and
      // the cumulative counters are retained.
      const bus = new EventBus({
        otlpMetrics: {
          endpoint,
          flushIntervalMs: 60_000,
          headers: { Authorization: 'Bearer old' },
          serviceName: 'svc-name',
        },
      });
      try {
        await bus.flushOtlpMetrics();
        const exportedBefore = bus.getStats().otlpMetrics?.exportedMetrics ?? 0;
        assert.ok(exportedBefore > 0, 'expected a first export before the rotation');
        bus.setOtlpMetricsHeaders({ Authorization: 'Bearer new' });
        assert.equal(bus.getStats().otlpMetrics?.endpoint, endpoint);
        await bus.flushOtlpMetrics();
        assert.equal(collector.requests.length, 2);
        assert.equal(collector.requests[1].headers['authorization'], 'Bearer new');
        assert.equal(serviceNameOf(collector.requests[1].body), 'svc-name');
        assert.ok(
          (bus.getStats().otlpMetrics?.exportedMetrics ?? 0) > exportedBefore,
          'the cumulative series must keep counting past the rotation',
        );
      } finally {
        await bus.close();
      }
    } finally {
      await collector.close();
    }
  });

  it('a closed bus refuses the rotation', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      otlpMetrics: {
        endpoint,
        flushIntervalMs: 60_000,
        headers: { Authorization: 'Bearer x' },
      },
    });
    try {
      await bus.close();
      assert.throws(() => bus.setOtlpMetricsHeaders({ Authorization: 'Bearer y' }), RangeError);
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_metrics_headers_revised' }).length, 0);
    } finally {
      await collector.close();
    }
  });

  it('mutating the caller object after the call cannot move the live headers', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      otlpMetrics: {
        endpoint,
        flushIntervalMs: 60_000,
        headers: { Authorization: 'Bearer old' },
      },
    });
    try {
      const rotated = { Authorization: 'Bearer new' };
      bus.setOtlpMetricsHeaders(rotated);
      rotated.Authorization = 'Bearer tampered';
      await bus.flushOtlpMetrics();
      assert.equal(collector.requests.length, 1);
      assert.equal(collector.requests[0].headers['authorization'], 'Bearer new');
    } finally {
      await bus.close();
      await collector.close();
    }
  });
});
