/**
 * Bus-level OTLP metrics export revision (EB-374).
 *
 * `setOtlpMetrics` retunes the metrics exporter live — the export
 * cadence, the per-request timeout, and the per-topic series cap —
 * without restarting the bus. Only a bus that enabled `otlpMetrics`
 * can be revised; illegal values fail closed with the live tuning
 * and the audit log untouched; an identical (or empty) revision is
 * a no-op with no audit. The counters and the Sum start time are
 * retained across a retune, an in-flight flush is untouched, a
 * revised timeout governs the next POST, a revised per-topic cap
 * governs the next export, and a revised flush interval rebuilds
 * the flush timer at the new cadence from the next tick.
 * `endpoint`, `headers`, and `serviceName` are not revisable here.
 * A closed bus refuses the revision (EB-154 fail-fast).
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { createServer, type Server, type IncomingMessage, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import { EventBus } from '../src/bus.ts';
import {
  OtlpMetricsExporter,
  resolveOtlpMetricsOptions,
} from '../src/otlp-metrics.ts';

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

interface CapturedRequest {
  url: string;
  headers: Record<string, string | string[] | undefined>;
  body: string;
}

/**
 * Stub OTLP/HTTP collector on 127.0.0.1. Records every POST body and
 * headers; the response status is configurable per test (default
 * 200) and the response may be delayed (`delayMs`) so a revised
 * timeout can be observed. The exporter uses `agent: false`, so
 * connections close after each response and `close()` never hangs on
 * pooled sockets.
 */
function startCollector(
  statusCode = 200,
  delayMs = 0,
): {
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
      const respond = () => {
        res.writeHead(statusCode, { 'content-type': 'application/json' });
        res.end('{}');
      };
      if (delayMs > 0) setTimeout(respond, delayMs);
      else respond();
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

function metricsOf(body: MetricsBody): Array<Record<string, unknown>> {
  return body.resourceMetrics[0].scopeMetrics[0].metrics;
}

function findMetric(
  body: MetricsBody,
  name: string,
): Record<string, unknown> | undefined {
  return metricsOf(body).find((m) => m.name === name);
}

function dataPoints(metric: Record<string, unknown> | undefined): Array<Record<string, any>> {
  if (metric === undefined) return [];
  const section = (metric.sum ?? metric.gauge ?? metric.histogram) as
    | { dataPoints: Array<Record<string, any>> }
    | undefined;
  return section?.dataPoints ?? [];
}

function attrValue(point: Record<string, any>, key: string): string | undefined {
  const found = (point.attributes as Array<{ key: string; value: { stringValue: string } }>).find(
    (a) => a.key === key,
  );
  return found?.value.stringValue;
}

describe('OTLP metrics export revision (EB-374)', () => {
  it('setOtlpMetrics fails closed when otlpMetrics is not enabled', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    try {
      const plain = new EventBus();
      assert.throws(() => plain.setOtlpMetrics({ timeoutMs: 1000 }), RangeError);
      assert.equal(plain.getSubscriptionAudit({ action: 'otlp_metrics_revised' }).length, 0);

      // Span export alone is not metrics export: the metrics
      // exporter was never built.
      const traced = new EventBus({
        trace: { sampleRate: 1 },
        otlp: { endpoint, flushIntervalMs: 60_000 },
      });
      assert.throws(() => traced.setOtlpMetrics({ timeoutMs: 1000 }), RangeError);
      assert.equal(traced.getSubscriptionAudit({ action: 'otlp_metrics_revised' }).length, 0);
    } finally {
      await collector.close();
    }
  });

  it('setOtlpMetrics rejects illegal values and leaves the live tuning untouched', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      otlpMetrics: { endpoint, flushIntervalMs: 60_000, timeoutMs: 5000 },
    });
    try {
      const bad: Array<unknown> = [
        { flushIntervalMs: 0 },
        { flushIntervalMs: -1 },
        { flushIntervalMs: Number.NaN },
        { flushIntervalMs: Infinity },
        { timeoutMs: 0 },
        { timeoutMs: -1 },
        { timeoutMs: Number.NaN },
        { timeoutMs: Infinity },
        { perTopic: { maxTopics: 0 } },
        { perTopic: { maxTopics: -1 } },
        { perTopic: { maxTopics: 1.5 } },
        { perTopic: 'all' },
        { perTopic: 42 },
        { perTopic: [] },
        'nope',
        42,
      ];
      for (const revision of bad) {
        assert.throws(
          () => bus.setOtlpMetrics(revision as never),
          RangeError,
          `expected RangeError for ${JSON.stringify(revision)}`,
        );
      }
      // The failed calls changed nothing: the live tuning still
      // reports the construction values.
      const live = bus.getStats().otlpMetrics;
      assert.ok(live !== undefined);
      assert.equal(live.flushIntervalMs, 60_000);
      assert.equal(live.timeoutMs, 5000);
      assert.deepEqual(live.perTopic, { enabled: true, maxTopics: 20 });
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_metrics_revised' }).length, 0);
      await bus.close();
    } finally {
      await collector.close();
    }
  });

  it('setOtlpMetrics with identical values or an empty revision is a no-op with no audit', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      otlpMetrics: { endpoint, flushIntervalMs: 60_000, timeoutMs: 5000 },
    });
    try {
      bus.setOtlpMetrics({ flushIntervalMs: 60_000 });
      bus.setOtlpMetrics({ timeoutMs: 5000 });
      bus.setOtlpMetrics({ perTopic: true });
      bus.setOtlpMetrics({ perTopic: { maxTopics: 20 } });
      bus.setOtlpMetrics({});
      bus.setOtlpMetrics(null as never);
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_metrics_revised' }).length, 0);
      // And the bus still revises afterwards.
      bus.setOtlpMetrics({ timeoutMs: 2500 });
      assert.equal(bus.getStats().otlpMetrics?.timeoutMs, 2500);
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_metrics_revised' }).length, 1);
      await bus.close();
    } finally {
      await collector.close();
    }
  });

  it('flushIntervalMs revision rebuilds the timer: exports drain at the new cadence, none lost', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      otlpMetrics: { endpoint, flushIntervalMs: 60_000 },
    });
    try {
      bus.subscribe('t', () => {});
      bus.publish('t', 'a');
      await flush();
      // Nothing exports on its own at the 60s cadence.
      await sleep(150);
      assert.equal(collector.requests.length, 0);
      // Tighten the interval: the retune keeps the counters, the new
      // timer exports the snapshot within a few ticks.
      bus.setOtlpMetrics({ flushIntervalMs: 50 });
      await waitFor(() => (bus.getStats().otlpMetrics?.exportedMetrics ?? 0) > 0);
      assert.ok(collector.requests.length > 0);
      const body = parseBody(collector.requests[0]);
      const published = findMetric(body, 'eventbus_published_messages_total');
      assert.equal(dataPoints(published)[0].asInt, '1');
      const audit = bus.getSubscriptionAudit({ action: 'otlp_metrics_revised' });
      assert.equal(audit.length, 1);
      assert.deepEqual(audit[0].detail, {
        flushIntervalMs: 50,
        oldFlushIntervalMs: 60_000,
        timeoutMs: 5000,
        oldTimeoutMs: 5000,
        perTopicEnabled: true,
        oldPerTopicEnabled: true,
        perTopicMaxTopics: 20,
        oldPerTopicMaxTopics: 20,
      });
      await bus.close();
    } finally {
      await collector.close();
    }
  });

  it('timeoutMs revision governs the next export POST', async () => {
    // The collector answers after 300ms: generous under the 5000ms
    // construction timeout, past the revised 50ms timeout.
    const collector = startCollector(200, 300);
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      otlpMetrics: { endpoint, flushIntervalMs: 60_000, timeoutMs: 5000 },
    });
    try {
      bus.subscribe('t', () => {});
      bus.publish('t', 'a');
      await flush();
      await bus.flushOtlpMetrics();
      assert.equal(collector.requests.length, 1);
      assert.equal(bus.getStats().otlpMetrics?.failedExports, 0);
      const exportedBefore = bus.getStats().otlpMetrics?.exportedMetrics ?? 0;
      assert.ok(exportedBefore > 0);
      // The next POST times out at the live 50ms.
      bus.setOtlpMetrics({ timeoutMs: 50 });
      await bus.flushOtlpMetrics();
      assert.equal(bus.getStats().otlpMetrics?.failedExports, 1);
      // The timed-out snapshot was dropped, not counted as exported.
      assert.equal(bus.getStats().otlpMetrics?.exportedMetrics, exportedBefore);
      await bus.close();
    } finally {
      await collector.close();
    }
  });

  it('perTopic:false revision drops per-topic series from the next export', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      otlpMetrics: { endpoint, flushIntervalMs: 60_000 },
    });
    try {
      bus.subscribe('t', () => {});
      for (let i = 0; i < 5; i += 1) bus.publish('t', i);
      await flush();
      await bus.flushOtlpMetrics();
      const before = parseBody(collector.requests[0]);
      const beforePoints = dataPoints(findMetric(before, 'eventbus_published_messages_total'));
      assert.ok(beforePoints.some((p) => attrValue(p, 'topic') === 't'));
      // Disable per-topic series: the next export carries the
      // bus-level point only.
      bus.setOtlpMetrics({ perTopic: false });
      assert.deepEqual(bus.getStats().otlpMetrics?.perTopic, { enabled: false, maxTopics: 20 });
      await bus.flushOtlpMetrics();
      assert.equal(collector.requests.length, 2);
      const after = parseBody(collector.requests[1]);
      for (const metric of metricsOf(after)) {
        for (const point of dataPoints(metric)) {
          assert.equal(attrValue(point, 'topic'), undefined);
        }
      }
      await bus.close();
    } finally {
      await collector.close();
    }
  });

  it('maxTopics revision truncates per-topic series from the next export', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      otlpMetrics: { endpoint, flushIntervalMs: 60_000, perTopic: { maxTopics: 3 } },
    });
    try {
      bus.subscribe('hot', () => {});
      bus.subscribe('warm', () => {});
      bus.subscribe('cold', () => {});
      for (let i = 0; i < 5; i += 1) bus.publish('hot', i);
      for (let i = 0; i < 3; i += 1) bus.publish('warm', i);
      bus.publish('cold', 1);
      await flush();
      await bus.flushOtlpMetrics();
      const before = parseBody(collector.requests[0]);
      const beforeTopics = dataPoints(
        findMetric(before, 'eventbus_published_messages_total'),
      ).filter((p) => attrValue(p, 'topic') !== undefined);
      assert.equal(beforeTopics.length, 3);
      // Tighten the cap: the next export covers only the hottest.
      bus.setOtlpMetrics({ perTopic: { maxTopics: 1 } });
      await bus.flushOtlpMetrics();
      const after = parseBody(collector.requests[1]);
      const afterTopics = dataPoints(
        findMetric(after, 'eventbus_published_messages_total'),
      ).filter((p) => attrValue(p, 'topic') !== undefined);
      assert.equal(afterTopics.length, 1);
      assert.equal(attrValue(afterTopics[0], 'topic'), 'hot');
      assert.equal(afterTopics[0].asInt, '5');
      await bus.close();
    } finally {
      await collector.close();
    }
  });

  it('revision does not reset counters or the Sum start time', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      otlpMetrics: { endpoint, flushIntervalMs: 60_000 },
    });
    try {
      bus.subscribe('t', () => {});
      bus.publish('t', 'a');
      await flush();
      await bus.flushOtlpMetrics();
      const first = parseBody(collector.requests[0]);
      const firstStart = dataPoints(
        findMetric(first, 'eventbus_published_messages_total'),
      )[0].startTimeUnixNano as string;
      const exportedBefore = bus.getStats().otlpMetrics?.exportedMetrics ?? 0;
      assert.ok(exportedBefore > 0);
      // A retune moves the tuning, not the series: the counters keep
      // counting and the Sum start stays the construction time.
      bus.setOtlpMetrics({ timeoutMs: 2500 });
      await bus.flushOtlpMetrics();
      const second = parseBody(collector.requests[1]);
      const secondStart = dataPoints(
        findMetric(second, 'eventbus_published_messages_total'),
      )[0].startTimeUnixNano as string;
      assert.equal(secondStart, firstStart);
      assert.ok((bus.getStats().otlpMetrics?.exportedMetrics ?? 0) > exportedBefore);
      assert.equal(bus.getStats().otlpMetrics?.failedExports, 0);
      await bus.close();
    } finally {
      await collector.close();
    }
  });

  it('audit carries old and new values, including the perTopic disable canonicalization', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      otlpMetrics: { endpoint, flushIntervalMs: 60_000, timeoutMs: 5000, perTopic: { maxTopics: 5 } },
    });
    try {
      bus.setOtlpMetrics({ flushIntervalMs: 1000, timeoutMs: 2500, perTopic: { maxTopics: 3 } });
      // Disabling resolves through the construction-time canonical
      // form, so the live cap reports the resolver default (20)
      // while disabled.
      bus.setOtlpMetrics({ perTopic: false });
      const audit = bus.getSubscriptionAudit({ action: 'otlp_metrics_revised' });
      assert.equal(audit.length, 2);
      assert.deepEqual(audit[0].detail, {
        flushIntervalMs: 1000,
        oldFlushIntervalMs: 60_000,
        timeoutMs: 2500,
        oldTimeoutMs: 5000,
        perTopicEnabled: true,
        oldPerTopicEnabled: true,
        perTopicMaxTopics: 3,
        oldPerTopicMaxTopics: 5,
      });
      assert.deepEqual(audit[1].detail, {
        flushIntervalMs: 1000,
        oldFlushIntervalMs: 1000,
        timeoutMs: 2500,
        oldTimeoutMs: 2500,
        perTopicEnabled: false,
        oldPerTopicEnabled: true,
        perTopicMaxTopics: 20,
        oldPerTopicMaxTopics: 3,
      });
      // Re-enabling with `true` restores per-topic series at the
      // live (canonical) cap.
      bus.setOtlpMetrics({ perTopic: true });
      assert.deepEqual(bus.getStats().otlpMetrics?.perTopic, { enabled: true, maxTopics: 20 });
      await bus.close();
    } finally {
      await collector.close();
    }
  });

  it('an in-flight flush is not disturbed by a revision', async () => {
    // The collector answers after 300ms: the flush started before
    // the revision is still in flight when the retune lands.
    const collector = startCollector(200, 300);
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      otlpMetrics: { endpoint, flushIntervalMs: 60_000, timeoutMs: 5000 },
    });
    try {
      bus.subscribe('t', () => {});
      bus.publish('t', 'a');
      await flush();
      const inFlight = bus.flushOtlpMetrics();
      bus.setOtlpMetrics({ timeoutMs: 2500, flushIntervalMs: 60_000 });
      // The in-flight POST was created under the old timeout and
      // completes normally; the new values are live immediately.
      await inFlight;
      assert.ok((bus.getStats().otlpMetrics?.exportedMetrics ?? 0) > 0);
      assert.equal(bus.getStats().otlpMetrics?.timeoutMs, 2500);
      assert.equal(bus.getStats().otlpMetrics?.failedExports, 0);
      await bus.close();
    } finally {
      await collector.close();
    }
  });

  it('revision after close is refused (EB-154 fail-fast)', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      otlpMetrics: { endpoint, flushIntervalMs: 60_000 },
    });
    try {
      bus.subscribe('t', () => {});
      bus.publish('t', 'a');
      await flush();
      await bus.close();
      // close() already flushed once and stopped the exporter.
      assert.equal(collector.requests.length, 1);
      assert.throws(() => bus.setOtlpMetrics({ timeoutMs: 1000 }), RangeError);
      assert.throws(() => bus.setOtlpMetrics({ flushIntervalMs: 1000 }), RangeError);
      assert.throws(() => bus.setOtlpMetrics({ perTopic: false }), RangeError);
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_metrics_revised' }).length, 0);
      assert.equal(collector.requests.length, 1);
    } finally {
      await collector.close();
    }
  });

  it('exporter retune moves the live values; current() reports them, and a retune after shutdown does not re-arm the timer', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const plain = new EventBus();
    const exporter = new OtlpMetricsExporter(
      resolveOtlpMetricsOptions({ endpoint, flushIntervalMs: 60_000 })!,
      () => plain.getStats(),
    );
    try {
      assert.equal(exporter.current().flushIntervalMs, 60_000);
      assert.equal(exporter.current().timeoutMs, 5000);
      assert.deepEqual(exporter.current().perTopic, { enabled: true, maxTopics: 20 });
      exporter.retune(
        resolveOtlpMetricsOptions({
          endpoint,
          flushIntervalMs: 50,
          timeoutMs: 2500,
          perTopic: { maxTopics: 3 },
        })!,
      );
      assert.equal(exporter.current().flushIntervalMs, 50);
      assert.equal(exporter.current().timeoutMs, 2500);
      assert.deepEqual(exporter.current().perTopic, { enabled: true, maxTopics: 3 });
      // The rebuilt 50ms timer exports on its own.
      await waitFor(() => exporter.getStats().exportedMetrics > 0);
      const exported = exporter.getStats().exportedMetrics;
      // A stopped exporter stays stopped: retuning the interval after
      // shutdown moves the value but arms no timer.
      exporter.shutdown();
      exporter.retune(
        resolveOtlpMetricsOptions({ endpoint, flushIntervalMs: 25 })!,
      );
      assert.equal(exporter.current().flushIntervalMs, 25);
      await exporter.flush();
      await sleep(150);
      assert.equal(exporter.getStats().exportedMetrics, exported);
    } finally {
      exporter.shutdown();
      await plain.close();
      await collector.close();
    }
  });
});
