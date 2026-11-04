import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createServer, type Server, type IncomingMessage, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import { EventBus } from '../src/bus.ts';
import {
  OtlpMetricsExporter,
  resolveOtlpMetricsOptions,
} from '../src/otlp-metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

interface CapturedRequest {
  url: string;
  headers: Record<string, string | string[] | undefined>;
  body: string;
}

/**
 * Stub OTLP/HTTP collector on 127.0.0.1. Records every POST body and
 * headers; the response status is configurable per test (default
 * 200). The exporter uses `agent: false`, so connections close after
 * each response and `close()` never hangs on pooled sockets.
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
      requests.push({ url: req.url ?? '/', headers: { ...req.headers }, body });
      res.writeHead(status, { 'content-type': 'application/json' });
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

/** A string payload whose JSON-serialized byte size is exactly `n`. */
const sized = (n: number): string => 'x'.repeat(n - 2);

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

test('resolveOtlpMetricsOptions rejects a non-URL endpoint', () => {
  assert.throws(
    () => resolveOtlpMetricsOptions({ endpoint: 'not a url' }),
    RangeError,
  );
});

test('resolveOtlpMetricsOptions rejects a non-http(s) endpoint', () => {
  assert.throws(
    () => resolveOtlpMetricsOptions({ endpoint: 'ftp://collector:4318/v1/metrics' }),
    RangeError,
  );
});

test('resolveOtlpMetricsOptions rejects non-positive flushIntervalMs/timeoutMs', () => {
  assert.throws(
    () => resolveOtlpMetricsOptions({ endpoint: 'http://c:4318/v1/metrics', flushIntervalMs: 0 }),
    RangeError,
  );
  assert.throws(
    () => resolveOtlpMetricsOptions({ endpoint: 'http://c:4318/v1/metrics', flushIntervalMs: -5 }),
    RangeError,
  );
  assert.throws(
    () => resolveOtlpMetricsOptions({ endpoint: 'http://c:4318/v1/metrics', timeoutMs: 0 }),
    RangeError,
  );
});

test('resolveOtlpMetricsOptions rejects a non-positive-integer maxTopics', () => {
  assert.throws(
    () =>
      resolveOtlpMetricsOptions({
        endpoint: 'http://c:4318/v1/metrics',
        perTopic: { maxTopics: 0 },
      }),
    RangeError,
  );
  assert.throws(
    () =>
      resolveOtlpMetricsOptions({
        endpoint: 'http://c:4318/v1/metrics',
        perTopic: { maxTopics: 1.5 },
      }),
    RangeError,
  );
});

test('resolveOtlpMetricsOptions applies defaults and returns undefined when disabled', () => {
  assert.equal(resolveOtlpMetricsOptions(undefined), undefined);
  assert.equal(resolveOtlpMetricsOptions(false), undefined);
  assert.equal(resolveOtlpMetricsOptions(null), undefined);
  const resolved = resolveOtlpMetricsOptions({ endpoint: 'http://c:4318/v1/metrics' });
  assert.ok(resolved !== undefined);
  assert.equal(resolved.serviceName, 'realtime-event-bus');
  assert.equal(resolved.flushIntervalMs, 5000);
  assert.equal(resolved.timeoutMs, 5000);
  assert.deepEqual(resolved.perTopic, { enabled: true, maxTopics: 20 });
  assert.deepEqual(resolveOtlpMetricsOptions({
    endpoint: 'http://c:4318/v1/metrics',
    perTopic: false,
  })?.perTopic, { enabled: false, maxTopics: 20 });
});

test('an invalid otlpMetrics config throws RangeError from the constructor', () => {
  assert.throws(
    () => new EventBus({ otlpMetrics: { endpoint: 'ftp://c:4318/v1/metrics' } }),
    RangeError,
  );
});

test('getStats() has no otlpMetrics field when metrics export is not configured', () => {
  const bus = new EventBus();
  assert.ok(!('otlpMetrics' in bus.getStats()));
});

test('stub collector receives valid OTLP JSON with Sum counters matching the snapshot', async () => {
  const collector = startCollector();
  const endpoint = await collector.endpoint;
  try {
    const bus = new EventBus({ otlpMetrics: { endpoint } });
    bus.subscribe('t', () => {});
    bus.publish('t', 'one');
    bus.publish('t', 'two');
    bus.publish('t', 'three');
    await flush();
    await bus.flushOtlpMetrics();
    assert.equal(collector.requests.length, 1);
    const body = parseBody(collector.requests[0]);
    assert.equal(
      body.resourceMetrics[0].resource.attributes.find((a) => a.key === 'service.name')?.value
        .stringValue,
      'realtime-event-bus',
    );
    assert.ok(metricsOf(body).length > 0);
    const published = findMetric(body, 'eventbus_published_messages_total');
    assert.ok(published !== undefined);
    const sum = published.sum as { aggregationTemporality: number; isMonotonic: boolean; dataPoints: Array<{ asInt: string; startTimeUnixNano: string; timeUnixNano: string }> };
    assert.equal(sum.aggregationTemporality, 2); // CUMULATIVE
    assert.equal(sum.isMonotonic, true);
    assert.equal(sum.dataPoints[0].asInt, '3');
    assert.ok(BigInt(sum.dataPoints[0].startTimeUnixNano) > 0n);
    assert.ok(BigInt(sum.dataPoints[0].timeUnixNano) >= BigInt(sum.dataPoints[0].startTimeUnixNano));
    // The bus-level Sum has no attributes; the per-topic twin (if any)
    // carries a `topic` attribute instead.
    assert.deepEqual(sum.dataPoints[0].attributes, []);
    const stats = bus.getStats().otlpMetrics;
    assert.ok(stats !== undefined);
    assert.equal(stats.endpoint, endpoint);
    assert.ok(stats.exportedMetrics > 0);
    assert.equal(stats.failedExports, 0);
    assert.ok(stats.lastExportAtMs > 0);
    await bus.close();
  } finally {
    await collector.close();
  }
});

test('delivery latency gauges match the snapshot p50/p95/p99', async () => {
  const collector = startCollector();
  const endpoint = await collector.endpoint;
  try {
    let nowMs = 1_700_000_000_000;
    const bus = new EventBus({ now: () => nowMs, otlpMetrics: { endpoint } });
    bus.subscribe('t', () => {}, { deliveryLatency: true });
    bus.publish('t', 'a');
    nowMs += 15;
    await flush();
    bus.publish('t', 'b');
    nowMs += 25;
    await flush();
    const snapshot = bus.getStats().deliveryLatency[0];
    assert.equal(snapshot.samples, 2);
    await bus.flushOtlpMetrics();
    assert.equal(collector.requests.length, 1);
    const body = parseBody(collector.requests[0]);
    const latency = findMetric(body, 'eventbus_delivery_latency_ms');
    assert.ok(latency !== undefined);
    const points = dataPoints(latency);
    assert.equal(points.length, 3);
    const byQuantile = new Map(points.map((p) => [attrValue(p, 'quantile'), p.asDouble]));
    assert.equal(byQuantile.get('0.5'), snapshot.p50Ms);
    assert.equal(byQuantile.get('0.95'), snapshot.p95Ms);
    assert.equal(byQuantile.get('0.99'), snapshot.p99Ms);
    const samples = findMetric(body, 'eventbus_delivery_latency_samples');
    assert.equal(dataPoints(samples)[0].asInt, '2');
    await bus.close();
  } finally {
    await collector.close();
  }
});

test('per-topic payload histogram maps to an OTLP Histogram with +Inf and differential counts', async () => {
  const collector = startCollector();
  const endpoint = await collector.endpoint;
  try {
    const bus = new EventBus({ otlpMetrics: { endpoint } });
    bus.subscribe('orders.eu', () => {});
    bus.setTopicPayloadHistogram('orders.eu', { buckets: [100, 10000] });
    bus.publish('orders.eu', sized(50));
    bus.publish('orders.eu', sized(5000));
    bus.publish('orders.eu', sized(20000));
    await flush();
    await bus.flushOtlpMetrics();
    assert.equal(collector.requests.length, 1);
    const body = parseBody(collector.requests[0]);
    const histogram = findMetric(body, 'eventbus_payload_bytes');
    assert.ok(histogram !== undefined);
    const points = dataPoints(histogram);
    const topicPoint = points.find((p) => attrValue(p, 'topic') === 'orders.eu');
    assert.ok(topicPoint !== undefined);
    assert.deepEqual(topicPoint.explicitBounds, [100, 10000, '+Inf']);
    assert.deepEqual(topicPoint.bucketCounts, ['1', '1', '1']);
    assert.equal(topicPoint.count, '3');
    assert.equal(topicPoint.sum, 50 + 5000 + 20000);
    await bus.close();
  } finally {
    await collector.close();
  }
});

test('per-topic series cover only the hottest maxTopics topics', async () => {
  const collector = startCollector();
  const endpoint = await collector.endpoint;
  try {
    const bus = new EventBus({ otlpMetrics: { endpoint, perTopic: { maxTopics: 1 } } });
    bus.subscribe('hot', () => {});
    bus.subscribe('warm', () => {});
    bus.subscribe('cold', () => {});
    for (let i = 0; i < 5; i += 1) bus.publish('hot', i);
    bus.publish('warm', 1);
    bus.publish('cold', 1);
    await flush();
    await bus.flushOtlpMetrics();
    const body = parseBody(collector.requests[0]);
    const published = findMetric(body, 'eventbus_published_messages_total');
    const topicPoints = dataPoints(published).filter((p) => attrValue(p, 'topic') !== undefined);
    assert.equal(topicPoints.length, 1);
    assert.equal(attrValue(topicPoints[0], 'topic'), 'hot');
    assert.equal(topicPoints[0].asInt, '5');
    await bus.close();
  } finally {
    await collector.close();
  }
});

test('perTopic:false emits no topic-labelled series', async () => {
  const collector = startCollector();
  const endpoint = await collector.endpoint;
  try {
    const bus = new EventBus({ otlpMetrics: { endpoint, perTopic: false } });
    bus.subscribe('t', () => {});
    bus.publish('t', 'x');
    await flush();
    await bus.flushOtlpMetrics();
    const body = parseBody(collector.requests[0]);
    for (const metric of metricsOf(body)) {
      for (const point of dataPoints(metric)) {
        assert.equal(attrValue(point, 'topic'), undefined);
      }
    }
    await bus.close();
  } finally {
    await collector.close();
  }
});

test('a failed POST counts failedExports and never disturbs publish/deliver', async () => {
  const collector = startCollector(500);
  const endpoint = await collector.endpoint;
  try {
    const bus = new EventBus({ otlpMetrics: { endpoint } });
    let delivered = 0;
    bus.subscribe('t', () => {
      delivered += 1;
    });
    bus.publish('t', 'x');
    await flush();
    assert.equal(delivered, 1);
    await bus.flushOtlpMetrics();
    assert.equal(collector.requests.length, 1);
    const stats = bus.getStats().otlpMetrics;
    assert.ok(stats !== undefined);
    assert.equal(stats.failedExports, 1);
    assert.equal(stats.exportedMetrics, 0);
    // Publish/deliver keep working after the failed export.
    bus.publish('t', 'y');
    await flush();
    assert.equal(delivered, 2);
    await bus.close();
  } finally {
    await collector.close();
  }
});

test('headers are passed through to the export POST', async () => {
  const collector = startCollector();
  const endpoint = await collector.endpoint;
  try {
    const bus = new EventBus({
      otlpMetrics: { endpoint, headers: { Authorization: 'Bearer test-token' } },
    });
    await bus.flushOtlpMetrics();
    assert.equal(collector.requests.length, 1);
    assert.equal(collector.requests[0].headers.authorization, 'Bearer test-token');
    await bus.close();
  } finally {
    await collector.close();
  }
});

test('flush() manually triggers one export and exportedMetrics increments', async () => {
  const collector = startCollector();
  const endpoint = await collector.endpoint;
  try {
    const bus = new EventBus({ otlpMetrics: { endpoint } });
    await bus.flushOtlpMetrics();
    await bus.flushOtlpMetrics();
    assert.equal(collector.requests.length, 2);
    const stats = bus.getStats().otlpMetrics;
    assert.ok(stats !== undefined);
    assert.ok(stats.exportedMetrics > 0);
    await bus.close();
  } finally {
    await collector.close();
  }
});

test('close() triggers exactly one final export and a second close() adds none', async () => {
  const collector = startCollector();
  const endpoint = await collector.endpoint;
  try {
    const bus = new EventBus({ otlpMetrics: { endpoint } });
    bus.subscribe('t', () => {});
    bus.publish('t', 'x');
    await flush();
    await bus.close();
    assert.equal(collector.requests.length, 1);
    const published = findMetric(parseBody(collector.requests[0]), 'eventbus_published_messages_total');
    assert.equal(dataPoints(published)[0].asInt, '1');
    await bus.close();
    assert.equal(collector.requests.length, 1);
  } finally {
    await collector.close();
  }
});

test('the unref export timer does not hold the process open', async () => {
  const exited = await new Promise<boolean>((resolve) => {
    const child = spawn(
      process.execPath,
      [
        '--input-type=module',
        '-e',
        "import('./src/bus.ts').then(({ EventBus }) => { new EventBus({ otlpMetrics: { endpoint: 'http://127.0.0.1:1/v1/metrics', flushIntervalMs: 60000 } }); });",
      ],
      { cwd: process.cwd() },
    );
    const timer = setTimeout(() => {
      child.kill('SIGKILL');
      resolve(false);
    }, 5000);
    child.on('exit', (code) => {
      clearTimeout(timer);
      resolve(code === 0);
    });
    child.on('error', () => {
      clearTimeout(timer);
      resolve(false);
    });
  });
  assert.equal(exited, true);
});

test('the exporter never throws on a failing snapshot supplier or a dead collector', async () => {
  const exporter = new OtlpMetricsExporter(
    resolveOtlpMetricsOptions({ endpoint: 'http://127.0.0.1:1/v1/metrics' })!,
    () => {
      throw new Error('boom');
    },
  );
  await exporter.flush();
  assert.equal(exporter.getStats().failedExports, 1);
  exporter.shutdown();
  // A stopped exporter never exports again.
  await exporter.flush();
  assert.equal(exporter.getStats().failedExports, 1);
});
