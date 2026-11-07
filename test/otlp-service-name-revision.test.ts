/**
 * OTLP span export service-name rename (EB-412).
 *
 * `setOtlpServiceName` renames the export identity live — the next
 * export POST carries the new `service.name`, with no restart and no
 * loss of live bus state. A POST already in flight keeps the name it
 * started with; buffered spans stay buffered, are never re-sent and
 * never dropped by the rename — but they export under the new name,
 * since the name is encoded at export time (the series forks at the
 * rename point, no backfill). The flush timer is not rebuilt; the five
 * numeric bounds, the endpoint, and `headers` are untouched. Only a
 * bus that already enabled `otlp` can be renamed; a closed bus
 * refuses; an illegal name fails closed with the live name untouched;
 * a rename identical to the current name is a no-op with no audit.
 * The live name is visible as `getStats().otlp.serviceName`.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { createServer, type Server, type IncomingMessage, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import { EventBus } from '../src/bus.ts';
import { OtlpSpanExporter, resolveOtlpOptions } from '../src/otlp.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** Counts the spans in one OTLP/HTTP JSON request body. */
function spanCount(body: string): number {
  const parsed = JSON.parse(body) as {
    resourceSpans: Array<{ scopeSpans: Array<{ spans: unknown[] }> }>;
  };
  return parsed.resourceSpans[0].scopeSpans[0].spans.length;
}

/** Reads the `service.name` resource attribute from a POST body. */
function serviceNameOf(body: string): string | undefined {
  const parsed = JSON.parse(body) as {
    resourceSpans: Array<{
      resource: { attributes: Array<{ key: string; value: { stringValue?: string } }> };
    }>;
  };
  return parsed.resourceSpans[0].resource.attributes.find((a) => a.key === 'service.name')?.value
    .stringValue;
}

/**
 * Stub OTLP/HTTP collector on 127.0.0.1. Records every POST body and
 * its headers. The exporter uses `agent: false`, so connections close
 * after each response and `close()` never hangs on pooled sockets.
 * `respondAfterMs` delays each response, so a rename can land while a
 * POST is genuinely in flight.
 */
function startCollector(respondAfterMs = 0): {
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
      const respond = () => {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end('{}');
      };
      if (respondAfterMs > 0) setTimeout(respond, respondAfterMs);
      else respond();
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

describe('OTLP span export service-name rename (EB-412)', () => {
  it('the next flush carries the new service.name and the old name is gone', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: { endpoint, flushIntervalMs: 60_000, serviceName: 'checkout-bus' },
    });
    try {
      bus.subscribe('t', () => {});
      bus.publish('t', 'a');
      await flush();
      await bus.flushOtlp();
      assert.equal(collector.requests.length, 1);
      assert.equal(serviceNameOf(collector.requests[0].body), 'checkout-bus');

      bus.setOtlpServiceName('payments-bus');
      bus.publish('t', 'b');
      await flush();
      await bus.flushOtlp();
      assert.equal(collector.requests.length, 2, 'the next flush must POST with the new name');
      assert.equal(serviceNameOf(collector.requests[1].body), 'payments-bus');
      assert.ok(!collector.requests[1].body.includes('checkout-bus'), 'the old name must not appear');
    } finally {
      await collector.close();
    }
  });

  it('buffered spans export under the new name: not lost, not re-sent', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: { endpoint, flushIntervalMs: 60_000, serviceName: 'old-name' },
    });
    try {
      bus.subscribe('t', () => {});
      bus.publish('t', 'a');
      await flush();
      const buffered = bus.getStats().otlp?.bufferedSpans ?? 0;
      assert.ok(buffered > 0, 'expected spans buffered before the rename');
      // The rename itself keeps the buffer.
      bus.setOtlpServiceName('new-name');
      assert.equal(bus.getStats().otlp?.bufferedSpans, buffered);
      await bus.flushOtlp();
      assert.equal(bus.getStats().otlp?.bufferedSpans, 0);
      assert.equal(collector.requests.length, 1);
      const received = collector.requests.reduce((n, r) => n + spanCount(r.body), 0);
      assert.equal(received, buffered, 'every buffered span must arrive exactly once');
      assert.equal(bus.getStats().otlp?.exportedSpans, buffered);
      // Buffered at rename time, exported after: the new name wins.
      assert.equal(serviceNameOf(collector.requests[0].body), 'new-name');
    } finally {
      await collector.close();
    }
  });

  it('a POST already in flight keeps the name it started with', async () => {
    const collector = startCollector(100);
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: { endpoint, flushIntervalMs: 60_000, serviceName: 'in-flight-old' },
    });
    try {
      bus.subscribe('t', () => {});
      bus.publish('t', 'a');
      await flush();
      // Start the flush, rename while the POST is in flight, then
      // wait for the POST to land: the body was encoded at request
      // build time, so the rename cannot reach it.
      const inFlight = bus.flushOtlp();
      bus.setOtlpServiceName('in-flight-new');
      await inFlight;
      assert.equal(collector.requests.length, 1);
      assert.equal(serviceNameOf(collector.requests[0].body), 'in-flight-old');
      // The next POST after the rename carries the new name.
      bus.publish('t', 'b');
      await flush();
      await bus.flushOtlp();
      assert.equal(collector.requests.length, 2);
      assert.equal(serviceNameOf(collector.requests[1].body), 'in-flight-new');
    } finally {
      await collector.close();
    }
  });

  it('setOtlpServiceName fails closed when otlp is not enabled', async () => {
    const plain = new EventBus();
    assert.throws(() => plain.setOtlpServiceName('svc'), RangeError);
    assert.equal(plain.getSubscriptionAudit({ action: 'otlp_service_name_revised' }).length, 0);

    // Trace alone is not export: the exporter was never built.
    const traced = new EventBus({ trace: { sampleRate: 1 } });
    assert.throws(() => traced.setOtlpServiceName('svc'), RangeError);
    assert.equal(traced.getSubscriptionAudit({ action: 'otlp_service_name_revised' }).length, 0);
  });

  it('setOtlpServiceName rejects illegal names and leaves the live name untouched', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: { endpoint, flushIntervalMs: 60_000, serviceName: 'live-name' },
    });
    try {
      for (const bad of ['', 42, null, undefined, {}, ['svc'], true]) {
        assert.throws(
          () => bus.setOtlpServiceName(bad as never),
          RangeError,
          `expected RangeError for ${JSON.stringify(bad)}`,
        );
      }
      assert.throws(
        () => bus.setOtlpServiceName('svc', ''),
        RangeError,
        'an illegal actor throws',
      );
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_service_name_revised' }).length, 0);
      assert.equal(bus.getStats().otlp?.serviceName, 'live-name');
      // The live name is untouched: the next POST still carries it.
      bus.subscribe('t', () => {});
      bus.publish('t', 'a');
      await flush();
      await bus.flushOtlp();
      assert.equal(collector.requests.length, 1);
      assert.equal(serviceNameOf(collector.requests[0].body), 'live-name');
      // And the bus still renames afterwards.
      bus.setOtlpServiceName('new-name');
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_service_name_revised' }).length, 1);
    } finally {
      await collector.close();
    }
  });

  it('renaming to the current name is a no-op with no audit', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: { endpoint, flushIntervalMs: 60_000, serviceName: 'same-name' },
    });
    try {
      bus.setOtlpServiceName('same-name');
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_service_name_revised' }).length, 0);
      assert.equal(bus.getStats().otlp?.serviceName, 'same-name');
      // The untouched name still governs the next flush.
      bus.subscribe('t', () => {});
      bus.publish('t', 'a');
      await flush();
      await bus.flushOtlp();
      assert.equal(collector.requests.length, 1);
      assert.equal(serviceNameOf(collector.requests[0].body), 'same-name');
    } finally {
      await collector.close();
    }
  });

  it('a rename audits the exact previous and current names with the actor', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: { endpoint, flushIntervalMs: 60_000, serviceName: 'checkout-bus' },
    });
    try {
      bus.setOtlpServiceName('payments-bus', 'ops');
      const audit = bus.getSubscriptionAudit({ action: 'otlp_service_name_revised' });
      assert.equal(audit.length, 1);
      assert.equal(audit[0].actor, 'ops');
      assert.deepEqual(audit[0].detail, {
        serviceName: 'payments-bus',
        oldServiceName: 'checkout-bus',
      });
      // The default service name audits as the previous value.
      const plain = new EventBus({
        trace: { sampleRate: 1 },
        otlp: { endpoint, flushIntervalMs: 60_000 },
      });
      plain.setOtlpServiceName('renamed');
      const plainAudit = plain.getSubscriptionAudit({ action: 'otlp_service_name_revised' });
      assert.deepEqual(plainAudit[0].detail, {
        serviceName: 'renamed',
        oldServiceName: 'realtime-event-bus',
      });
    } finally {
      await collector.close();
    }
  });

  it('a rename leaves the five numeric bounds, endpoint, and headers untouched', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    // The collector close owns the outer finally: an early throw in
    // either half must not leak the listening server (a leaked
    // listener keeps the test worker alive).
    try {
      // Exporter level: current() reports every live value.
      const exporter = new OtlpSpanExporter(
        resolveOtlpOptions({
          endpoint,
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
        exporter.setServiceName('svc-renamed');
        const after = exporter.current();
        assert.equal(after.endpoint.toString(), before.endpoint.toString());
        assert.equal(after.sampleRate, before.sampleRate);
        assert.equal(after.batchSize, before.batchSize);
        assert.equal(after.flushIntervalMs, before.flushIntervalMs);
        assert.equal(after.timeoutMs, before.timeoutMs);
        assert.equal(after.maxBuffered, before.maxBuffered);
        assert.deepEqual(after.headers, { Authorization: 'Bearer x' });
        assert.equal(after.serviceName, 'svc-renamed');
        assert.equal(exporter.getStats().serviceName, 'svc-renamed');
      } finally {
        await exporter.shutdown();
      }
      // Bus level: the renamed identity still rides the same
      // endpoint and headers.
      const bus = new EventBus({
        trace: { sampleRate: 1 },
        otlp: {
          endpoint,
          flushIntervalMs: 60_000,
          headers: { Authorization: 'Bearer old' },
          serviceName: 'svc-name',
        },
      });
      bus.subscribe('t', () => {});
      bus.setOtlpServiceName('svc-renamed');
      assert.equal(bus.getStats().otlp?.endpoint, endpoint);
      assert.equal(bus.getStats().otlp?.serviceName, 'svc-renamed');
      bus.publish('t', 'a');
      await flush();
      await bus.flushOtlp();
      assert.equal(collector.requests.length, 1);
      assert.equal(collector.requests[0].headers['authorization'], 'Bearer old');
      assert.equal(serviceNameOf(collector.requests[0].body), 'svc-renamed');
    } finally {
      await collector.close();
    }
  });

  it('getStats().otlp.serviceName follows the live name', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: { endpoint, flushIntervalMs: 60_000, serviceName: 'svc-a' },
    });
    try {
      assert.equal(bus.getStats().otlp?.serviceName, 'svc-a');
      bus.setOtlpServiceName('svc-b');
      assert.equal(bus.getStats().otlp?.serviceName, 'svc-b');
      bus.setOtlpServiceName('svc-c');
      assert.equal(bus.getStats().otlp?.serviceName, 'svc-c');
      // The default is reported when no name was configured.
      const plain = new EventBus({
        trace: { sampleRate: 1 },
        otlp: { endpoint, flushIntervalMs: 60_000 },
      });
      assert.equal(plain.getStats().otlp?.serviceName, 'realtime-event-bus');
    } finally {
      await collector.close();
    }
  });

  it('consecutive renames each govern the next POST', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: { endpoint, flushIntervalMs: 60_000, serviceName: 'first' },
    });
    try {
      bus.subscribe('t', () => {});
      bus.setOtlpServiceName('second');
      bus.publish('t', 'a');
      await flush();
      await bus.flushOtlp();
      assert.equal(serviceNameOf(collector.requests[0].body), 'second');
      bus.setOtlpServiceName('third');
      bus.publish('t', 'b');
      await flush();
      await bus.flushOtlp();
      assert.equal(collector.requests.length, 2);
      assert.equal(serviceNameOf(collector.requests[1].body), 'third');
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_service_name_revised' }).length, 2);
    } finally {
      await collector.close();
    }
  });

  it('a closed bus refuses the rename', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: { endpoint, flushIntervalMs: 60_000, serviceName: 'svc' },
    });
    try {
      await bus.close();
      assert.throws(() => bus.setOtlpServiceName('renamed'), RangeError);
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_service_name_revised' }).length, 0);
    } finally {
      await collector.close();
    }
  });
});
