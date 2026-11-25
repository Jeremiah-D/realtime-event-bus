/**
 * Bus-level OTLP span export live enable (EB-557).
 *
 * A bus that never enabled `otlp` gains an exporter live on the first
 * `setOtlp` call with an `endpoint` — built exactly like the
 * construction row (`resolveOtlpOptions` then `new OtlpSpanExporter`),
 * so the rest fall back to the construction defaults, the unref flush
 * timer arms itself, and the first spans export from the next
 * admission onward. The install needs the trace pipeline underneath
 * it: a bus whose trace was never enabled refuses (fail-closed, along
 * the construction-time wording), and a bus that enabled tracing live
 * first (`setTraceEnabled(true)`) enables export right after.
 * Installing the exporter re-composes the trace pipeline's span
 * fan-out around it, so a user-supplied `onTraceSpan` keeps firing
 * alongside export. A failed enable leaves the bus unconfigured, so
 * a later valid call still enables it; `null` on a never-enabled bus
 * is a silent no-op with no audit; a closed bus refuses the enable
 * (EB-154 fail-fast); and the enable is live-only — a restart
 * restores the unconfigured state. The enable is audited as
 * `otlp_revised` with every `old*` value `null`. Once enabled,
 * revision follows EB-299/EB-357 and the `endpoint` / `headers` /
 * `serviceName` setters (EB-406 / EB-409 / EB-412) act on the live
 * exporter.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { createServer, type Server, type IncomingMessage, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import { EventBus } from '../src/bus.ts';
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

type TracesBody = {
  resourceSpans: Array<{
    resource: { attributes: Array<{ key: string; value: { stringValue: string } }> };
    scopeSpans: Array<{
      spans: Array<{ name: string }>;
    }>;
  }>;
};

function parseBody(request: CapturedRequest): TracesBody {
  return JSON.parse(request.body) as TracesBody;
}

function spanNames(body: TracesBody): string[] {
  return body.resourceSpans[0].scopeSpans[0].spans.map((s) => s.name).sort();
}

function serviceNameOf(body: TracesBody): string | undefined {
  return body.resourceSpans[0].resource.attributes.find((a) => a.key === 'service.name')?.value
    .stringValue;
}

describe('OTLP span export live enable (EB-557)', () => {
  it('enables a traced never-enabled bus with explicit values and audits every old value as null', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({ trace: { sampleRate: 1 } });
    try {
      assert.equal(bus.getStats().otlp, undefined);
      bus.setOtlp({
        endpoint,
        serviceName: 'span-bus',
        sampleRate: 0.5,
        batchSize: 100,
        flushIntervalMs: 1234,
        timeoutMs: 2345,
        headers: { authorization: 'Bearer x' },
        maxBuffered: 50,
      });
      const live = bus.getStats().otlp;
      assert.ok(live !== undefined);
      assert.equal(live.endpoint, endpoint);
      assert.equal(live.serviceName, 'span-bus');
      assert.equal(live.bufferedSpans, 0);
      assert.equal(live.exportedSpans, 0);
      assert.equal(live.failedSpans, 0);
      assert.equal(live.droppedSpans, 0);
      const audit = bus.getSubscriptionAudit({ action: 'otlp_revised' });
      assert.equal(audit.length, 1);
      assert.equal(audit[0].actor, 'system');
      assert.deepEqual(audit[0].detail, {
        sampleRate: 0.5,
        oldSampleRate: null,
        batchSize: 100,
        oldBatchSize: null,
        flushIntervalMs: 1234,
        oldFlushIntervalMs: null,
        timeoutMs: 2345,
        oldTimeoutMs: null,
        maxBuffered: 50,
        oldMaxBuffered: null,
      });
    } finally {
      await bus.close();
      await collector.close();
    }
  });

  it('enables with an endpoint alone and takes the construction defaults', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({ trace: { sampleRate: 1 } });
    try {
      bus.setOtlp({ endpoint });
      assert.ok(bus.getStats().otlp !== undefined);
      assert.equal(bus.getStats().otlp?.endpoint, endpoint);
      assert.equal(bus.getStats().otlp?.serviceName, 'realtime-event-bus');
      const audit = bus.getSubscriptionAudit({ action: 'otlp_revised' });
      assert.equal(audit.length, 1);
      assert.deepEqual(audit[0].detail, {
        sampleRate: 1,
        oldSampleRate: null,
        batchSize: 512,
        oldBatchSize: null,
        flushIntervalMs: 5000,
        oldFlushIntervalMs: null,
        timeoutMs: 5000,
        oldTimeoutMs: null,
        maxBuffered: 10000,
        oldMaxBuffered: null,
      });
    } finally {
      await bus.close();
      await collector.close();
    }
  });

  it('exports the next span to the stub collector and keeps the user hook firing', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const hooked: TraceSpan[] = [];
    const bus = new EventBus({ trace: { sampleRate: 1, onTraceSpan: (s) => hooked.push(s) } });
    try {
      bus.setOtlp({ endpoint, serviceName: 'span-bus', flushIntervalMs: 60_000 });
      bus.subscribe('t.*', () => {});
      bus.publish('t.1', { n: 1 });
      await flush();
      await bus.flushOtlp();
      await waitFor(() => collector.requests.length > 0);
      const body = parseBody(collector.requests[0]);
      assert.ok(spanNames(body).includes('bus.publish'));
      assert.equal(serviceNameOf(body), 'span-bus');
      assert.ok(hooked.length > 0);
      assert.ok((bus.getStats().otlp?.exportedSpans ?? 0) > 0);
      assert.equal(bus.getStats().otlp?.failedSpans, 0);
    } finally {
      await bus.close();
      await collector.close();
    }
  });

  it('refuses without trace, and a bus that enables tracing live first then enables export', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const plain = new EventBus();
    try {
      assert.throws(
        () => plain.setOtlp({ endpoint }),
        /otlp span export requires EventBusOptions\.trace/,
      );
      assert.equal(plain.getStats().otlp, undefined);
      assert.equal(plain.getSubscriptionAudit({ action: 'otlp_revised' }).length, 0);
      // The live trace path (`setTraceEnabled(true)`) is the way in:
      // once tracing is live, the same enable call installs the
      // exporter and the next span ships.
      plain.setTraceEnabled(true);
      plain.setOtlp({ endpoint, flushIntervalMs: 60_000 });
      assert.ok(plain.getStats().otlp !== undefined);
      plain.subscribe('t.*', () => {});
      plain.publish('t.1', { n: 1 });
      await flush();
      await plain.flushOtlp();
      await waitFor(() => collector.requests.length > 0);
      assert.ok(spanNames(parseBody(collector.requests[0])).includes('bus.publish'));
    } finally {
      await plain.close();
      await collector.close();
    }
  });

  it('{} and endpoint-less patches refuse and leave the bus unconfigured, then a valid call still enables', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({ trace: { sampleRate: 1 } });
    try {
      const bad: Array<unknown> = [
        {},
        { timeoutMs: 1000 },
        { sampleRate: 0.5 },
        { flushIntervalMs: 100 },
        { maxBuffered: 10 },
      ];
      for (const patch of bad) {
        assert.throws(() => bus.setOtlp(patch as never), RangeError);
      }
      assert.equal(bus.getStats().otlp, undefined);
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_revised' }).length, 0);
      bus.setOtlp({ endpoint });
      assert.ok(bus.getStats().otlp !== undefined);
    } finally {
      await bus.close();
      await collector.close();
    }
  });

  it('illegal endpoints refuse and leave the bus unconfigured', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({ trace: { sampleRate: 1 } });
    try {
      const bad: Array<unknown> = ['', 'not-a-url', 'ftp://127.0.0.1:9/v1/traces', 123];
      for (const patch of bad) {
        assert.throws(
          () => bus.setOtlp({ endpoint: patch } as never),
          RangeError,
          `expected RangeError for ${JSON.stringify(patch)}`,
        );
      }
      assert.equal(bus.getStats().otlp, undefined);
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_revised' }).length, 0);
      bus.setOtlp({ endpoint });
      assert.ok(bus.getStats().otlp !== undefined);
    } finally {
      await bus.close();
      await collector.close();
    }
  });

  it('illegal tuning values refuse and leave the bus unconfigured', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({ trace: { sampleRate: 1 } });
    try {
      const bad: Array<unknown> = [
        { sampleRate: 2 },
        { sampleRate: -0.1 },
        { sampleRate: Number.NaN },
        { sampleRate: Infinity },
        { batchSize: 0 },
        { batchSize: -1 },
        { batchSize: 1.5 },
        { flushIntervalMs: 0 },
        { flushIntervalMs: -1 },
        { flushIntervalMs: Number.NaN },
        { flushIntervalMs: Infinity },
        { timeoutMs: 0 },
        { timeoutMs: -1 },
        { timeoutMs: Number.NaN },
        { maxBuffered: 0 },
        { maxBuffered: 1.5 },
        { serviceName: '' },
        { headers: 'x' },
        { headers: { authorization: 1 } },
      ];
      for (const patch of bad) {
        assert.throws(
          () => bus.setOtlp({ endpoint, ...(patch as Record<string, unknown>) }),
          RangeError,
          `expected RangeError for ${JSON.stringify(patch)}`,
        );
      }
      assert.equal(bus.getStats().otlp, undefined);
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_revised' }).length, 0);
      bus.setOtlp({ endpoint });
      assert.ok(bus.getStats().otlp !== undefined);
    } finally {
      await bus.close();
      await collector.close();
    }
  });

  it('null on a never-enabled bus is a silent no-op with no audit', async () => {
    const bus = new EventBus({ trace: { sampleRate: 1 } });
    try {
      bus.setOtlp(null as never);
      assert.equal(bus.getStats().otlp, undefined);
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_revised' }).length, 0);
      // And the bus still enables afterwards.
      const collector = startCollector();
      const endpoint = await collector.endpoint;
      try {
        bus.setOtlp({ endpoint });
        assert.ok(bus.getStats().otlp !== undefined);
      } finally {
        await collector.close();
      }
    } finally {
      await bus.close();
    }
  });

  it('non-object revisions refuse and leave the bus unconfigured', async () => {
    const bus = new EventBus({ trace: { sampleRate: 1 } });
    try {
      for (const patch of ['nope', 42, []] as unknown[]) {
        assert.throws(() => bus.setOtlp(patch as never), RangeError);
      }
      assert.equal(bus.getStats().otlp, undefined);
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_revised' }).length, 0);
    } finally {
      await bus.close();
    }
  });

  it('post-enable revision merges live values and the live exporter keeps shipping', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({ trace: { sampleRate: 1 } });
    try {
      bus.setOtlp({ endpoint, flushIntervalMs: 60_000, timeoutMs: 5000 });
      bus.setOtlp({ flushIntervalMs: 50 });
      const audit = bus.getSubscriptionAudit({ action: 'otlp_revised' });
      assert.equal(audit.length, 2);
      assert.deepEqual(audit[1].detail, {
        sampleRate: 1,
        oldSampleRate: 1,
        batchSize: 512,
        oldBatchSize: 512,
        flushIntervalMs: 50,
        oldFlushIntervalMs: 60_000,
        timeoutMs: 5000,
        oldTimeoutMs: 5000,
        maxBuffered: 10000,
        oldMaxBuffered: 10000,
      });
      bus.subscribe('t.*', () => {});
      bus.publish('t.1', { n: 1 });
      await flush();
      await bus.flushOtlp();
      await waitFor(() => collector.requests.length > 0);
      assert.ok(spanNames(parseBody(collector.requests[0])).includes('bus.publish'));
    } finally {
      await bus.close();
      await collector.close();
    }
  });

  it('post-enable same-value and empty revisions are no-ops with no audit', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({ trace: { sampleRate: 1 } });
    try {
      bus.setOtlp({ endpoint, sampleRate: 0.5, batchSize: 100, flushIntervalMs: 60_000 });
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_revised' }).length, 1);
      bus.setOtlp({ sampleRate: 0.5 });
      bus.setOtlp({ batchSize: 100 });
      bus.setOtlp({ flushIntervalMs: 60_000 });
      bus.setOtlp({ timeoutMs: 5000 });
      bus.setOtlp({ maxBuffered: 10000 });
      bus.setOtlp({});
      bus.setOtlp(null as never);
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_revised' }).length, 1);
      // And the bus still revises afterwards.
      bus.setOtlp({ timeoutMs: 1000 });
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_revised' }).length, 2);
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
    const bus = new EventBus({ trace: { sampleRate: 1 } });
    try {
      bus.setOtlp({ endpoint: endpointA, flushIntervalMs: 60_000 });
      bus.setOtlpServiceName('renamed');
      bus.setOtlpHeaders({ authorization: 'Bearer test-token' });
      bus.setOtlpEndpoint(endpointB);
      const live = bus.getStats().otlp;
      assert.ok(live !== undefined);
      assert.equal(live.endpoint, endpointB);
      assert.equal(live.serviceName, 'renamed');
      bus.subscribe('t.*', () => {});
      bus.publish('t.1', { n: 1 });
      await flush();
      await bus.flushOtlp();
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
    const bus = new EventBus({ trace: { sampleRate: 1 } });
    try {
      await bus.close();
      assert.throws(() => bus.setOtlp({ endpoint }), /bus is closed/);
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_revised' }).length, 0);
    } finally {
      await collector.close();
    }
  });

  it('a restart restores the unconfigured state', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({ trace: { sampleRate: 1 } });
    try {
      bus.setOtlp({ endpoint });
      assert.ok(bus.getStats().otlp !== undefined);
      await bus.close();
      const restarted = new EventBus({ trace: { sampleRate: 1 } });
      try {
        assert.equal(restarted.getStats().otlp, undefined);
        assert.equal(restarted.getSubscriptionAudit({ action: 'otlp_revised' }).length, 0);
        assert.throws(() => restarted.setOtlp({ batchSize: 10 }), RangeError);
      } finally {
        await restarted.close();
      }
    } finally {
      await collector.close();
    }
  });
});
