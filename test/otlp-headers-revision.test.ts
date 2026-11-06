/**
 * OTLP span export headers rotation (EB-409).
 *
 * `setOtlpHeaders` rotates the collector request headers live — the
 * next export POST carries the new header set, with no restart and no
 * loss of live bus state. Full replacement, never a per-header merge:
 * a header absent from the new map stops being sent, and `{}` clears
 * the headers back to the construction-time default. A POST already in
 * flight keeps the headers it started with; buffered spans stay
 * buffered, are never re-sent and never dropped by the rotation; the
 * flush timer is not rebuilt; the five numeric bounds, the endpoint,
 * and `serviceName` are untouched. Only a bus that already enabled
 * `otlp` can be rotated; a closed bus refuses; illegal headers fail
 * closed with the live headers untouched; a rotation identical to the
 * current header set (deep equality, key order-insensitive) is a
 * no-op with no audit. The audit records header names only — values
 * are never written to the audit record.
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

describe('OTLP span export headers rotation (EB-409)', () => {
  it('rotation sends the next flush with the new Authorization', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: {
        endpoint,
        flushIntervalMs: 60_000,
        headers: { Authorization: 'Bearer old', 'x-tenant': 'acme' },
      },
    });
    try {
      bus.subscribe('t', () => {});
      bus.publish('t', 'a');
      await flush();
      await bus.flushOtlp();
      assert.equal(collector.requests.length, 1);
      assert.equal(collector.requests[0].headers['authorization'], 'Bearer old');
      assert.equal(collector.requests[0].headers['x-tenant'], 'acme');

      bus.setOtlpHeaders({ Authorization: 'Bearer new' });
      bus.publish('t', 'b');
      await flush();
      await bus.flushOtlp();
      assert.equal(collector.requests.length, 2, 'the next flush must POST with the new headers');
      assert.equal(collector.requests[1].headers['authorization'], 'Bearer new');
    } finally {
      await collector.close();
    }
  });

  it('the old credential is gone from the next POST: no per-header merge', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: {
        endpoint,
        flushIntervalMs: 60_000,
        headers: { Authorization: 'Bearer old', 'x-tenant': 'acme', 'x-extra': 'e' },
      },
    });
    try {
      bus.subscribe('t', () => {});
      // A single-field rotation replaces the whole set: the old
      // Authorization and the untouched-by-the-call x-extra must not
      // linger beside the new tenant header.
      bus.setOtlpHeaders({ 'x-tenant': 'other' });
      bus.publish('t', 'a');
      await flush();
      await bus.flushOtlp();
      assert.equal(collector.requests.length, 1);
      assert.equal(collector.requests[0].headers['authorization'], undefined);
      assert.equal(collector.requests[0].headers['x-tenant'], 'other');
      assert.equal(collector.requests[0].headers['x-extra'], undefined);
    } finally {
      await collector.close();
    }
  });

  it('buffered spans survive a rotation: not lost, not re-sent', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: {
        endpoint,
        flushIntervalMs: 60_000,
        headers: { Authorization: 'Bearer old' },
      },
    });
    try {
      bus.subscribe('t', () => {});
      bus.publish('t', 'a');
      await flush();
      const buffered = bus.getStats().otlp?.bufferedSpans ?? 0;
      assert.ok(buffered > 0, 'expected spans buffered before the rotation');
      // The rotation itself keeps the buffer.
      bus.setOtlpHeaders({ Authorization: 'Bearer new' });
      assert.equal(bus.getStats().otlp?.bufferedSpans, buffered);
      await bus.flushOtlp();
      assert.equal(bus.getStats().otlp?.bufferedSpans, 0);
      assert.equal(collector.requests.length, 1);
      const received = collector.requests.reduce((n, r) => n + spanCount(r.body), 0);
      assert.equal(received, buffered, 'every buffered span must arrive exactly once');
      assert.equal(bus.getStats().otlp?.exportedSpans, buffered);
      assert.equal(collector.requests[0].headers['authorization'], 'Bearer new');
    } finally {
      await collector.close();
    }
  });

  it('setOtlpHeaders fails closed when otlp is not enabled', async () => {
    const plain = new EventBus();
    assert.throws(() => plain.setOtlpHeaders({ Authorization: 'Bearer x' }), RangeError);
    assert.equal(plain.getSubscriptionAudit({ action: 'otlp_headers_revised' }).length, 0);

    // Trace alone is not export: the exporter was never built.
    const traced = new EventBus({ trace: { sampleRate: 1 } });
    assert.throws(() => traced.setOtlpHeaders({ Authorization: 'Bearer x' }), RangeError);
    assert.equal(traced.getSubscriptionAudit({ action: 'otlp_headers_revised' }).length, 0);
  });

  it('setOtlpHeaders rejects illegal headers and leaves the live headers untouched', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: {
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
          () => bus.setOtlpHeaders(bad as never),
          RangeError,
          `expected RangeError for ${JSON.stringify(bad)}`,
        );
      }
      assert.throws(
        () => bus.setOtlpHeaders({ Authorization: 'Bearer x' }, ''),
        RangeError,
        'an illegal actor throws',
      );
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_headers_revised' }).length, 0);
      // The live headers are untouched: the next POST still carries
      // the original credential.
      bus.subscribe('t', () => {});
      bus.publish('t', 'a');
      await flush();
      await bus.flushOtlp();
      assert.equal(collector.requests.length, 1);
      assert.equal(collector.requests[0].headers['authorization'], 'Bearer live');
      // And the bus still rotates afterwards.
      bus.setOtlpHeaders({ Authorization: 'Bearer new' });
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_headers_revised' }).length, 1);
    } finally {
      await collector.close();
    }
  });

  it('rotating to the current headers is a no-op with no audit', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: {
        endpoint,
        flushIntervalMs: 60_000,
        headers: { Authorization: 'Bearer x', 'x-tenant': 'acme' },
      },
    });
    try {
      bus.setOtlpHeaders({ Authorization: 'Bearer x', 'x-tenant': 'acme' });
      // Key order does not matter: a reordered copy is the same set.
      bus.setOtlpHeaders({ 'x-tenant': 'acme', Authorization: 'Bearer x' });
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_headers_revised' }).length, 0);
      // The untouched headers still govern the next flush.
      bus.subscribe('t', () => {});
      bus.publish('t', 'a');
      await flush();
      await bus.flushOtlp();
      assert.equal(collector.requests.length, 1);
      assert.equal(collector.requests[0].headers['authorization'], 'Bearer x');
      assert.equal(collector.requests[0].headers['x-tenant'], 'acme');
    } finally {
      await collector.close();
    }
  });

  it('a rotation audits header names only, never credential values', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: {
        endpoint,
        flushIntervalMs: 60_000,
        headers: { Authorization: 'Bearer old' },
      },
    });
    try {
      bus.setOtlpHeaders({ Authorization: 'Bearer new', 'x-tenant': 't-1' }, 'ops');
      const audit = bus.getSubscriptionAudit({ action: 'otlp_headers_revised' });
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
      await collector.close();
    }
  });

  it('an empty object clears the headers', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: {
        endpoint,
        flushIntervalMs: 60_000,
        headers: { Authorization: 'Bearer old' },
      },
    });
    try {
      bus.subscribe('t', () => {});
      bus.publish('t', 'a');
      await flush();
      await bus.flushOtlp();
      assert.equal(collector.requests[0].headers['authorization'], 'Bearer old');

      bus.setOtlpHeaders({});
      const audit = bus.getSubscriptionAudit({ action: 'otlp_headers_revised' });
      assert.deepEqual(audit[0].detail, { headers: [], oldHeaders: ['Authorization'] });
      bus.publish('t', 'b');
      await flush();
      await bus.flushOtlp();
      assert.equal(collector.requests.length, 2);
      assert.equal(collector.requests[1].headers['authorization'], undefined);
    } finally {
      await collector.close();
    }
  });

  it('a rotation leaves the five numeric bounds, endpoint, and serviceName untouched', async () => {
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
        const rotated = { Authorization: 'Bearer new' };
        exporter.setHeaders(rotated);
        // Mutating the caller's object afterwards must not move the
        // live headers: setHeaders stores a defensive copy.
        rotated.Authorization = 'Bearer tampered';
        const after = exporter.current();
        assert.equal(after.endpoint.toString(), before.endpoint.toString());
        assert.equal(after.sampleRate, before.sampleRate);
        assert.equal(after.batchSize, before.batchSize);
        assert.equal(after.flushIntervalMs, before.flushIntervalMs);
        assert.equal(after.timeoutMs, before.timeoutMs);
        assert.equal(after.maxBuffered, before.maxBuffered);
        assert.deepEqual(after.headers, { Authorization: 'Bearer new' });
        assert.equal(after.serviceName, before.serviceName);
      } finally {
        await exporter.shutdown();
      }
      // Bus level: the new headers still ride the same identity.
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
      bus.setOtlpHeaders({ Authorization: 'Bearer new' });
      assert.equal(bus.getStats().otlp?.endpoint, endpoint);
      bus.publish('t', 'a');
      await flush();
      await bus.flushOtlp();
      assert.equal(collector.requests.length, 1);
      assert.equal(collector.requests[0].headers['authorization'], 'Bearer new');
      const body = JSON.parse(collector.requests[0].body) as {
        resourceSpans: Array<{
          resource: { attributes: Array<{ key: string; value: { stringValue?: string } }> };
        }>;
      };
      const serviceName = body.resourceSpans[0].resource.attributes.find(
        (a) => a.key === 'service.name',
      )?.value.stringValue;
      assert.equal(serviceName, 'svc-name');
    } finally {
      await collector.close();
    }
  });

  it('a closed bus refuses the rotation', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: { endpoint, flushIntervalMs: 60_000, headers: { Authorization: 'Bearer x' } },
    });
    try {
      await bus.close();
      assert.throws(() => bus.setOtlpHeaders({ Authorization: 'Bearer y' }), RangeError);
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_headers_revised' }).length, 0);
    } finally {
      await collector.close();
    }
  });

  it('mutating the caller object after the call cannot move the live headers', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: {
        endpoint,
        flushIntervalMs: 60_000,
        headers: { Authorization: 'Bearer old' },
      },
    });
    try {
      const rotated = { Authorization: 'Bearer new' };
      bus.setOtlpHeaders(rotated);
      rotated.Authorization = 'Bearer tampered';
      bus.subscribe('t', () => {});
      bus.publish('t', 'a');
      await flush();
      await bus.flushOtlp();
      assert.equal(collector.requests.length, 1);
      assert.equal(collector.requests[0].headers['authorization'], 'Bearer new');
    } finally {
      await collector.close();
    }
  });
});
