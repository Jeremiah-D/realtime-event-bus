/**
 * Bus-level OTLP export revision (EB-299; EB-357 adds the flush
 * interval).
 *
 * `setOtlp` retunes the exporter live — the export-side sample rate,
 * the batch size, the flush interval, the per-request timeout, and
 * the buffer bound — without restarting the bus. Only a bus that
 * enabled `otlp` can be revised; illegal values fail closed with the
 * live tuning and the audit log untouched; an identical (or empty)
 * revision is a no-op with no audit. In-flight buffered spans are
 * untouched: a revised sample rate governs from the next span
 * admission, a revised batch size from the next flush, a revised
 * buffer bound from the next admission's drop-oldest check, a revised
 * timeout from the next export POST, and a revised flush interval
 * rebuilds the flush timer at the new cadence from the next tick.
 * `endpoint`, `headers`, and `serviceName` are not revisable here.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { createServer, type Server, type IncomingMessage, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import { EventBus } from '../src/bus.ts';
import { OtlpSpanExporter, resolveOtlpOptions } from '../src/otlp.ts';
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

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/**
 * Stub OTLP/HTTP collector on 127.0.0.1. Records every POST body; the
 * exporter uses `agent: false`, so connections close after each
 * response and `close()` never hangs on pooled sockets.
 */
function startCollector(): {
  requests: string[];
  endpoint: Promise<string>;
  close: () => Promise<void>;
} {
  const requests: string[] = [];
  const server: Server = createServer((req: IncomingMessage, res: ServerResponse) => {
    let body = '';
    req.on('data', (chunk: Buffer) => {
      body += chunk.toString('utf8');
    });
    req.on('end', () => {
      requests.push(body);
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

function makeSpan(overrides: Partial<TraceSpan> = {}): TraceSpan {
  return {
    traceId: 'a'.repeat(32),
    spanId: 'b'.repeat(16),
    name: 'bus.publish',
    at: 1_700_000_000_000,
    durationMs: 5,
    attrs: { topic: 't.1', seq: 7 },
    ...overrides,
  };
}

describe('OTLP export revision (EB-299)', () => {
  it('setOtlp fails closed when otlp is not enabled', async () => {
    const plain = new EventBus();
    assert.throws(() => plain.setOtlp({ sampleRate: 0.5 }), RangeError);
    assert.equal(plain.getSubscriptionAudit({ action: 'otlp_revised' }).length, 0);

    // Trace alone is not export: the exporter was never built.
    const traced = new EventBus({ trace: { sampleRate: 1 } });
    assert.throws(() => traced.setOtlp({ batchSize: 10 }), RangeError);
    assert.equal(traced.getSubscriptionAudit({ action: 'otlp_revised' }).length, 0);
  });

  it('setOtlp rejects illegal values and leaves the live tuning untouched', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: { endpoint, sampleRate: 0, flushIntervalMs: 60_000 },
    });
    try {
      bus.subscribe('t', () => {});
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
        'nope',
        42,
      ];
      for (const revision of bad) {
        assert.throws(
          () => bus.setOtlp(revision as never),
          RangeError,
          `expected RangeError for ${JSON.stringify(revision)}`,
        );
      }
      // The failed calls changed nothing: the live sample rate is still
      // 0, so a publish admits no span for export.
      bus.publish('t', 'a');
      await flush();
      assert.equal(bus.getStats().otlp?.bufferedSpans, 0);
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_revised' }).length, 0);
    } finally {
      await collector.close();
    }
  });

  it('setOtlp with identical values or an empty revision is a no-op with no audit', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: { endpoint, sampleRate: 0, flushIntervalMs: 60_000 },
    });
    try {
      bus.subscribe('t', () => {});
      bus.setOtlp({ sampleRate: 0 });
      bus.setOtlp({ batchSize: 512 });
      bus.setOtlp({ flushIntervalMs: 60_000 });
      bus.setOtlp({ timeoutMs: 5000 });
      bus.setOtlp({ maxBuffered: 10000 });
      bus.setOtlp({});
      bus.setOtlp(null as never);
      assert.equal(bus.getSubscriptionAudit({ action: 'otlp_revised' }).length, 0);
      // The untouched tuning still governs: nothing is admitted.
      bus.publish('t', 'a');
      await flush();
      assert.equal(bus.getStats().otlp?.bufferedSpans, 0);
      // And the bus still revises afterwards.
      bus.setOtlp({ sampleRate: 1 });
      bus.publish('t', 'b');
      await flush();
      assert.ok((bus.getStats().otlp?.bufferedSpans ?? 0) > 0);
    } finally {
      await collector.close();
    }
  });

  it('sampleRate revision governs from the next span admission, audit carries old and new', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: { endpoint, sampleRate: 1, flushIntervalMs: 60_000 },
    });
    try {
      bus.subscribe('t', () => {});
      bus.publish('t', 'a');
      await flush();
      const buffered = bus.getStats().otlp?.bufferedSpans ?? 0;
      assert.ok(buffered > 0, 'expected spans buffered before the revision');
      // In-flight buffered spans are retained by the retune itself.
      bus.setOtlp({ sampleRate: 0 });
      assert.equal(bus.getStats().otlp?.bufferedSpans, buffered);
      // The next admission samples at the live rate 0: nothing new
      // is admitted, the retained buffer is untouched.
      bus.publish('t', 'b');
      await flush();
      assert.equal(bus.getStats().otlp?.bufferedSpans, buffered);
      const audit = bus.getSubscriptionAudit({ action: 'otlp_revised' });
      assert.equal(audit.length, 1);
      assert.deepEqual(audit[0].detail, {
        sampleRate: 0,
        oldSampleRate: 1,
        batchSize: 512,
        oldBatchSize: 512,
        flushIntervalMs: 60_000,
        oldFlushIntervalMs: 60_000,
        timeoutMs: 5000,
        oldTimeoutMs: 5000,
        maxBuffered: 10000,
        oldMaxBuffered: 10000,
      });
    } finally {
      await collector.close();
    }
  });

  it('partial revision moves only the provided field', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: { endpoint, flushIntervalMs: 60_000 },
    });
    try {
      bus.setOtlp({ timeoutMs: 2500 });
      const audit = bus.getSubscriptionAudit({ action: 'otlp_revised' });
      assert.equal(audit.length, 1);
      assert.deepEqual(audit[0].detail, {
        sampleRate: 1,
        oldSampleRate: 1,
        batchSize: 512,
        oldBatchSize: 512,
        flushIntervalMs: 60_000,
        oldFlushIntervalMs: 60_000,
        timeoutMs: 2500,
        oldTimeoutMs: 5000,
        maxBuffered: 10000,
        oldMaxBuffered: 10000,
      });
    } finally {
      await collector.close();
    }
  });

  it('batchSize revision governs from the next flush', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: { endpoint, batchSize: 2, flushIntervalMs: 60_000 },
    });
    try {
      bus.subscribe('t', () => {});
      // One publish fans out several spans, so the buffer reaches the
      // live batch size of 2 and auto-flushes to the collector. Wait on
      // the exporter's own counter (not the collector's request log):
      // the collector records the POST before the exporter counts the
      // batch exported on the response.
      bus.publish('t', 'a');
      await flush();
      await waitFor(() => (bus.getStats().otlp?.exportedSpans ?? 0) > 0);
      assert.ok(collector.requests.length > 0);
      const posted = collector.requests.length;
      // Widen the batch: the next publish buffers instead of flushing —
      // no new POST can fire (the timer is 60s away, the batch is
      // nowhere near full).
      bus.setOtlp({ batchSize: 1000 });
      bus.publish('t', 'b');
      await flush();
      assert.ok((bus.getStats().otlp?.bufferedSpans ?? 0) > 0);
      await sleep(100);
      assert.equal(collector.requests.length, posted);
    } finally {
      await collector.close();
    }
  });

  it('maxBuffered revision drops the oldest from the next admission, buffer retained until then', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: { endpoint, flushIntervalMs: 60_000 },
    });
    try {
      bus.subscribe('t', () => {});
      bus.publish('t', 'a');
      await flush();
      const buffered = bus.getStats().otlp?.bufferedSpans ?? 0;
      assert.ok(buffered > 2, 'expected more than two spans buffered before the revision');
      // The retune alone trims nothing: the in-flight buffer is retained.
      bus.setOtlp({ maxBuffered: 2 });
      assert.equal(bus.getStats().otlp?.bufferedSpans, buffered);
      // The next admission's drop-oldest check enforces the live bound.
      bus.publish('t', 'b');
      await flush();
      assert.equal(bus.getStats().otlp?.bufferedSpans, 2);
      assert.ok((bus.getStats().otlp?.droppedSpans ?? 0) > 0);
    } finally {
      await collector.close();
    }
  });

  it('exporter retune moves the live values; current() reports them', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const exporter = new OtlpSpanExporter(
      resolveOtlpOptions({ endpoint, sampleRate: 0, flushIntervalMs: 60_000 })!,
    );
    try {
      assert.equal(exporter.current().sampleRate, 0);
      assert.equal(exporter.current().batchSize, 512);
      assert.equal(exporter.current().flushIntervalMs, 60_000);
      for (let i = 0; i < 3; i++) exporter.exportSpan(makeSpan());
      assert.equal(exporter.getStats().bufferedSpans, 0);
      exporter.retune(resolveOtlpOptions({ endpoint, sampleRate: 1, batchSize: 100, flushIntervalMs: 60_000 })!);
      assert.equal(exporter.current().sampleRate, 1);
      assert.equal(exporter.current().batchSize, 100);
      exporter.exportSpan(makeSpan());
      assert.equal(exporter.getStats().bufferedSpans, 1);
    } finally {
      await exporter.shutdown();
      await collector.close();
    }
  });

  it('flushIntervalMs revision rebuilds the timer: buffered spans flush at the new cadence, none lost', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: { endpoint, flushIntervalMs: 60_000 },
    });
    try {
      bus.subscribe('t', () => {});
      bus.publish('t', 'a');
      await flush();
      const buffered = bus.getStats().otlp?.bufferedSpans ?? 0;
      assert.ok(buffered > 0, 'expected spans buffered before the revision');
      // Nothing flushes on its own at the 60s cadence.
      await sleep(150);
      assert.equal(collector.requests.length, 0);
      // Tighten the interval: the retune itself keeps the buffer, the
      // new timer drains it within a few ticks — nothing is lost or
      // re-sent, the batch is exported exactly once.
      bus.setOtlp({ flushIntervalMs: 50 });
      assert.equal(bus.getStats().otlp?.bufferedSpans, buffered);
      await waitFor(() => (bus.getStats().otlp?.exportedSpans ?? 0) >= buffered);
      assert.equal(bus.getStats().otlp?.bufferedSpans, 0);
      assert.ok(collector.requests.length > 0);
      const audit = bus.getSubscriptionAudit({ action: 'otlp_revised' });
      assert.equal(audit.length, 1);
      assert.deepEqual(audit[0].detail, {
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
    } finally {
      await collector.close();
    }
  });

  it('widening flushIntervalMs delays the timer flush; a later tightening re-arms it', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const bus = new EventBus({
      trace: { sampleRate: 1 },
      otlp: { endpoint, flushIntervalMs: 50 },
    });
    try {
      bus.subscribe('t', () => {});
      bus.publish('t', 'a');
      await flush();
      assert.ok((bus.getStats().otlp?.bufferedSpans ?? 0) > 0);
      // Widen to 60s: the 50ms timer is gone, so no flush fires now.
      bus.setOtlp({ flushIntervalMs: 60_000 });
      await sleep(200);
      assert.equal(collector.requests.length, 0);
      assert.ok((bus.getStats().otlp?.bufferedSpans ?? 0) > 0);
      // Tighten again: the rebuilt timer drains the retained buffer.
      bus.setOtlp({ flushIntervalMs: 50 });
      await waitFor(() => (bus.getStats().otlp?.exportedSpans ?? 0) > 0);
      assert.equal(bus.getStats().otlp?.bufferedSpans, 0);
    } finally {
      await collector.close();
    }
  });

  it('exporter retune moves flushIntervalMs; current() reports it, and a retune after shutdown does not re-arm the timer', async () => {
    const collector = startCollector();
    const endpoint = await collector.endpoint;
    const exporter = new OtlpSpanExporter(
      resolveOtlpOptions({ endpoint, flushIntervalMs: 60_000 })!,
    );
    try {
      assert.equal(exporter.current().flushIntervalMs, 60_000);
      exporter.retune(
        resolveOtlpOptions({ endpoint, flushIntervalMs: 25 })!,
      );
      assert.equal(exporter.current().flushIntervalMs, 25);
      exporter.exportSpan(makeSpan());
      await waitFor(() => exporter.getStats().exportedSpans > 0);
      // A stopped exporter stays stopped: retuning the interval after
      // shutdown moves the value but arms no timer.
      await exporter.shutdown();
      exporter.retune(
        resolveOtlpOptions({ endpoint, flushIntervalMs: 50 })!,
      );
      assert.equal(exporter.current().flushIntervalMs, 50);
      exporter.exportSpan(makeSpan());
      await sleep(150);
      assert.equal(exporter.getStats().bufferedSpans, 1);
      assert.equal(exporter.getStats().exportedSpans, 1);
    } finally {
      await exporter.shutdown();
      await collector.close();
    }
  });
});
