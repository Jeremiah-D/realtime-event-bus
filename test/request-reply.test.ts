import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import {
  RPC_REPLY_TOPIC,
  RequestTimeoutError,
  cleanCorrelationId,
  randomCorrelationId,
  routeRpcResponse,
} from '../src/rpc.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

async function settled(promise: Promise<unknown>, timeoutMs = 2000): Promise<unknown> {
  return Promise.race([
    promise,
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error('test harness timed out waiting for promise')), timeoutMs),
    ),
  ]);
}

test('rpc helpers: cleaning, id generation, pure routing decision', () => {
  assert.equal(cleanCorrelationId('abc'), 'abc');
  assert.equal(cleanCorrelationId(''), undefined);
  assert.equal(cleanCorrelationId(42), undefined);
  assert.equal(cleanCorrelationId('x'.repeat(1025)), undefined);
  const id = randomCorrelationId();
  assert.match(id, /^[0-9a-f]{32}$/);
  assert.notEqual(randomCorrelationId(), id);
  const pending = new Map([['a', {}]]);
  const settledMap = new Map([['b', Date.now()]]);
  assert.deepEqual(routeRpcResponse('a', pending, settledMap), { kind: 'settle' });
  assert.deepEqual(routeRpcResponse('b', pending, settledMap), { kind: 'discard-late' });
  assert.deepEqual(routeRpcResponse('zzz', pending, settledMap), { kind: 'discard-unknown' });
  assert.deepEqual(routeRpcResponse(undefined, pending, settledMap), { kind: 'discard-unknown' });
});

test('requestResponse: happy path resolves with the responder payload', async () => {
  const bus = new EventBus();
  try {
    let seenRequest: { correlationId?: string; replyTo?: string } | undefined;
    bus.subscribe('rpc.echo', (msg) => {
      seenRequest = { correlationId: msg.correlationId, replyTo: msg.replyTo };
      bus.reply(msg, { echo: msg.payload });
    });
    const result = await settled(bus.requestResponse('rpc.echo', { n: 1 }, { timeoutMs: 2000 }));
    assert.deepEqual(result, { echo: { n: 1 } });
    assert.ok(seenRequest?.correlationId, 'request carried a correlationId');
    assert.equal(seenRequest?.replyTo, RPC_REPLY_TOPIC);
    const stats = bus.getStats().rpc;
    assert.equal(stats.requests, 1);
    assert.equal(stats.responses, 1);
    assert.equal(stats.timeouts, 0);
    assert.equal(stats.pending, 0);
  } finally {
    await bus.close();
  }
});

test('requestResponse: explicit correlationId is honored end to end', async () => {
  const bus = new EventBus();
  try {
    let seenId: string | undefined;
    bus.subscribe('rpc.id', (msg) => {
      seenId = msg.correlationId;
      bus.reply(msg, 'ok');
    });
    const result = await settled(
      bus.requestResponse('rpc.id', {}, { timeoutMs: 2000, correlationId: 'order-42' }),
    );
    assert.equal(result, 'ok');
    assert.equal(seenId, 'order-42');
  } finally {
    await bus.close();
  }
});

test('requestResponse: duplicate pending correlationId throws RangeError', async () => {
  const bus = new EventBus();
  try {
    const first = bus.requestResponse('rpc.never', {}, { timeoutMs: 500, correlationId: 'dup' });
    // Attach a no-op catch so the eventual timeout rejection is observed.
    void first.catch(() => {});
    assert.throws(
      () => bus.requestResponse('rpc.never', {}, { timeoutMs: 500, correlationId: 'dup' }),
      RangeError,
    );
    await assert.rejects(() => settled(first), RequestTimeoutError);
  } finally {
    await bus.close();
  }
});

test('requestResponse: timeout rejects with RequestTimeoutError carrying id + timeoutMs', async () => {
  const bus = new EventBus();
  try {
    const err = await bus
      .requestResponse('rpc.nobody-home', {}, { timeoutMs: 50 })
      .then(
        () => { throw new Error('should have timed out'); },
        (e: unknown) => e,
      );
    assert.ok(err instanceof RequestTimeoutError);
    assert.equal(err.name, 'RequestTimeoutError');
    assert.equal(err.timeoutMs, 50);
    assert.match(err.correlationId, /^[0-9a-f]{32}$/);
    assert.equal(bus.getStats().rpc.timeouts, 1);
    assert.equal(bus.getStats().rpc.pending, 0);
  } finally {
    await bus.close();
  }
});

test('requestResponse: non-positive timeoutMs throws RangeError', async () => {
  const bus = new EventBus();
  try {
    assert.throws(() => bus.requestResponse('t', {}, { timeoutMs: 0 }), RangeError);
    assert.throws(() => bus.requestResponse('t', {}, { timeoutMs: -5 }), RangeError);
    assert.throws(() => bus.requestResponse('t', {}, { timeoutMs: Number.NaN }), RangeError);
  } finally {
    await bus.close();
  }
});

test('requestResponse: duplicate responses — first wins, second discarded by correlationId', async () => {
  const bus = new EventBus();
  try {
    bus.subscribe('rpc.dupe', (msg) => {
      bus.reply(msg, 'first');
      bus.reply(msg, 'second');
    });
    const result = await settled(bus.requestResponse('rpc.dupe', {}, { timeoutMs: 2000 }));
    assert.equal(result, 'first');
    await flush();
    const stats = bus.getStats().rpc;
    assert.equal(stats.responses, 1);
    assert.equal(stats.discardedResponses, 1);
  } finally {
    await bus.close();
  }
});

test('requestResponse: late response after timeout is discarded, never settles twice', async () => {
  const bus = new EventBus();
  try {
    let captured: { correlationId?: string; replyTo?: string } | undefined;
    bus.subscribe('rpc.slow', (msg) => {
      captured = { correlationId: msg.correlationId, replyTo: msg.replyTo };
      // Deliberately slow: the requester times out at 50ms, we answer at 150ms.
      setTimeout(() => bus.reply(msg, 'too-late'), 150);
    });
    const promise = bus.requestResponse('rpc.slow', {}, { timeoutMs: 50 });
    await assert.rejects(() => settled(promise), RequestTimeoutError);
    // Let the late response arrive and be routed.
    await new Promise((resolve) => setTimeout(resolve, 300));
    const stats = bus.getStats().rpc;
    assert.equal(stats.timeouts, 1);
    assert.equal(stats.responses, 0);
    assert.equal(stats.discardedResponses, 1);
    assert.ok(captured?.correlationId, 'late response still carried the correlationId');
  } finally {
    await bus.close();
  }
});

test('requestResponse: rejected request publish fails fast, not via timeout', async () => {
  const bus = new EventBus();
  try {
    bus.setTopicSchema('rpc.strict', (payload: unknown) => typeof payload === 'object' && payload !== null && 'ok' in (payload as object));
    const started = Date.now();
    await assert.rejects(
      () => settled(bus.requestResponse('rpc.strict', 'not-an-object', { timeoutMs: 5000 })),
      /not admitted/,
    );
    assert.ok(Date.now() - started < 1000, 'rejected fast instead of waiting out the timeout');
    assert.equal(bus.getStats().rpc.timeouts, 0);
  } finally {
    await bus.close();
  }
});

test('reply: throws RangeError on a non-RPC message', async () => {
  const bus = new EventBus();
  try {
    assert.throws(() => bus.reply({ topic: 't', payload: 1, seq: 1 } as never, 'x'), RangeError);
    assert.throws(
      () => bus.reply({ topic: 't', payload: 1, seq: 1, replyTo: '_bus.replies' } as never, 'x'),
      RangeError,
    );
  } finally {
    await bus.close();
  }
});

test('requestResponse: publish() supports correlationId + replyTo directly', async () => {
  const bus = new EventBus();
  try {
    const seen: Array<{ correlationId?: string; replyTo?: string }> = [];
    bus.subscribe('rpc.manual', (msg) => {
      seen.push({ correlationId: msg.correlationId, replyTo: msg.replyTo });
    });
    bus.publish('rpc.manual', { a: 1 }, { correlationId: 'manual-1', replyTo: 'rpc.answers' });
    // Lenient absent-rule, like messageId: empty values are not stamped.
    bus.publish('rpc.manual', { a: 2 }, { correlationId: '', replyTo: '' });
    await flush();
    assert.deepEqual(seen, [
      { correlationId: 'manual-1', replyTo: 'rpc.answers' },
      { correlationId: undefined, replyTo: undefined },
    ]);
  } finally {
    await bus.close();
  }
});

test('requestResponse: composes with reliable (at-least-once) reply delivery', async () => {
  const bus = new EventBus();
  try {
    let deliveries = 0;
    bus.subscribe('rpc.rel', (msg) => {
      deliveries += 1;
      bus.reply(msg, { n: deliveries });
    });
    const result = await settled(
      bus.requestResponse('rpc.rel', {}, { timeoutMs: 2000, reliable: true }),
    );
    assert.deepEqual(result, { n: 1 });
    // The router acked the response: no redelivery storm followed.
    await new Promise((resolve) => setTimeout(resolve, 300));
    assert.equal(deliveries, 1);
    assert.equal(bus.getStats().rpc.discardedResponses, 0);
  } finally {
    await bus.close();
  }
});

test('requestResponse: close() rejects pending requests with BusClosedError', async () => {
  const bus = new EventBus();
  const promise = bus.requestResponse('rpc.open', {}, { timeoutMs: 60_000 });
  const rejection = promise.then(
    () => 'resolved',
    (e: unknown) => (e as Error).name,
  );
  await bus.close();
  assert.equal(await settled(rejection), 'BusClosedError');
  assert.equal(bus.getStats().rpc.pending, 0);
});

test('requestResponse: publishOptions passthrough reaches the request', async () => {
  const bus = new EventBus();
  try {
    let seenHeaders: Record<string, string> | undefined;
    bus.subscribe('rpc.hdrs', (msg) => {
      seenHeaders = msg.headers;
      bus.reply(msg, 'ok');
    });
    const result = await settled(
      bus.requestResponse(
        'rpc.hdrs',
        {},
        { timeoutMs: 2000, publishOptions: { headers: { tenant: 't1' } } },
      ),
    );
    assert.equal(result, 'ok');
    assert.deepEqual(seenHeaders, { tenant: 't1' });
  } finally {
    await bus.close();
  }
});
