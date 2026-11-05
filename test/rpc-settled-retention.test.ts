import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import {
  RPC_REPLY_TOPIC,
  RPC_SETTLED_RING_SIZE,
  RPC_SETTLED_TTL_MS,
  RequestTimeoutError,
  routeRpcResponse,
} from '../src/rpc.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** The bus's private settled ring, read-only for assertions (repo precedent). */
function settledRing(bus: EventBus): Map<string, number> {
  return (bus as unknown as { rpcSettled: Map<string, number> }).rpcSettled;
}

/** Settles one correlation id by timing out a request against a fake clock. */
async function timeoutSettle(
  bus: EventBus,
  correlationId: string,
  now: () => number,
  timeoutMs = 20,
): Promise<void> {
  await assert.rejects(
    bus.requestResponse('rpc.never', {}, { timeoutMs, correlationId, now }),
    RequestTimeoutError,
  );
}

test('defaults: getter returns the module constants before any revision', async () => {
  const bus = new EventBus();
  try {
    assert.deepEqual(bus.getRpcSettledRetention(), {
      ringSize: RPC_SETTLED_RING_SIZE,
      ttlMs: RPC_SETTLED_TTL_MS,
    });
    assert.equal(RPC_SETTLED_RING_SIZE, 4096);
    assert.equal(RPC_SETTLED_TTL_MS, 5 * 60 * 1000);
  } finally {
    await bus.close();
  }
});

test('partial revision moves only ringSize; ttlMs keeps its live value', async () => {
  const bus = new EventBus();
  try {
    bus.setRpcSettledRetention({ ringSize: 128 });
    assert.deepEqual(bus.getRpcSettledRetention(), { ringSize: 128, ttlMs: RPC_SETTLED_TTL_MS });
    bus.setRpcSettledRetention({ ringSize: 256 });
    assert.deepEqual(bus.getRpcSettledRetention(), { ringSize: 256, ttlMs: RPC_SETTLED_TTL_MS });
  } finally {
    await bus.close();
  }
});

test('partial revision moves only ttlMs; ringSize keeps its live value', async () => {
  const bus = new EventBus();
  try {
    bus.setRpcSettledRetention({ ttlMs: 60_000 });
    assert.deepEqual(bus.getRpcSettledRetention(), { ringSize: RPC_SETTLED_RING_SIZE, ttlMs: 60_000 });
  } finally {
    await bus.close();
  }
});

test('illegal ringSize throws RangeError and leaves live retention untouched', async () => {
  const bus = new EventBus();
  try {
    const before = bus.getRpcSettledRetention();
    for (const ringSize of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, 'x']) {
      assert.throws(
        () => bus.setRpcSettledRetention({ ringSize: ringSize as unknown as number }),
        RangeError,
      );
      assert.deepEqual(bus.getRpcSettledRetention(), before);
    }
    assert.equal(bus.getSubscriptionAudit({ action: 'rpc_settled_retention_revised' }).length, 0);
  } finally {
    await bus.close();
  }
});

test('illegal ttlMs throws RangeError and leaves live retention untouched', async () => {
  const bus = new EventBus();
  try {
    bus.setRpcSettledRetention({ ringSize: 64 });
    const before = bus.getRpcSettledRetention();
    for (const ttlMs of [0, -100, Number.NaN, Number.POSITIVE_INFINITY, 'x']) {
      assert.throws(
        () => bus.setRpcSettledRetention({ ttlMs: ttlMs as unknown as number }),
        RangeError,
      );
      assert.deepEqual(bus.getRpcSettledRetention(), before);
    }
    assert.equal(bus.getSubscriptionAudit({ action: 'rpc_settled_retention_revised' }).length, 1);
  } finally {
    await bus.close();
  }
});

test('non-object revision throws; null, undefined and {} are silent no-ops with no audit', async () => {
  const bus = new EventBus();
  try {
    assert.throws(() => bus.setRpcSettledRetention('nope' as unknown as { ringSize: number }), RangeError);
    assert.throws(() => bus.setRpcSettledRetention([] as unknown as { ringSize: number }), RangeError);
    assert.throws(() => bus.setRpcSettledRetention(42 as unknown as { ringSize: number }), RangeError);
    bus.setRpcSettledRetention(null as unknown as { ringSize: number });
    bus.setRpcSettledRetention(undefined as unknown as { ringSize: number });
    bus.setRpcSettledRetention({});
    assert.deepEqual(bus.getRpcSettledRetention(), {
      ringSize: RPC_SETTLED_RING_SIZE,
      ttlMs: RPC_SETTLED_TTL_MS,
    });
    assert.equal(bus.getSubscriptionAudit({ action: 'rpc_settled_retention_revised' }).length, 0);
  } finally {
    await bus.close();
  }
});

test('re-affirming the current retention is a no-op with no audit', async () => {
  const bus = new EventBus();
  try {
    bus.setRpcSettledRetention({ ringSize: 100, ttlMs: 60_000 });
    assert.equal(bus.getSubscriptionAudit({ action: 'rpc_settled_retention_revised' }).length, 1);
    bus.setRpcSettledRetention({ ringSize: 100, ttlMs: 60_000 });
    bus.setRpcSettledRetention({ ringSize: 100 });
    bus.setRpcSettledRetention({ ttlMs: 60_000 });
    assert.equal(bus.getSubscriptionAudit({ action: 'rpc_settled_retention_revised' }).length, 1);
  } finally {
    await bus.close();
  }
});

test('audit pins previous/current exactly and records the actor', async () => {
  const bus = new EventBus();
  try {
    bus.setRpcSettledRetention({ ringSize: 100 }, 'ops');
    bus.setRpcSettledRetention({ ringSize: 100, ttlMs: 60_000 }, 'ops');
    const audit = bus.getSubscriptionAudit({ action: 'rpc_settled_retention_revised' });
    assert.equal(audit.length, 2);
    assert.deepEqual(audit[0].detail, {
      previous: { ringSize: RPC_SETTLED_RING_SIZE, ttlMs: RPC_SETTLED_TTL_MS },
      current: { ringSize: 100, ttlMs: RPC_SETTLED_TTL_MS },
    });
    assert.deepEqual(audit[1].detail, {
      previous: { ringSize: 100, ttlMs: RPC_SETTLED_TTL_MS },
      current: { ringSize: 100, ttlMs: 60_000 },
    });
    assert.equal(audit[0].actor, 'ops');
    assert.throws(() => bus.setRpcSettledRetention({ ringSize: 7 }, ''), RangeError);
  } finally {
    await bus.close();
  }
});

test('ring tighten evicts the oldest entries at the next prune', async () => {
  const bus = new EventBus();
  try {
    let nowMs = 1000;
    const now = () => nowMs;
    bus.setRpcSettledRetention({ ringSize: 3 });
    await timeoutSettle(bus, 'ring-a', now);
    await timeoutSettle(bus, 'ring-b', now);
    await timeoutSettle(bus, 'ring-c', now);
    assert.equal(settledRing(bus).size, 3);
    assert.ok(settledRing(bus).has('ring-a'));
    await timeoutSettle(bus, 'ring-d', now);
    // The fourth settle pruned under the tightened ring: oldest first.
    assert.equal(settledRing(bus).size, 3);
    assert.ok(!settledRing(bus).has('ring-a'));
    assert.ok(settledRing(bus).has('ring-b'));
    assert.ok(settledRing(bus).has('ring-d'));
    // The evicted id is forgotten: its straggler drops as unknown.
    const pending = new Map<string, unknown>();
    assert.deepEqual(routeRpcResponse('ring-a', pending, settledRing(bus)), {
      kind: 'discard-unknown',
    });
    assert.deepEqual(routeRpcResponse('ring-b', pending, settledRing(bus)), {
      kind: 'discard-late',
    });
    // A late reply for the evicted id is still discarded, never delivered.
    bus.publish(RPC_REPLY_TOPIC, { late: true }, { correlationId: 'ring-a' });
    await flush();
    assert.equal(bus.getStats().rpc.discardedResponses, 1);
  } finally {
    await bus.close();
  }
});

test('ring widen keeps late duplicates recognizable instead of unknown', async () => {
  const bus = new EventBus();
  try {
    let nowMs = 1000;
    const now = () => nowMs;
    bus.setRpcSettledRetention({ ringSize: 2 });
    await timeoutSettle(bus, 'wide-a', now);
    await timeoutSettle(bus, 'wide-b', now);
    bus.setRpcSettledRetention({ ringSize: 10 });
    await timeoutSettle(bus, 'wide-c', now);
    await timeoutSettle(bus, 'wide-d', now);
    await timeoutSettle(bus, 'wide-e', now);
    await timeoutSettle(bus, 'wide-f', now);
    // Six settled ids fit the widened ring: the oldest survives.
    assert.equal(settledRing(bus).size, 6);
    assert.ok(settledRing(bus).has('wide-a'));
    const pending = new Map<string, unknown>();
    assert.deepEqual(routeRpcResponse('wide-a', pending, settledRing(bus)), {
      kind: 'discard-late',
    });
    // End to end: the late duplicate is discarded, not delivered twice.
    bus.publish(RPC_REPLY_TOPIC, { late: true }, { correlationId: 'wide-a' });
    await flush();
    assert.equal(bus.getStats().rpc.discardedResponses, 1);
  } finally {
    await bus.close();
  }
});

test('TTL tighten forgets entries at the next read; settledAt is never rewritten', async () => {
  const bus = new EventBus();
  try {
    let nowMs = 1000;
    const now = () => nowMs;
    bus.setRpcSettledRetention({ ttlMs: 100 });
    await timeoutSettle(bus, 'ttl-a', now);
    assert.equal(settledRing(bus).get('ttl-a'), 1000);
    // Advancing the clock past the tightened TTL, then settling another
    // id, prunes the stale entry at the next read.
    nowMs = 2000;
    await timeoutSettle(bus, 'ttl-b', now);
    assert.ok(!settledRing(bus).has('ttl-a'));
    assert.ok(settledRing(bus).has('ttl-b'));
    assert.equal(settledRing(bus).get('ttl-b'), 2000);
    const pending = new Map<string, unknown>();
    assert.deepEqual(routeRpcResponse('ttl-a', pending, settledRing(bus)), {
      kind: 'discard-unknown',
    });
    assert.deepEqual(routeRpcResponse('ttl-b', pending, settledRing(bus)), {
      kind: 'discard-late',
    });
  } finally {
    await bus.close();
  }
});

test('interleaved revisions and settles keep the ring within the live bound', async () => {
  const bus = new EventBus();
  try {
    let nowMs = 1000;
    const now = () => nowMs;
    for (let i = 0; i < 12; i++) {
      const ringSize = 2 + (i % 3);
      bus.setRpcSettledRetention({ ringSize });
      await timeoutSettle(bus, `race-${i}`, now);
      nowMs += 10;
      assert.ok(
        settledRing(bus).size <= bus.getRpcSettledRetention().ringSize,
        `ring exceeded live bound at step ${i}`,
      );
    }
    assert.deepEqual(bus.getRpcSettledRetention().ringSize, 2 + (11 % 3));
  } finally {
    await bus.close();
  }
});
