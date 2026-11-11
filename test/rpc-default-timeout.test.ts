import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { RequestTimeoutError } from '../src/rpc.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** Captures a promise's rejection as a value (never throws). */
const captureError = (p: Promise<unknown>): Promise<unknown> =>
  p.then(
    () => new Error('expected the promise to reject'),
    (e: unknown) => e,
  );

test('the factory default RPC timeout is 5000', () => {
  const bus = new EventBus();
  assert.equal(bus.getRpcDefaultTimeoutMs(), 5000);
});

test('a bare requestResponse with no opts times out at the 5000ms factory default', async () => {
  const bus = new EventBus();
  try {
    const started = Date.now();
    const err = await captureError(bus.requestResponse('rpc.nobody-home', {}));
    const elapsed = Date.now() - started;
    assert.ok(err instanceof RequestTimeoutError);
    assert.equal((err as RequestTimeoutError).timeoutMs, 5000);
    assert.ok(
      elapsed >= 4900 && elapsed < 8000,
      `a bare call must arm at the 5000ms factory default (elapsed ${elapsed}ms)`,
    );
    assert.equal(bus.getStats().rpc.timeouts, 1);
    assert.equal(bus.getStats().rpc.pending, 0);
  } finally {
    await bus.close();
  }
});

test('a bare requestResponse arms at a revised live default, exactly', async () => {
  const bus = new EventBus();
  try {
    bus.setRpcDefaultTimeoutMs(120);
    const started = Date.now();
    const err = await captureError(bus.requestResponse('rpc.nobody-home', {}));
    const elapsed = Date.now() - started;
    assert.ok(err instanceof RequestTimeoutError);
    assert.equal((err as RequestTimeoutError).timeoutMs, 120);
    assert.ok(
      elapsed >= 100 && elapsed < 2000,
      `a bare call must arm at the revised 120ms default (elapsed ${elapsed}ms)`,
    );
    assert.equal(bus.getStats().rpc.timeouts, 1);
  } finally {
    await bus.close();
  }
});

test('a bare requestResponse with an empty opts object also falls back to the live default', async () => {
  const bus = new EventBus();
  try {
    bus.setRpcDefaultTimeoutMs(60);
    const err = await captureError(bus.requestResponse('rpc.nobody-home', {}, {}));
    assert.ok(err instanceof RequestTimeoutError);
    assert.equal((err as RequestTimeoutError).timeoutMs, 60);
  } finally {
    await bus.close();
  }
});

test('revising the default to 30000 keeps a bare call pending well past the old default', async () => {
  const bus = new EventBus();
  try {
    bus.setRpcDefaultTimeoutMs(30_000);
    assert.equal(bus.getRpcDefaultTimeoutMs(), 30_000);
    // Waiting the full 30s is not what this test is for: the 120ms
    // test above proves bare calls arm at the live default exactly.
    // Here the widened default must simply not time out early.
    const pending = bus.requestResponse('rpc.nobody-home', {});
    void pending.catch(() => {});
    await sleep(600);
    await flush();
    assert.equal(bus.getStats().rpc.pending, 1, 'the bare call must still be waiting at 600ms');
    assert.equal(bus.getStats().rpc.timeouts, 0);
  } finally {
    await bus.close();
  }
});

test('a per-call timeoutMs wins over the bus default in both directions', async () => {
  const bus = new EventBus();
  try {
    // Explicit 60 beats a 30s default: the call times out fast.
    bus.setRpcDefaultTimeoutMs(30_000);
    const started = Date.now();
    const fastErr = await captureError(
      bus.requestResponse('rpc.nobody-home', {}, { timeoutMs: 60 }),
    );
    const fastElapsed = Date.now() - started;
    assert.ok(fastErr instanceof RequestTimeoutError);
    assert.equal((fastErr as RequestTimeoutError).timeoutMs, 60);
    assert.ok(
      fastElapsed >= 50 && fastElapsed < 2000,
      `an explicit 60ms must beat a widened default (elapsed ${fastElapsed}ms)`,
    );

    // Explicit 5000 beats a 60ms default: no timeout inside 300ms.
    bus.setRpcDefaultTimeoutMs(60);
    const pending = bus.requestResponse('rpc.nobody-home', {}, { timeoutMs: 5000 });
    void pending.catch(() => {});
    await sleep(300);
    await flush();
    assert.equal(bus.getStats().rpc.pending, 1, 'an explicit 5000ms must beat a tightened default');
    assert.equal(bus.getStats().rpc.timeouts, 1, 'only the fast call timed out so far');
  } finally {
    await bus.close();
  }
});

test('a pending request keeps its armed timer after the default is revised', async () => {
  const bus = new EventBus();
  try {
    bus.setRpcDefaultTimeoutMs(60);
    const started = Date.now();
    const pending = bus.requestResponse('rpc.nobody-home', {});
    // Widen the bus default far past the armed 60ms timer: the
    // in-flight request must still time out on its original deadline.
    bus.setRpcDefaultTimeoutMs(30_000);
    const err = await captureError(pending);
    const elapsed = Date.now() - started;
    assert.ok(err instanceof RequestTimeoutError);
    assert.equal((err as RequestTimeoutError).timeoutMs, 60);
    assert.ok(
      elapsed >= 50 && elapsed < 2000,
      `the pending request must keep its original deadline (elapsed ${elapsed}ms)`,
    );
    assert.equal(bus.getStats().rpc.timeouts, 1);
  } finally {
    await bus.close();
  }
});

test('setRpcDefaultTimeoutMs rejects illegal values fail-closed', () => {
  const bus = new EventBus();
  bus.setRpcDefaultTimeoutMs(30_000);
  for (const timeoutMs of [0, -5, Number.NaN, Number.POSITIVE_INFINITY]) {
    assert.throws(() => bus.setRpcDefaultTimeoutMs(timeoutMs), RangeError, `timeoutMs=${timeoutMs}`);
  }
  assert.throws(() => bus.setRpcDefaultTimeoutMs('5000' as never), RangeError);
  assert.throws(() => bus.setRpcDefaultTimeoutMs(100, ''), RangeError, 'an illegal actor throws');
  // An illegal per-call value also throws and leaves the default alone.
  assert.throws(() => bus.requestResponse('t', {}, { timeoutMs: 0 }), RangeError);
  assert.throws(() => bus.requestResponse('t', {}, { timeoutMs: Number.NaN }), RangeError);
  assert.throws(() => bus.requestResponse('t', {}, { timeoutMs: '5000' as never }), RangeError);
  // The live default and the audit log are untouched: only the one
  // valid setup revision is recorded.
  assert.equal(bus.getRpcDefaultTimeoutMs(), 30_000);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'rpc_default_timeout_revised' }).length,
    1,
  );
});

test('revising to the current default is a no-op with no audit', () => {
  const bus = new EventBus();
  bus.setRpcDefaultTimeoutMs(5000);
  assert.equal(bus.getSubscriptionAudit({ action: 'rpc_default_timeout_revised' }).length, 0);
  bus.setRpcDefaultTimeoutMs(30_000);
  bus.setRpcDefaultTimeoutMs(30_000);
  const audit = bus.getSubscriptionAudit({ action: 'rpc_default_timeout_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { previous_timeout_ms: 5000, timeout_ms: 30_000 });
});

test('setRpcDefaultTimeoutMs audits each revision with its old and new value and actor', () => {
  const bus = new EventBus();
  bus.setRpcDefaultTimeoutMs(30_000, 'ops');
  bus.setRpcDefaultTimeoutMs(5000, 'ops');
  const events = bus.getSubscriptionAudit({ action: 'rpc_default_timeout_revised' });
  assert.equal(events.length, 2);
  assert.equal(events[0].actor, 'ops');
  assert.deepEqual(events[0].detail, { previous_timeout_ms: 5000, timeout_ms: 30_000 });
  assert.equal(events[1].actor, 'ops');
  assert.deepEqual(events[1].detail, { previous_timeout_ms: 30_000, timeout_ms: 5000 });
});

test('the getter follows the live value and setting 5000 restores the factory default', async () => {
  const bus = new EventBus();
  try {
    assert.equal(bus.getRpcDefaultTimeoutMs(), 5000);
    bus.setRpcDefaultTimeoutMs(60);
    assert.equal(bus.getRpcDefaultTimeoutMs(), 60);
    // Under the tightened default a bare call times out fast.
    const fastErr = await captureError(bus.requestResponse('rpc.nobody-home', {}));
    assert.ok(fastErr instanceof RequestTimeoutError);
    assert.equal((fastErr as RequestTimeoutError).timeoutMs, 60);
    // Restored: a bare call is still waiting after 300ms.
    bus.setRpcDefaultTimeoutMs(5000);
    assert.equal(bus.getRpcDefaultTimeoutMs(), 5000);
    const pending = bus.requestResponse('rpc.nobody-home', {});
    void pending.catch(() => {});
    await sleep(300);
    await flush();
    assert.equal(bus.getStats().rpc.pending, 1, 'a restored default must not time out early');
    assert.equal(bus.getStats().rpc.timeouts, 1, 'only the tightened-default call timed out');
  } finally {
    await bus.close();
  }
});

test('RequestTimeoutError carries the effective timeout, per-call or default', async () => {
  const bus = new EventBus();
  try {
    // Bare call under a 90ms default: the error carries 90.
    bus.setRpcDefaultTimeoutMs(90);
    const defaultErr = await captureError(bus.requestResponse('rpc.nobody-home', {}));
    assert.ok(defaultErr instanceof RequestTimeoutError);
    assert.equal((defaultErr as RequestTimeoutError).timeoutMs, 90);
    // Explicit 50 under a 30s default: the error carries 50.
    bus.setRpcDefaultTimeoutMs(30_000);
    const explicitErr = await captureError(
      bus.requestResponse('rpc.nobody-home', {}, { timeoutMs: 50 }),
    );
    assert.ok(explicitErr instanceof RequestTimeoutError);
    assert.equal((explicitErr as RequestTimeoutError).timeoutMs, 50);
  } finally {
    await bus.close();
  }
});

test('a bare call still resolves when the responder answers inside the default', async () => {
  const bus = new EventBus();
  try {
    bus.subscribe('rpc.echo', (msg) => {
      bus.reply(msg, { echo: msg.payload });
    });
    const result = await bus.requestResponse('rpc.echo', { n: 1 });
    assert.deepEqual(result, { echo: { n: 1 } });
    assert.equal(bus.getStats().rpc.responses, 1);
  } finally {
    await bus.close();
  }
});
