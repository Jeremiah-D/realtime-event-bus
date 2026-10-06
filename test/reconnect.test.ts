import test from 'node:test';
import assert from 'node:assert/strict';
import {
  ExponentialBackoff,
  ReconnectController,
} from '../src/reconnect.ts';

function waitFor(cond: () => boolean, timeoutMs = 10000): Promise<void> {
  return new Promise((resolve, reject) => {
    const start = Date.now();
    const id = setInterval(() => {
      if (cond()) {
        clearInterval(id);
        resolve();
      } else if (Date.now() - start > timeoutMs) {
        clearInterval(id);
        reject(new Error('timed out waiting for condition'));
      }
    }, 5);
  });
}

test('backoff delays grow exponentially within the jitter window', async () => {
  // Spy on the timers the controller schedules (not the test's own timers).
  const recorded: number[] = [];
  const originalSetTimeout = globalThis.setTimeout;
  let armed = false;
  (globalThis as unknown as { setTimeout: unknown }).setTimeout = (
    fn: (...args: unknown[]) => void,
    ms: number,
    ...rest: unknown[]
  ) => {
    if (armed) recorded.push(ms);
    return (originalSetTimeout as (...a: unknown[]) => unknown)(fn, ms, ...rest);
  };

  let gaveUp = 0;
  const controller = new ReconnectController({
    connectFn: async () => {
      throw new Error('network down');
    },
    baseDelayMs: 100,
    maxDelayMs: 100_000,
    jitterMs: 50,
    maxAttempts: 4,
    onGiveUp: () => {
      gaveUp += 1;
      armed = false;
    },
  });
  try {
    armed = true;
    controller.start();
    await waitFor(() => gaveUp === 1);
  } finally {
    (globalThis as unknown as { setTimeout: unknown }).setTimeout = originalSetTimeout;
    controller.stop();
  }

  // Attempts 1..3 fail, scheduling the waits before attempts 2, 3 and 4.
  assert.equal(recorded.length, 3);
  const ranges: Array<[number, number]> = [
    [200, 250],
    [400, 450],
    [800, 850],
  ];
  recorded.forEach((delay, i) => {
    const [lo, hi] = ranges[i];
    assert.ok(delay >= lo && delay < hi, `delay ${delay} outside [${lo}, ${hi})`);
  });
  assert.ok(recorded[0] < recorded[1] && recorded[1] < recorded[2], 'delays must increase');
});

test('maxAttempts exhaustion triggers onGiveUp and returns to idle', async () => {
  const giveUpCalls: number[] = [];
  const controller = new ReconnectController({
    connectFn: async () => {
      throw new Error('boom');
    },
    baseDelayMs: 5,
    jitterMs: 0,
    maxAttempts: 3,
    onGiveUp: (attempts) => giveUpCalls.push(attempts),
  });
  controller.start();
  await waitFor(() => giveUpCalls.length === 1);
  assert.deepEqual(giveUpCalls, [3]);
  assert.equal(controller.getState(), 'idle');
  assert.equal(controller.getAttempt(), 0);
  controller.stop();
});

test('successful connect resets the attempt counter', async () => {
  let calls = 0;
  const controller = new ReconnectController({
    connectFn: async () => {
      calls += 1;
      if (calls < 3) throw new Error('flaky');
    },
    baseDelayMs: 5,
    jitterMs: 0,
    maxAttempts: 5,
  });
  controller.start();
  await waitFor(() => controller.getState() === 'connected');
  assert.equal(calls, 3);
  assert.equal(controller.getAttempt(), 0);
  controller.stop();
  assert.equal(controller.getState(), 'idle');
});

test('notifyDisconnected moves from connected into backoff', async () => {
  const controller = new ReconnectController({
    connectFn: async () => {},
    baseDelayMs: 50,
    jitterMs: 0,
  });
  controller.start();
  await waitFor(() => controller.getState() === 'connected');
  controller.notifyDisconnected();
  assert.equal(controller.getState(), 'backoff');
  controller.stop();
  assert.equal(controller.getState(), 'idle');
});

test('custom BackoffStrategy is honored verbatim (constant-delay strategy)', async () => {
  // A trivial strategy every realtime client eventually wants: fixed delay.
  class FixedDelay {
    private readonly ms: number;
    constructor(ms: number) {
      this.ms = ms;
    }
    delayMs(_attempt: number): number {
      return this.ms;
    }
  }

  const recorded: number[] = [];
  const originalSetTimeout = globalThis.setTimeout;
  let armed = false;
  (globalThis as unknown as { setTimeout: unknown }).setTimeout = (
    fn: (...args: unknown[]) => void,
    ms: number,
    ...rest: unknown[]
  ) => {
    if (armed) recorded.push(ms);
    return (originalSetTimeout as (...a: unknown[]) => unknown)(fn, ms, ...rest);
  };

  let gaveUp = 0;
  const controller = new ReconnectController({
    connectFn: async () => {
      throw new Error('network down');
    },
    strategy: new FixedDelay(25),
    // These legacy knobs must be ignored once a strategy is injected.
    baseDelayMs: 10_000,
    maxDelayMs: 60_000,
    jitterMs: 5_000,
    maxAttempts: 4,
    onGiveUp: () => {
      gaveUp += 1;
      armed = false;
    },
  });
  try {
    armed = true;
    controller.start();
    await waitFor(() => gaveUp === 1);
  } finally {
    (globalThis as unknown as { setTimeout: unknown }).setTimeout = originalSetTimeout;
    controller.stop();
  }

  assert.equal(recorded.length, 3);
  assert.deepEqual(recorded, [25, 25, 25]);
});

test('strategy receives 1-based attempt numbers (linear strategy)', () => {
  const seen: number[] = [];
  const linear = {
    delayMs: (attempt: number): number => {
      seen.push(attempt);
      return attempt * 100;
    },
  };
  const controller = new ReconnectController({
    connectFn: async () => {},
    strategy: linear,
  });
  assert.equal(controller.computeDelayMs(1), 100);
  assert.equal(controller.computeDelayMs(2), 200);
  assert.equal(controller.computeDelayMs(3), 300);
  assert.deepEqual(seen, [1, 2, 3]);
  controller.stop();
});

test('default ExponentialBackoff grows and caps with injected randomness', () => {
  const strategy = new ExponentialBackoff({
    baseDelayMs: 100,
    maxDelayMs: 250,
    jitterMs: 100,
    random: () => 0.5, // deterministic: jitter contributes exactly half
  });
  assert.equal(strategy.delayMs(1), 100 + 50);
  assert.equal(strategy.delayMs(2), 200 + 50);
  // 100 * 2^2 = 400 would exceed the cap; capped to 250 before jitter.
  assert.equal(strategy.delayMs(3), 250 + 50);
  assert.equal(strategy.delayMs(10), 250 + 50);
  // Non-positive attempt numbers clamp to the first retry's delay.
  assert.equal(strategy.delayMs(0), strategy.delayMs(1));
});

test('without a strategy the controller still uses exponential backoff', async () => {
  const controller = new ReconnectController({
    connectFn: async () => {},
    baseDelayMs: 100,
    maxDelayMs: 800,
    jitterMs: 0,
  });
  assert.equal(controller.computeDelayMs(1), 100);
  assert.equal(controller.computeDelayMs(2), 200);
  assert.equal(controller.computeDelayMs(3), 400);
  assert.equal(controller.computeDelayMs(4), 800);
  assert.equal(controller.computeDelayMs(5), 800); // capped
  controller.stop();
});
