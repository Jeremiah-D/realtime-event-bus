import test from 'node:test';
import assert from 'node:assert/strict';
import { ReconnectController } from '../src/reconnect.ts';

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
