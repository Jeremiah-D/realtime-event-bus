import test from 'node:test';
import assert from 'node:assert/strict';
import { ReconnectController } from '../src/reconnect.ts';
import { HeartbeatMonitor } from '../src/heartbeat.ts';

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

/** Starts the controller and waits until it reports `connected`. */
async function connectedController(
  connectFn: () => Promise<void> = async () => {},
): Promise<ReconnectController> {
  const controller = new ReconnectController({
    connectFn,
    baseDelayMs: 10,
    jitterMs: 0,
  });
  controller.start();
  await waitFor(() => controller.getState() === 'connected');
  return controller;
}

/**
 * A connected controller whose reconnect attempt after a disconnect is
 * parked far in the future, so the monitor only ever sends one ping
 * before the test ends.
 */
async function slowReconnectController(): Promise<ReconnectController> {
  const controller = new ReconnectController({
    connectFn: async () => {},
    baseDelayMs: 60_000,
    jitterMs: 0,
  });
  controller.start();
  await waitFor(() => controller.getState() === 'connected');
  return controller;
}

test('retune tightening the interval makes the tick cadence follow', async () => {
  const controller = await connectedController();
  let pings = 0;
  const monitor = new HeartbeatMonitor(controller, {
    intervalMs: 200,
    timeoutMs: 1000,
    pingFn: async () => {
      pings += 1;
    },
  });
  try {
    monitor.start();
    // At the 200ms cadence at most one tick fires in this window.
    await new Promise((resolve) => setTimeout(resolve, 250));
    assert.ok(pings <= 2, `slow cadence should ping at most twice in 250ms, got ${pings}`);
    monitor.retune({ intervalMs: 20 });
    assert.equal(monitor.getIntervalMs(), 20);
    const atRetune = pings;
    await waitFor(() => pings - atRetune >= 5, 2000);
    assert.ok(pings - atRetune >= 5, 'the tightened cadence should ping several times');
  } finally {
    monitor.stop();
    controller.stop();
  }
});

test('retune tightening the timeout makes a hung peer judged missed sooner', async () => {
  const controller = await connectedController();
  const timeoutCalls: number[] = [];
  let attempts = 0;
  const monitor = new HeartbeatMonitor(controller, {
    // Wide interval so the retune lands well before the third tick.
    intervalMs: 50,
    timeoutMs: 5000,
    pingFn: () => {
      attempts += 1;
      if (attempts <= 2) return Promise.resolve();
      return new Promise<void>(() => {}); // peer hangs from the third ping on
    },
    onTimeout: (missed) => timeoutCalls.push(missed),
  });
  try {
    monitor.start();
    await waitFor(() => attempts >= 2);
    assert.deepEqual(timeoutCalls, [], 'no timeout while the peer answers');
    monitor.retune({ timeoutMs: 40 });
    assert.equal(monitor.getTimeoutMs(), 40);
    // The next ping hangs and must time out at the tightened 40ms,
    // not the original 5000ms.
    await waitFor(() => timeoutCalls.length >= 1, 2000);
    assert.deepEqual(timeoutCalls, [1], 'exactly one timeout at the tightened budget');
    assert.equal(monitor.getMissed(), 1);
  } finally {
    monitor.stop();
    controller.stop();
  }
});

test('retune rejects illegal values and leaves the live values untouched', async () => {
  const controller = await connectedController();
  const monitor = new HeartbeatMonitor(controller, {
    intervalMs: 100,
    timeoutMs: 200,
    pingFn: async () => {},
  });
  try {
    for (const bad of [0, -1, Number.NaN, Number.POSITIVE_INFINITY]) {
      assert.throws(() => monitor.retune({ intervalMs: bad }), RangeError);
      assert.throws(() => monitor.retune({ timeoutMs: bad }), RangeError);
    }
    assert.throws(
      () => monitor.retune({ intervalMs: 'fast' as unknown as number }),
      RangeError,
    );
    // One bad field must not move the other: a mixed revision is all-or-nothing.
    assert.throws(() => monitor.retune({ intervalMs: 50, timeoutMs: 0 }), RangeError);
    assert.equal(monitor.getIntervalMs(), 100);
    assert.equal(monitor.getTimeoutMs(), 200);
    // The constructor enforces the same rule (EB-394 shares the validation).
    assert.throws(
      () => new HeartbeatMonitor(controller, { intervalMs: 0, pingFn: async () => {} }),
      RangeError,
    );
    assert.throws(
      () => new HeartbeatMonitor(controller, { timeoutMs: -5, pingFn: async () => {} }),
      RangeError,
    );
  } finally {
    monitor.stop();
    controller.stop();
  }
});

test('retune to the current values is a no-op', async () => {
  const controller = await connectedController();
  let pings = 0;
  const monitor = new HeartbeatMonitor(controller, {
    intervalMs: 20,
    timeoutMs: 1000,
    pingFn: async () => {
      pings += 1;
    },
  });
  try {
    monitor.start();
    await waitFor(() => pings >= 2);
    monitor.retune({ intervalMs: 20, timeoutMs: 1000 });
    assert.equal(monitor.getIntervalMs(), 20);
    assert.equal(monitor.getTimeoutMs(), 1000);
    // Still ticking at the same cadence, undisturbed.
    const atRetune = pings;
    await waitFor(() => pings - atRetune >= 2);
  } finally {
    monitor.stop();
    controller.stop();
  }
});

test('retune before start only stores the values for the next start()', async () => {
  const controller = await connectedController();
  let pings = 0;
  const monitor = new HeartbeatMonitor(controller, {
    intervalMs: 1000,
    timeoutMs: 5000,
    pingFn: async () => {
      pings += 1;
    },
  });
  try {
    monitor.retune({ intervalMs: 20, timeoutMs: 100 });
    assert.equal(monitor.getIntervalMs(), 20);
    assert.equal(monitor.getTimeoutMs(), 100);
    monitor.start();
    // Three pings in ~60ms at the stored 20ms cadence; the original
    // 1000ms cadence could not produce a single ping this fast.
    await waitFor(() => pings >= 3, 2000);
  } finally {
    monitor.stop();
    controller.stop();
  }
});

test('an in-flight ping keeps the timeout it was armed with', async () => {
  const controller = await slowReconnectController();
  const timeoutCalls: number[] = [];
  let pings = 0;
  const monitor = new HeartbeatMonitor(controller, {
    intervalMs: 20,
    timeoutMs: 300,
    pingFn: () => {
      pings += 1;
      return new Promise<void>(() => {}); // peer never answers
    },
    onTimeout: (missed) => timeoutCalls.push(missed),
  });
  try {
    monitor.start();
    await waitFor(() => pings >= 1);
    monitor.retune({ timeoutMs: 30 });
    assert.equal(monitor.getTimeoutMs(), 30);
    // The in-flight ping was armed with the old 300ms timeout: no
    // timeout may fire inside the tightened 30ms window.
    await new Promise((resolve) => setTimeout(resolve, 150));
    assert.deepEqual(timeoutCalls, [], 'the in-flight ping must not use the new timeout');
    assert.equal(controller.getState(), 'connected');
    // …but it does time out at its original ~300ms budget.
    await waitFor(() => timeoutCalls.length === 1, 1000);
    assert.deepEqual(timeoutCalls, [1]);
    assert.equal(monitor.getMissed(), 1);
  } finally {
    monitor.stop();
    controller.stop();
  }
});

test('getters follow the live values', async () => {
  const controller = await connectedController();
  const monitor = new HeartbeatMonitor(controller, {
    pingFn: async () => {},
  });
  try {
    assert.equal(monitor.getIntervalMs(), 30_000);
    assert.equal(monitor.getTimeoutMs(), 5_000);
    monitor.retune({ intervalMs: 123 });
    assert.equal(monitor.getIntervalMs(), 123);
    assert.equal(monitor.getTimeoutMs(), 5_000);
    monitor.retune({ timeoutMs: 456 });
    assert.equal(monitor.getIntervalMs(), 123);
    assert.equal(monitor.getTimeoutMs(), 456);
  } finally {
    monitor.stop();
    controller.stop();
  }
});

test('retune does not reset the miss count', async () => {
  const controller = await slowReconnectController();
  const monitor = new HeartbeatMonitor(controller, {
    intervalMs: 10,
    timeoutMs: 30,
    pingFn: () => new Promise<void>(() => {}), // peer never answers
  });
  try {
    monitor.start();
    await waitFor(() => monitor.getMissed() === 1);
    monitor.retune({ intervalMs: 20, timeoutMs: 50 });
    assert.equal(monitor.getMissed(), 1, 'a retune must not clear recorded misses');
    assert.equal(monitor.getIntervalMs(), 20);
    assert.equal(monitor.getTimeoutMs(), 50);
  } finally {
    monitor.stop();
    controller.stop();
  }
});
