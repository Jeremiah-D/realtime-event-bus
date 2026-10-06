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

test('successful pong reports latency and keeps the connection up', async () => {
  const controller = await connectedController();
  const latencies: number[] = [];
  let timeouts = 0;
  let pings = 0;
  const monitor = new HeartbeatMonitor(controller, {
    intervalMs: 10,
    timeoutMs: 100,
    pingFn: async () => {
      pings += 1;
    },
    onPong: (latencyMs) => latencies.push(latencyMs),
    onTimeout: () => {
      timeouts += 1;
    },
  });
  try {
    monitor.start();
    await waitFor(() => latencies.length >= 3);
    // Pings only fire while the controller is connected.
    assert.equal(controller.getState(), 'connected');
  } finally {
    monitor.stop();
    controller.stop();
  }
  assert.ok(pings >= 3, 'should have pinged multiple times');
  assert.equal(timeouts, 0, 'no timeout should have fired');
  assert.equal(monitor.getMissed(), 0);
  for (const latency of latencies) assert.ok(latency >= 0, 'latency must be non-negative');
});

test('a hung peer times out and hands the connection back to reconnect', async () => {
  // Slow reconnect: the reconnect attempt after the disconnect must not fire
  // inside the test window, so the monitor only ever sends one ping.
  const controller = new ReconnectController({
    connectFn: async () => {},
    baseDelayMs: 60_000,
    jitterMs: 0,
  });
  controller.start();
  await waitFor(() => controller.getState() === 'connected');
  const timeoutCalls: number[] = [];
  let pings = 0;
  const monitor = new HeartbeatMonitor(controller, {
    intervalMs: 10,
    timeoutMs: 30,
    pingFn: () => {
      pings += 1;
      return new Promise<void>(() => {}); // peer never answers
    },
    onTimeout: (missed) => timeoutCalls.push(missed),
  });
  try {
    monitor.start();
    await waitFor(() => controller.getState() === 'backoff');
  } finally {
    monitor.stop();
    controller.stop();
  }
  assert.equal(pings, 1, 'the first ping hangs, so the monitor must not stack more');
  assert.deepEqual(timeoutCalls, [1], 'exactly one timeout before disconnect is reported');
  assert.equal(monitor.getMissed(), 1);
});

test('no pings are sent while the controller is not connected', async () => {
  let pings = 0;
  const controller = new ReconnectController({
    // Always fails: the controller sits in backoff forever.
    connectFn: async () => {
      throw new Error('network down');
    },
    baseDelayMs: 20,
    jitterMs: 0,
    maxAttempts: 1000,
  });
  const monitor = new HeartbeatMonitor(controller, {
    intervalMs: 10,
    timeoutMs: 10,
    pingFn: async () => {
      pings += 1;
    },
  });
  controller.start();
  try {
    await waitFor(() => controller.getState() === 'backoff');
    monitor.start();
    // Let several heartbeat intervals pass while stuck in backoff.
    await new Promise((resolve) => setTimeout(resolve, 60));
  } finally {
    monitor.stop();
    controller.stop();
  }
  assert.equal(pings, 0, 'heartbeats must stay silent during reconnect backoff');
});

test('consecutive timeouts increment the miss counter until a pong resets it', async () => {
  const controller = await connectedController();
  const timeoutCalls: number[] = [];
  // The first two pings fail outright; the third answers normally.
  let attempts = 0;
  const monitor = new HeartbeatMonitor(controller, {
    intervalMs: 10,
    timeoutMs: 10,
    pingFn: () => {
      attempts += 1;
      if (attempts <= 2) return Promise.reject(new Error('transport reset'));
      return Promise.resolve();
    },
    onTimeout: (missed) => timeoutCalls.push(missed),
  });
  try {
    monitor.start();
    await waitFor(() => timeoutCalls.length === 2);
    await waitFor(() => attempts >= 3 && controller.getState() === 'connected');
  } finally {
    monitor.stop();
    controller.stop();
  }
  assert.deepEqual(timeoutCalls, [1, 2], 'miss counter must grow across consecutive failures');
  assert.equal(monitor.getMissed(), 0, 'a pong resets the miss counter');
});

test('stop halts the ping interval', async () => {
  const controller = await connectedController();
  let pings = 0;
  const monitor = new HeartbeatMonitor(controller, {
    intervalMs: 10,
    timeoutMs: 1000,
    pingFn: async () => {
      pings += 1;
    },
  });
  try {
    monitor.start();
    await waitFor(() => pings >= 1);
    monitor.stop();
    const frozen = pings;
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.equal(pings, frozen, 'no pings after stop');
    assert.equal(monitor.getMissed(), 0);
  } finally {
    controller.stop();
  }
});
