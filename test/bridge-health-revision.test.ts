/**
 * Bus-level bridge-health monitoring revision (EB-305).
 *
 * `setBridgeHealth` retunes the probe interval and the failure
 * threshold live — without restarting the bus. Only a bus that already
 * enabled `bridge.health` can be revised; illegal values fail closed
 * with the live tuning and the audit log untouched; an identical (or
 * empty) revision is a no-op with no audit. The health state is
 * untouched by a revision: the consecutive-failure counter, the
 * last-probe timestamp, and the healthy/unhealthy state all survive,
 * and a moved interval clears and re-arms the probe timer at the new
 * interval with no extra probe fired.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BridgeEnvelope, type BridgeTransport } from '../src/bus.ts';
import {
  BridgeHealthMonitor,
  resolveBridgeHealthOptions,
} from '../src/bridgehealth.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

function failingTransport(): BridgeTransport & { calls: number } {
  const t = {
    name: 'failing',
    calls: 0,
    publish(_envelope: BridgeEnvelope) {
      t.calls += 1;
      throw new Error('broker down');
    },
  };
  return t;
}

/**
 * Captures the monitor's injected timers: every `setTimer` call records
 * its callback and interval and returns a 1-based handle; every
 * `clearTimer` records the cleared handle.
 */
function timerCapture() {
  const captured: Array<{ fn: () => void; ms: number }> = [];
  const cleared: unknown[] = [];
  return {
    captured,
    cleared,
    setTimer: (fn: () => void, ms: number) => {
      captured.push({ fn, ms });
      return captured.length;
    },
    clearTimer: (h: unknown) => {
      cleared.push(h);
    },
  };
}

describe('bridge-health revision (EB-305)', () => {
  it('setBridgeHealth fails closed when bridge.health is not enabled', () => {
    const bus = new EventBus({ bridge: { transport: failingTransport() } });
    assert.throws(() => bus.setBridgeHealth({ intervalMs: 100 }), RangeError);
    assert.throws(() => bus.setBridgeHealth({ failureThreshold: 1 }), RangeError);
    assert.equal(bus.getSubscriptionAudit({ action: 'bridge_health_revised' }).length, 0);
  });

  it('setBridgeHealth rejects illegal values and leaves the live tuning untouched', async () => {
    const timers = timerCapture();
    const transport = failingTransport();
    const bus = new EventBus({
      bridge: {
        transport,
        health: {
          intervalMs: 30_000,
          failureThreshold: 3,
          setTimer: timers.setTimer,
          clearTimer: timers.clearTimer,
        },
      },
    });
    bus.subscribe('t', () => {});
    const bad: Array<unknown> = [
      { intervalMs: 0 },
      { intervalMs: -5 },
      { intervalMs: Number.NaN },
      { intervalMs: Number.POSITIVE_INFINITY },
      { intervalMs: 'fast' },
      { failureThreshold: 0 },
      { failureThreshold: -1 },
      { failureThreshold: 1.5 },
      { failureThreshold: 'many' },
      'nope',
      42,
      ['x'],
    ];
    for (const revision of bad) {
      assert.throws(
        () => bus.setBridgeHealth(revision as never),
        RangeError,
        `expected RangeError for ${JSON.stringify(revision)}`,
      );
    }
    // The failed calls changed nothing: the probe timer is still armed
    // at the original interval, and two mirror failures still leave
    // the bridge healthy under the live threshold of 3.
    assert.equal(timers.captured.length, 1);
    assert.equal(timers.captured[0].ms, 30_000);
    assert.deepEqual(timers.cleared, []);
    bus.publish('t', 1);
    bus.publish('t', 2);
    assert.equal(bus.getStats().bridge.health.state, 'healthy');
    assert.equal(bus.getStats().bridge.health.consecutiveFailures, 2);
    assert.equal(bus.getSubscriptionAudit({ action: 'bridge_health_revised' }).length, 0);
    await bus.close();
  });

  it('setBridgeHealth with identical values or an empty revision is a no-op with no audit', async () => {
    const timers = timerCapture();
    const bus = new EventBus({
      bridge: {
        transport: failingTransport(),
        health: {
          intervalMs: 5_000,
          failureThreshold: 2,
          setTimer: timers.setTimer,
          clearTimer: timers.clearTimer,
        },
      },
    });
    bus.setBridgeHealth({ intervalMs: 5_000 });
    bus.setBridgeHealth({ failureThreshold: 2 });
    bus.setBridgeHealth({ intervalMs: 5_000, failureThreshold: 2 });
    bus.setBridgeHealth({});
    bus.setBridgeHealth(null as never);
    // No re-arm, no audit: the single timer armed at construction is
    // still the only one.
    assert.equal(timers.captured.length, 1);
    assert.equal(timers.captured[0].ms, 5_000);
    assert.deepEqual(timers.cleared, []);
    assert.equal(bus.getSubscriptionAudit({ action: 'bridge_health_revised' }).length, 0);
    await bus.close();
  });

  it('setBridgeHealth partially revises only the fields provided', async () => {
    const timers = timerCapture();
    const bus = new EventBus({
      bridge: {
        transport: failingTransport(),
        health: {
          intervalMs: 30_000,
          failureThreshold: 3,
          setTimer: timers.setTimer,
          clearTimer: timers.clearTimer,
        },
      },
    });
    bus.subscribe('t', () => {});
    // Interval-only: the threshold still trips at 3 failures.
    bus.setBridgeHealth({ intervalMs: 100 });
    bus.publish('t', 1);
    bus.publish('t', 2);
    assert.equal(bus.getStats().bridge.health.state, 'healthy');
    bus.publish('t', 3);
    assert.equal(bus.getStats().bridge.health.state, 'unhealthy');
    assert.equal(bus.getStats().bridge.health.consecutiveFailures, 3);
    // The audit records the moved field and the kept one.
    const revised = bus.getSubscriptionAudit({ action: 'bridge_health_revised' });
    assert.equal(revised.length, 1);
    assert.deepEqual(revised[0].detail, {
      intervalMs: 100,
      oldIntervalMs: 30_000,
      failureThreshold: 3,
      oldFailureThreshold: 3,
    });
    await bus.close();
  });

  it('setBridgeHealth re-arms the probe timer at the new interval', async () => {
    const clock = controllableClock();
    const timers = timerCapture();
    const transport = failingTransport();
    const bus = new EventBus({
      now: clock.now,
      bridge: {
        transport,
        health: {
          intervalMs: 30_000,
          failureThreshold: 5,
          setTimer: timers.setTimer,
          clearTimer: timers.clearTimer,
        },
      },
    });
    assert.equal(timers.captured.length, 1);
    assert.equal(timers.captured[0].ms, 30_000);

    bus.setBridgeHealth({ intervalMs: 100 });
    // Old timer cleared, new one armed at the new interval — and no
    // probe fired as a side effect of the revision itself.
    assert.deepEqual(timers.cleared, [1]);
    assert.equal(timers.captured.length, 2);
    assert.equal(timers.captured[1].ms, 100);
    assert.equal(bus.getStats().bridge.health.lastProbeAt, undefined);

    // Firing the re-armed tick runs a probe at the new interval, and
    // the schedule re-arms at the new interval again.
    transport.publish = () => {};
    clock.nowMs = 5_000;
    timers.captured[1].fn();
    await flush();
    await flush();
    assert.equal(bus.getStats().bridge.health.lastProbeAt, 5_000);
    assert.equal(timers.captured.length, 3);
    assert.equal(timers.captured[2].ms, 100);
    await bus.close();
  });

  it('setBridgeHealth keeps health state and the failure counter across a revision', async () => {
    const timers = timerCapture();
    const transport = failingTransport();
    const bus = new EventBus({
      bridge: {
        transport,
        health: {
          intervalMs: 30_000,
          failureThreshold: 3,
          setTimer: timers.setTimer,
          clearTimer: timers.clearTimer,
        },
      },
    });
    bus.subscribe('t', () => {});
    bus.publish('t', 1);
    bus.publish('t', 2);
    assert.deepEqual(bus.getStats().bridge.health, {
      state: 'healthy',
      consecutiveFailures: 2,
    });
    // Tighten the threshold: the counter is kept, so the bridge is
    // still healthy until the next failure trips it at the new bound.
    bus.setBridgeHealth({ failureThreshold: 2 });
    assert.deepEqual(bus.getStats().bridge.health, {
      state: 'healthy',
      consecutiveFailures: 2,
    });
    // Threshold-only revision: the pending probe's countdown is
    // untouched — no timer was cleared or re-armed.
    assert.equal(timers.captured.length, 1);
    assert.deepEqual(timers.cleared, []);
    bus.publish('t', 3);
    assert.equal(bus.getStats().bridge.health.state, 'unhealthy');
    assert.equal(bus.getStats().bridge.health.consecutiveFailures, 3);
    assert.equal(bus.getSubscriptionAudit({ action: 'bridge_unhealthy' }).length, 1);
    // The unhealthy state survives a further revision too.
    bus.setBridgeHealth({ intervalMs: 100 });
    assert.equal(bus.getStats().bridge.health.state, 'unhealthy');
    assert.equal(bus.getStats().bridge.health.consecutiveFailures, 3);
    await bus.close();
  });

  it('a loosened threshold needs more failures from the revision onward', async () => {
    const transport = failingTransport();
    const bus = new EventBus({
      bridge: { transport, health: { failureThreshold: 2 } },
    });
    bus.subscribe('t', () => {});
    bus.publish('t', 1);
    assert.equal(bus.getStats().bridge.health.consecutiveFailures, 1);
    bus.setBridgeHealth({ failureThreshold: 5 });
    // The kept counter of 1 is now far below the new threshold.
    bus.publish('t', 2);
    bus.publish('t', 3);
    bus.publish('t', 4);
    assert.equal(bus.getStats().bridge.health.state, 'healthy');
    bus.publish('t', 5);
    assert.equal(bus.getStats().bridge.health.state, 'unhealthy');
    assert.equal(bus.getStats().bridge.health.consecutiveFailures, 5);
    await bus.close();
  });

  it('monitor retune re-arms an armed timer and leaves a probe in flight to re-arm itself', async () => {
    const timers = timerCapture();
    let releaseProbe!: () => void;
    const probeGate = new Promise<void>((resolve) => {
      releaseProbe = resolve;
    });
    const transport: BridgeTransport = {
      name: 'gated-probe',
      publish() {
        return probeGate;
      },
    };
    const monitor = new BridgeHealthMonitor(
      transport,
      resolveBridgeHealthOptions(
        {
          intervalMs: 50,
          failureThreshold: 5,
          setTimer: timers.setTimer,
          clearTimer: timers.clearTimer,
        },
        'test',
      )!,
      () => 7,
      { onUnhealthy() {}, onRecovered() {} },
    );
    monitor.start();
    assert.equal(timers.captured.length, 1);
    assert.equal(timers.captured[0].ms, 50);
    assert.deepEqual(monitor.current(), { intervalMs: 50, failureThreshold: 5 });

    // Threshold-only retune: no timer churn.
    monitor.retune({ intervalMs: 50, failureThreshold: 2 });
    assert.equal(timers.captured.length, 1);
    assert.deepEqual(timers.cleared, []);
    assert.deepEqual(monitor.current(), { intervalMs: 50, failureThreshold: 2 });

    // Interval retune while armed: old timer cleared, new one armed.
    monitor.retune({ intervalMs: 100, failureThreshold: 2 });
    assert.deepEqual(timers.cleared, [1]);
    assert.equal(timers.captured.length, 2);
    assert.equal(timers.captured[1].ms, 100);

    // Fire the new tick: the probe starts and the timer slot clears
    // while it is in flight. A retune mid-probe must not arm a second
    // timer — the in-flight probe's `finally` re-arms at the new
    // interval once it resolves.
    timers.captured[1].fn();
    monitor.retune({ intervalMs: 200, failureThreshold: 2 });
    assert.equal(timers.captured.length, 2);
    assert.deepEqual(timers.cleared, [1]);
    releaseProbe();
    await flush();
    await flush();
    assert.equal(monitor.getStats().lastProbeAt, 7);
    assert.equal(timers.captured.length, 3);
    assert.equal(timers.captured[2].ms, 200);
    monitor.stop();
  });
});
