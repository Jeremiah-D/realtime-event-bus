import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  type BridgeEnvelope,
  type BridgeTransport,
} from '../src/bus.ts';
import {
  BridgeHealthMonitor,
  resolveBridgeHealthOptions,
  BRIDGE_PROBE_TOPIC,
  DEFAULT_BRIDGE_HEALTH_FAILURE_THRESHOLD,
  DEFAULT_BRIDGE_HEALTH_INTERVAL_MS,
} from '../src/bridgehealth.ts';
import { resolveBridgeOptions } from '../src/bridge.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const sink: BridgeTransport = { name: 'sink', publish() {} };

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

// --- option validation ---

test('resolveBridgeHealthOptions validates and applies defaults', () => {
  assert.equal(resolveBridgeHealthOptions(undefined, 'test'), undefined);
  const d = resolveBridgeHealthOptions({}, 'test')!;
  assert.equal(d.intervalMs, DEFAULT_BRIDGE_HEALTH_INTERVAL_MS);
  assert.equal(d.failureThreshold, DEFAULT_BRIDGE_HEALTH_FAILURE_THRESHOLD);

  const bad: unknown[] = [
    'yes',
    [],
    { intervalMs: 0 },
    { intervalMs: -5 },
    { intervalMs: Number.NaN },
    { intervalMs: 'fast' },
    { failureThreshold: 0 },
    { failureThreshold: 1.5 },
    { now: 42 },
    { setTimer: 'x' },
    { clearTimer: 'x' },
  ];
  for (const options of bad) {
    assert.throws(
      () => resolveBridgeHealthOptions(options as never, 'test'),
      RangeError,
      `should throw for ${JSON.stringify(options)}`,
    );
  }
  const ok = resolveBridgeHealthOptions(
    { intervalMs: 100, failureThreshold: 1, now: () => 0 },
    'test',
  )!;
  assert.equal(ok.intervalMs, 100);
  assert.equal(ok.failureThreshold, 1);
});

test('resolveBridgeOptions validates bridge.health and transport.probe', () => {
  const resolved = resolveBridgeOptions({ transport: sink, health: {} }, 'test')!;
  assert.equal(resolved.health?.failureThreshold, DEFAULT_BRIDGE_HEALTH_FAILURE_THRESHOLD);
  assert.equal(resolveBridgeOptions({ transport: sink }, 'test')!.health, undefined);
  assert.throws(() => resolveBridgeOptions({ transport: sink, health: 'x' }, 'test'), RangeError);
  assert.throws(
    () => resolveBridgeOptions({ transport: { name: 'bad', publish() {}, probe: 'x' } }, 'test'),
    RangeError,
  );
});

// --- BridgeHealthMonitor unit tests ---

function monitorWith(
  transport: BridgeTransport,
  healthOpts: Parameters<typeof resolveBridgeHealthOptions>[0] = {},
) {
  const events: string[] = [];
  const timers: Array<() => void> = [];
  const monitor = new BridgeHealthMonitor(
    transport,
    resolveBridgeHealthOptions(healthOpts, 'test')!,
    () => 1000,
    {
      onUnhealthy: () => events.push('unhealthy'),
      onRecovered: () => events.push('recovered'),
    },
  );
  return { monitor, events, timers };
}

test('monitor trips at the failure threshold and recovers on success', () => {
  const { monitor, events } = monitorWith(sink, { failureThreshold: 2 });
  assert.equal(monitor.healthy(), true);
  monitor.recordFailure();
  assert.equal(monitor.healthy(), true);
  assert.deepEqual(events, []);
  monitor.recordFailure();
  assert.equal(monitor.healthy(), false);
  assert.deepEqual(events, ['unhealthy']);
  assert.deepEqual(monitor.getStats(), { state: 'unhealthy', consecutiveFailures: 2 });
  // Further failures do not re-fire the transition.
  monitor.recordFailure();
  assert.deepEqual(events, ['unhealthy']);
  // Any success recovers.
  monitor.recordSuccess();
  assert.equal(monitor.healthy(), true);
  assert.deepEqual(events, ['unhealthy', 'recovered']);
  assert.deepEqual(monitor.getStats(), { state: 'healthy', consecutiveFailures: 0 });
});

test('monitor success resets the consecutive failure counter without tripping', () => {
  const { monitor, events } = monitorWith(sink, { failureThreshold: 3 });
  monitor.recordFailure();
  monitor.recordSuccess();
  assert.equal(monitor.getStats().consecutiveFailures, 0);
  assert.equal(monitor.healthy(), true);
  assert.deepEqual(events, []);
});

test('probeOnce uses the transport custom probe when present', async () => {
  let probeResult = false;
  let probeCalls = 0;
  const transport: BridgeTransport = {
    name: 'custom-probe',
    publish() {},
    probe() {
      probeCalls += 1;
      return probeResult;
    },
  };
  const { monitor, events } = monitorWith(transport, { failureThreshold: 1 });
  await monitor.probeOnce();
  assert.equal(probeCalls, 1);
  assert.equal(monitor.healthy(), false);
  assert.deepEqual(events, ['unhealthy']);
  assert.equal(monitor.getStats().lastProbeAt, 1000);
  probeResult = true;
  await monitor.probeOnce();
  assert.equal(monitor.healthy(), true);
  assert.deepEqual(events, ['unhealthy', 'recovered']);
});

test('probeOnce falls back to an envelope round-trip without a custom probe', async () => {
  const seen: BridgeEnvelope[] = [];
  const transport: BridgeTransport = {
    name: 'round-trip',
    publish(envelope) {
      seen.push(envelope);
    },
  };
  const { monitor } = monitorWith(transport, { failureThreshold: 1 });
  await monitor.probeOnce();
  assert.equal(seen.length, 1);
  assert.equal(seen[0].topic, BRIDGE_PROBE_TOPIC);
  assert.equal(monitor.healthy(), true);
  assert.equal(monitor.getStats().lastProbeAt, 1000);
});

test('probeOnce treats a throwing probe and a rejected publish as failures', async () => {
  const throwing: BridgeTransport = {
    name: 'throwing-probe',
    publish() {},
    probe() {
      throw new Error('nope');
    },
  };
  const rejecting: BridgeTransport = {
    name: 'rejecting',
    publish() {
      return Promise.reject(new Error('down'));
    },
  };
  for (const transport of [throwing, rejecting]) {
    const { monitor } = monitorWith(transport, { failureThreshold: 1 });
    await monitor.probeOnce(); // never rejects
    assert.equal(monitor.healthy(), false, transport.name);
  }
});

test('monitor start/stop drives the injected timer; stop is idempotent', async () => {
  const captured: Array<{ fn: () => void; ms: number }> = [];
  const cleared: unknown[] = [];
  const transport: BridgeTransport = { name: 't', publish() {} };
  const monitor = new BridgeHealthMonitor(
    transport,
    resolveBridgeHealthOptions(
      {
        intervalMs: 50,
        failureThreshold: 5,
        setTimer: (fn, ms) => {
          captured.push({ fn, ms });
          return captured.length;
        },
        clearTimer: (h) => {
          cleared.push(h);
        },
      },
      'test',
    )!,
    () => 7,
    { onUnhealthy() {}, onRecovered() {} },
  );
  monitor.start();
  monitor.start(); // idempotent: one timer only
  assert.equal(captured.length, 1);
  assert.equal(captured[0].ms, 50);
  // Fire the tick; the probe runs and the schedule re-arms.
  captured[0].fn();
  await flush();
  await flush();
  assert.equal(monitor.getStats().lastProbeAt, 7);
  assert.equal(captured.length, 2);
  monitor.stop();
  monitor.stop(); // idempotent
  assert.deepEqual(cleared, [2]);
});

// --- bus integration ---

test('bridge health is disabled by default: failures never pause mirroring', async () => {
  const transport = failingTransport();
  const bus = new EventBus({ bridge: { transport } });
  assert.deepEqual(bus.getStats().bridge.health, {
    state: 'disabled',
    consecutiveFailures: 0,
  });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publish('t', 1);
  bus.publish('t', 2);
  await flush();
  // Every publish still attempts the mirror (swallowed), nothing pauses.
  assert.equal(transport.calls, 2);
  assert.equal(bus.getStats().bridge.outbound, 2);
  assert.deepEqual(received, [1, 2]);
  assert.deepEqual(bus.getSubscriptionAudit({ action: 'bridge_unhealthy' }), []);
});

test('consecutive mirror failures pause outbound mirroring; local delivery continues', async () => {
  const clock = controllableClock();
  const transport = failingTransport();
  const bus = new EventBus({
    now: clock.now,
    bridge: { transport, health: { failureThreshold: 2 } },
  });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));

  bus.publish('t', 1);
  assert.equal(bus.getStats().bridge.health.state, 'healthy');
  assert.equal(bus.getStats().bridge.health.consecutiveFailures, 1);

  bus.publish('t', 2);
  const health = bus.getStats().bridge.health;
  assert.equal(health.state, 'unhealthy');
  assert.equal(health.consecutiveFailures, 2);

  // Paused: the third publish is not mirrored and not counted…
  bus.publish('t', 3);
  assert.equal(transport.calls, 2);
  assert.equal(bus.getStats().bridge.outbound, 2);
  // …but local delivery is unaffected.
  await flush();
  assert.deepEqual(received, [1, 2, 3]);

  // One audit event per transition.
  const unhealthy = bus.getSubscriptionAudit({ action: 'bridge_unhealthy' });
  assert.equal(unhealthy.length, 1);
  assert.equal(unhealthy[0].actor, 'system');
  assert.equal(
    (unhealthy[0].detail as { transport: string }).transport,
    'failing',
  );
});

test('a successful probe recovers the bridge and resumes mirroring', async () => {
  const transport = failingTransport();
  const captured: Array<() => void> = [];
  const bus = new EventBus({
    bridge: {
      transport,
      health: {
        failureThreshold: 1,
        setTimer: (fn) => {
          captured.push(fn);
          return captured.length;
        },
      },
    },
  });
  bus.subscribe('t', () => {});
  bus.publish('t', 1);
  assert.equal(bus.getStats().bridge.health.state, 'unhealthy');
  assert.equal(captured.length, 1);

  // Heal the transport, then fire the scheduled probe: the envelope
  // round-trip succeeds, the bridge recovers, and mirroring resumes for
  // new publishes. (While unhealthy, mirrors are paused, so recovery is
  // probe-driven by design.)
  transport.publish = () => {};
  captured[0]();
  await flush();
  await flush();
  assert.equal(bus.getStats().bridge.health.state, 'healthy');
  assert.equal(bus.getStats().bridge.health.consecutiveFailures, 0);
  const recovered = bus.getSubscriptionAudit({ action: 'bridge_recovered' });
  assert.equal(recovered.length, 1);

  const before = bus.getStats().bridge.outbound;
  bus.publish('t', 2);
  assert.equal(bus.getStats().bridge.outbound, before + 1);
  await bus.close();
});

test('rejected mirror promises feed the health monitor', async () => {
  let calls = 0;
  const transport: BridgeTransport = {
    name: 'rejecting',
    publish() {
      calls += 1;
      return Promise.reject(new Error('down'));
    },
  };
  const bus = new EventBus({ bridge: { transport, health: { failureThreshold: 2 } } });
  bus.subscribe('t', () => {});
  bus.publish('t', 1);
  bus.publish('t', 2);
  await flush();
  await flush();
  assert.equal(bus.getStats().bridge.health.state, 'unhealthy');
  assert.equal(bus.getStats().bridge.health.consecutiveFailures, 2);
  assert.equal(calls, 2);
});

test('close() stops the health probe timer', async () => {
  const captured: Array<() => void> = [];
  const cleared: unknown[] = [];
  const bus = new EventBus({
    bridge: {
      transport: sink,
      health: {
        intervalMs: 10,
        setTimer: (fn) => {
          captured.push(fn);
          return captured.length;
        },
        clearTimer: (h) => {
          cleared.push(h);
        },
      },
    },
  });
  assert.equal(captured.length, 1);
  await bus.close();
  assert.deepEqual(cleared, [1]);
  assert.equal(bus.getStats().bridge.health.state, 'healthy');
});
