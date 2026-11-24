/**
 * Bridge-health live enable (EB-550):
 * `setBridgeHealth` on a bus whose `bridge` is configured but never
 * opted into `bridge.health` installs the monitor live — no restart,
 * no lost subscriptions — with the revision resolved through the
 * construction-time rule (`resolveBridgeHealthOptions`: `{}` takes
 * every default, a partial object falls back per field). The fresh
 * monitor starts healthy with zero consecutive failures — mirror
 * failures before the enable were never recorded, so nothing is
 * counted retroactively — and it is live-only: a restart restores the
 * unconfigured state. The enable is audited like any revision, with
 * `oldIntervalMs` / `oldFailureThreshold` `null`. A bus with no
 * `bridge` at all still refuses, and a closed bus refuses the enable
 * and the revision.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BridgeEnvelope, type BridgeTransport } from '../src/bus.ts';

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** Polls until `check` is true or the budget (ms) runs out. */
async function waitFor(check: () => boolean, budgetMs = 500): Promise<boolean> {
  const deadline = Date.now() + budgetMs;
  while (!check()) {
    if (Date.now() >= deadline) return false;
    await sleep(5);
  }
  return true;
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

/** A transport that fails until `failing` flips, then succeeds. */
function flippableTransport(): BridgeTransport & { calls: number; failing: boolean } {
  const t = {
    name: 'flippable',
    calls: 0,
    failing: true,
    publish(_envelope: BridgeEnvelope) {
      t.calls += 1;
      if (t.failing) throw new Error('broker down');
    },
  };
  return t;
}

/** A transport whose custom probe counts its calls and succeeds. */
function probeCountingTransport(): BridgeTransport & { probeCalls: number } {
  const t = {
    name: 'probe-counting',
    probeCalls: 0,
    publish(_envelope: BridgeEnvelope) {},
    probe() {
      t.probeCalls += 1;
      return true;
    },
  };
  return t;
}

function auditEntries(bus: EventBus, action: string) {
  return bus.getSubscriptionAudit({ action: action as never });
}

describe('bridge-health live enable (EB-550)', () => {
  it('the first enable with {} takes the construction defaults', async () => {
    const bus = new EventBus({ bridge: { transport: failingTransport() } });
    assert.deepEqual(bus.getStats().bridge.health, {
      state: 'disabled',
      consecutiveFailures: 0,
    });
    bus.setBridgeHealth({});
    assert.deepEqual(bus.getStats().bridge.health, {
      state: 'healthy',
      consecutiveFailures: 0,
    });
    const audit = auditEntries(bus, 'bridge_health_revised');
    assert.equal(audit.length, 1);
    assert.equal(audit[0].actor, 'system');
    assert.deepEqual(audit[0].detail, {
      intervalMs: 30_000,
      oldIntervalMs: null,
      failureThreshold: 3,
      oldFailureThreshold: null,
    });
    await bus.close();
  });

  it('a partial enable falls back per field', async () => {
    const first = new EventBus({ bridge: { transport: failingTransport() } });
    first.setBridgeHealth({ intervalMs: 100 });
    assert.deepEqual(auditEntries(first, 'bridge_health_revised')[0].detail, {
      intervalMs: 100,
      oldIntervalMs: null,
      failureThreshold: 3,
      oldFailureThreshold: null,
    });
    await first.close();

    const second = new EventBus({ bridge: { transport: failingTransport() } });
    second.setBridgeHealth({ failureThreshold: 1 });
    assert.deepEqual(auditEntries(second, 'bridge_health_revised')[0].detail, {
      intervalMs: 30_000,
      oldIntervalMs: null,
      failureThreshold: 1,
      oldFailureThreshold: null,
    });
    await second.close();
  });

  it('an explicit enable sets both values exactly', async () => {
    const bus = new EventBus({ bridge: { transport: failingTransport() } });
    bus.setBridgeHealth({ intervalMs: 5_000, failureThreshold: 2 });
    assert.deepEqual(auditEntries(bus, 'bridge_health_revised')[0].detail, {
      intervalMs: 5_000,
      oldIntervalMs: null,
      failureThreshold: 2,
      oldFailureThreshold: null,
    });
    await bus.close();
  });

  it('null on a never-enabled bus is a silent no-op with no audit', async () => {
    const bus = new EventBus({ bridge: { transport: failingTransport() } });
    bus.setBridgeHealth(null as never);
    assert.equal(auditEntries(bus, 'bridge_health_revised').length, 0);
    assert.equal(bus.getStats().bridge.health.state, 'disabled');
    // The bus is still unconfigured: a later valid call enables it.
    bus.setBridgeHealth({ intervalMs: 100 });
    assert.equal(auditEntries(bus, 'bridge_health_revised').length, 1);
    await bus.close();
  });

  it('a non-object revision on a never-enabled bus throws and leaves the bus unconfigured', async () => {
    const bus = new EventBus({ bridge: { transport: failingTransport() } });
    const bad: Array<unknown> = ['nope', 42, ['x']];
    for (const revision of bad) {
      assert.throws(
        () => bus.setBridgeHealth(revision as never),
        RangeError,
        `expected RangeError for ${JSON.stringify(revision)}`,
      );
    }
    assert.equal(auditEntries(bus, 'bridge_health_revised').length, 0);
    assert.equal(bus.getStats().bridge.health.state, 'disabled');
    // A later valid call still enables it.
    bus.setBridgeHealth({});
    assert.equal(auditEntries(bus, 'bridge_health_revised').length, 1);
    await bus.close();
  });

  it('an illegal value on a never-enabled bus throws and a later valid call still enables', async () => {
    const bus = new EventBus({ bridge: { transport: failingTransport() } });
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
    ];
    for (const revision of bad) {
      assert.throws(
        () => bus.setBridgeHealth(revision as never),
        RangeError,
        `expected RangeError for ${JSON.stringify(revision)}`,
      );
    }
    assert.equal(auditEntries(bus, 'bridge_health_revised').length, 0);
    assert.equal(bus.getStats().bridge.health.state, 'disabled');
    bus.setBridgeHealth({ intervalMs: 100 });
    assert.deepEqual(auditEntries(bus, 'bridge_health_revised')[0].detail, {
      intervalMs: 100,
      oldIntervalMs: null,
      failureThreshold: 3,
      oldFailureThreshold: null,
    });
    await bus.close();
  });

  it('publishes before the enable are never counted; failures after the enable count from zero', async () => {
    const transport = failingTransport();
    const bus = new EventBus({ bridge: { transport } });
    bus.subscribe('t', () => {});
    // Three mirror failures with no monitor: attempted, never recorded.
    bus.publish('t', 1);
    bus.publish('t', 2);
    bus.publish('t', 3);
    assert.equal(transport.calls, 3);
    assert.deepEqual(bus.getStats().bridge.health, {
      state: 'disabled',
      consecutiveFailures: 0,
    });
    bus.setBridgeHealth({ failureThreshold: 2 });
    assert.deepEqual(bus.getStats().bridge.health, {
      state: 'healthy',
      consecutiveFailures: 0,
    });
    bus.publish('t', 4);
    assert.equal(bus.getStats().bridge.health.consecutiveFailures, 1);
    assert.equal(bus.getStats().bridge.health.state, 'healthy');
    bus.publish('t', 5);
    assert.equal(bus.getStats().bridge.health.consecutiveFailures, 2);
    assert.equal(bus.getStats().bridge.health.state, 'unhealthy');
    assert.equal(auditEntries(bus, 'bridge_unhealthy').length, 1);
    // While unhealthy the mirror is paused, not attempted.
    assert.equal(transport.calls, 5);
    bus.publish('t', 6);
    assert.equal(transport.calls, 5);
    await bus.close();
  });

  it('failures after the enable trip unhealthy at the new threshold and a probe recovers it', async () => {
    const transport = flippableTransport();
    const bus = new EventBus({ bridge: { transport } });
    bus.subscribe('t', () => {});
    bus.setBridgeHealth({ intervalMs: 10, failureThreshold: 2 });
    bus.publish('t', 1);
    bus.publish('t', 2);
    assert.equal(bus.getStats().bridge.health.state, 'unhealthy');
    assert.equal(auditEntries(bus, 'bridge_unhealthy').length, 1);
    // The transport recovers: the next probe succeeds and mirroring resumes.
    transport.failing = false;
    assert.equal(await waitFor(() => bus.getStats().bridge.health.state === 'healthy'), true);
    assert.equal(bus.getStats().bridge.health.consecutiveFailures, 0);
    const recovered = auditEntries(bus, 'bridge_recovered');
    assert.equal(recovered.length, 1);
    assert.equal(recovered[0].detail.transport, 'flippable');
    // Mirroring resumes for new publishes.
    bus.publish('t', 3);
    assert.equal(bus.getStats().bridge.health.state, 'healthy');
    await bus.close();
  });

  it('the enable arms the first probe one full interval after the enable', async () => {
    const clock = controllableClock(5_000);
    const transport = probeCountingTransport();
    const bus = new EventBus({ now: clock.now, bridge: { transport } });
    bus.setBridgeHealth({ intervalMs: 10 });
    // No probe fired as a side effect of the enable itself.
    assert.equal(bus.getStats().bridge.health.lastProbeAt, undefined);
    assert.equal(transport.probeCalls, 0);
    assert.equal(
      await waitFor(() => bus.getStats().bridge.health.lastProbeAt === 5_000),
      true,
    );
    assert.ok(transport.probeCalls >= 1);
    await bus.close();
  });

  it('a revision after the enable merges against the live enabled values (EB-305)', async () => {
    const bus = new EventBus({ bridge: { transport: failingTransport() } });
    bus.setBridgeHealth({ intervalMs: 100 });
    bus.setBridgeHealth({ failureThreshold: 2 });
    let audit = auditEntries(bus, 'bridge_health_revised');
    assert.equal(audit.length, 2);
    assert.deepEqual(audit[1].detail, {
      intervalMs: 100,
      oldIntervalMs: 100,
      failureThreshold: 2,
      oldFailureThreshold: 3,
    });
    // Re-affirming the live tuning is a no-op with no audit.
    bus.setBridgeHealth({ intervalMs: 100, failureThreshold: 2 });
    audit = auditEntries(bus, 'bridge_health_revised');
    assert.equal(audit.length, 2);
    await bus.close();
  });

  it('a second enable call revises the live tuning instead of re-enabling', async () => {
    const bus = new EventBus({ bridge: { transport: failingTransport() } });
    bus.setBridgeHealth({ intervalMs: 100 });
    bus.setBridgeHealth({ intervalMs: 200 });
    const audit = auditEntries(bus, 'bridge_health_revised');
    assert.equal(audit.length, 2);
    // The old side is real now — the enable half ran only once.
    assert.deepEqual(audit[1].detail, {
      intervalMs: 200,
      oldIntervalMs: 100,
      failureThreshold: 3,
      oldFailureThreshold: 3,
    });
    await bus.close();
  });

  it('the enable is live-only: a fresh bus is unconfigured again', async () => {
    const transport = failingTransport();
    const bus = new EventBus({ bridge: { transport } });
    bus.setBridgeHealth({ intervalMs: 100 });
    assert.equal(auditEntries(bus, 'bridge_health_revised').length, 1);
    // A restart restores the unconfigured state: a fresh bus built
    // from the same construction options holds no monitor.
    const fresh = new EventBus({ bridge: { transport: failingTransport() } });
    assert.equal(fresh.getStats().bridge.health.state, 'disabled');
    assert.equal(auditEntries(fresh, 'bridge_health_revised').length, 0);
    // The fresh bus can still be enabled the same way.
    fresh.setBridgeHealth({});
    assert.equal(auditEntries(fresh, 'bridge_health_revised').length, 1);
    await bus.close();
    await fresh.close();
  });

  it('a bus with no bridge at all still refuses', async () => {
    const bus = new EventBus();
    assert.throws(
      () => bus.setBridgeHealth({}),
      (e: unknown) => e instanceof RangeError && /not enabled on this bus/.test((e as Error).message),
    );
    assert.throws(
      () => bus.setBridgeHealth({ intervalMs: 100 }),
      (e: unknown) => e instanceof RangeError && /not enabled on this bus/.test((e as Error).message),
    );
    assert.equal(auditEntries(bus, 'bridge_health_revised').length, 0);
    // Null stays a silent no-op even with no bridge: the null branch
    // sits before the enable gate.
    bus.setBridgeHealth(null as never);
    assert.equal(auditEntries(bus, 'bridge_health_revised').length, 0);
    await bus.close();
  });

  it('a closed bus refuses the enable and the revision with RangeError', async () => {
    const unconfigured = new EventBus({ bridge: { transport: failingTransport() } });
    await unconfigured.close();
    assert.throws(
      () => unconfigured.setBridgeHealth({ intervalMs: 100 }),
      (e: unknown) => e instanceof RangeError && /bus is closed/.test((e as Error).message),
    );
    assert.equal(auditEntries(unconfigured, 'bridge_health_revised').length, 0);

    const configured = new EventBus({
      bridge: { transport: failingTransport(), health: { intervalMs: 5_000, failureThreshold: 2 } },
    });
    await configured.close();
    assert.throws(
      () => configured.setBridgeHealth({ intervalMs: 100 }),
      (e: unknown) => e instanceof RangeError && /bus is closed/.test((e as Error).message),
    );
    assert.equal(auditEntries(configured, 'bridge_health_revised').length, 0);
  });
});
