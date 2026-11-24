/**
 * Bus-level chaos live enable (EB-552):
 * `setChaos` on a bus that never enabled `chaos` installs a fresh
 * injector live — no restart, no lost subscriptions — with the
 * scenarios resolved through the construction-time rule
 * (`assertValidChaosScenario`). The fresh injector's counters start
 * at zero, so publishes before the enable were never evaluated, and
 * the enable is audited like any revision, with `oldScenarios`
 * `null`. An empty list cannot enable, since there is nothing to
 * build; `null` is a silent no-op; and a closed bus refuses the
 * enable and the revision. The enable is live-only: a restart
 * restores the unconfigured state.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { ChaosInjectionError, type ChaosScenario } from '../src/chaos.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** The default seed (`DEFAULT_SEED` in `src/chaos.ts`): an omitted seed equals this. */
const DEFAULT_SEED = 0x9e3779b9;

describe('chaos live enable (EB-552)', () => {
  it('enables on a never-configured bus: the next publish faults, counters start at zero', async () => {
    const bus = new EventBus();
    bus.subscribe('t', () => {});
    assert.equal(bus.getStats().chaos, undefined);
    // Publishes before the enable were never evaluated.
    assert.equal(bus.publish('t', 'before'), 1);
    bus.setChaos([{ type: 'error', target: 'admission', rate: 1 }]);
    assert.notEqual(bus.getStats().chaos, undefined);
    assert.throws(() => bus.publish('t', 'a'), ChaosInjectionError);
    // Only the post-enable fault counts — nothing retroactive.
    assert.equal(bus.getStats().chaos?.injected, 1);
    assert.equal(bus.getStats().chaos?.byTarget.admission, 1);
    await bus.close();
  });

  it('getStats().chaos appears after the enable with live counters', async () => {
    const bus = new EventBus();
    bus.subscribe('t', () => {});
    bus.setChaos([{ type: 'drop', target: 'admission', rate: 1, seed: 5 }]);
    assert.deepEqual(bus.getStats().chaos, {
      injected: 0,
      byTarget: { handler: 0, admission: 0, drain: 0 },
    });
    assert.equal(bus.publish('t', 'a'), 0);
    assert.equal(bus.publish('t', 'b'), 0);
    assert.deepEqual(bus.getStats().chaos, {
      injected: 2,
      byTarget: { handler: 0, admission: 2, drain: 0 },
    });
    await bus.close();
  });

  it('an enabled scenario with an explicit seed reproduces the construction-time sequence', async () => {
    const scenarios = (seed: number): ChaosScenario[] => [
      { type: 'drop', target: 'admission', rate: 0.5, seed },
    ];
    const built = new EventBus({ chaos: { scenarios: scenarios(99) } });
    built.subscribe('t', () => {});
    const enabled = new EventBus();
    enabled.subscribe('t', () => {});
    enabled.setChaos(scenarios(99));
    const run = (bus: EventBus) => {
      const accepted: number[] = [];
      for (let i = 0; i < 20; i += 1) accepted.push(bus.publish('t', i));
      return accepted;
    };
    assert.deepEqual(run(enabled), run(built));
    await built.close();
    await enabled.close();
  });

  it('an empty list cannot enable: the bus stays unconfigured and can still be enabled', async () => {
    const bus = new EventBus();
    bus.subscribe('t', () => {});
    assert.throws(() => bus.setChaos([]), /chaos is not enabled/);
    assert.equal(bus.getStats().chaos, undefined);
    assert.equal(bus.getSubscriptionAudit({ action: 'chaos_revised' }).length, 0);
    assert.equal(bus.publish('t', 'a'), 1);
    bus.setChaos([{ type: 'error', target: 'admission', rate: 1 }]);
    assert.throws(() => bus.publish('t', 'b'), ChaosInjectionError);
    await bus.close();
  });

  it('null on a never-enabled bus is a silent no-op with no audit', async () => {
    const bus = new EventBus();
    bus.setChaos(null as never);
    assert.equal(bus.getStats().chaos, undefined);
    assert.equal(bus.getSubscriptionAudit({ action: 'chaos_revised' }).length, 0);
    // Still unconfigured — and still enableable.
    bus.setChaos([{ type: 'error', target: 'admission', rate: 1 }]);
    assert.notEqual(bus.getStats().chaos, undefined);
    await bus.close();
  });

  it('illegal scenarios are rejected and leave the bus unconfigured', async () => {
    const bus = new EventBus();
    bus.subscribe('t', () => {});
    const bad: Array<unknown> = [
      'nope',
      42,
      {},
      [{ type: 'nope', target: 'admission', rate: 1 }],
      [{ type: 'error', target: 'nope', rate: 1 }],
      // 'error' is not supported at the 'drain' target.
      [{ type: 'error', target: 'drain', rate: 1 }],
      // 'latency' is not supported at the 'admission' target.
      [{ type: 'latency', target: 'admission', rate: 1, latencyMs: 10 }],
      [{ type: 'error', target: 'admission', rate: 2 }],
      [{ type: 'error', target: 'admission', rate: -0.1 }],
      [{ type: 'error', target: 'admission', rate: Number.NaN }],
      [{ type: 'error', target: 'admission', rate: 1, seed: Number.NaN }],
      // 'latency' without latencyMs.
      [{ type: 'latency', target: 'drain', rate: 1 }],
      // latencyMs on a non-latency fault.
      [{ type: 'error', target: 'admission', rate: 1, latencyMs: 10 }],
      [{ type: 'latency', target: 'drain', rate: 1, latencyMs: 0 }],
      [null],
    ];
    for (const revision of bad) {
      assert.throws(
        () => bus.setChaos(revision as never),
        RangeError,
        `expected RangeError for ${JSON.stringify(revision)}`,
      );
      assert.equal(bus.getStats().chaos, undefined);
      assert.equal(bus.getSubscriptionAudit({ action: 'chaos_revised' }).length, 0);
    }
    // Publishes still go through, and a later valid call still enables.
    assert.equal(bus.publish('t', 'a'), 1);
    bus.setChaos([{ type: 'error', target: 'admission', rate: 1 }]);
    assert.throws(() => bus.publish('t', 'b'), ChaosInjectionError);
    await bus.close();
  });

  it('a revision after the enable replaces scenarios and retains counters', async () => {
    const bus = new EventBus();
    let calls = 0;
    bus.subscribe('t', () => {
      calls += 1;
    });
    bus.setChaos([{ type: 'drop', target: 'admission', rate: 1, seed: 3 }]);
    assert.equal(bus.publish('t', 'a'), 0);
    assert.equal(bus.publish('t', 'b'), 0);
    await flush();
    assert.equal(calls, 0);
    assert.equal(bus.getStats().chaos?.injected, 2);
    bus.setChaos([{ type: 'error', target: 'admission', rate: 1, seed: 4 }]);
    assert.throws(() => bus.publish('t', 'c'), ChaosInjectionError);
    assert.equal(bus.getStats().chaos?.injected, 3);
    const audit = bus.getSubscriptionAudit({ action: 'chaos_revised' });
    assert.equal(audit.length, 2);
    assert.deepEqual(audit[1].detail, {
      scenarios: [{ type: 'error', target: 'admission', rate: 1, seed: 4 }],
      oldScenarios: [{ type: 'drop', target: 'admission', rate: 1, seed: 3 }],
    });
    await bus.close();
  });

  it('re-affirming the enabled scenarios is a no-op with no audit', async () => {
    const bus = new EventBus();
    bus.subscribe('t', () => {});
    bus.setChaos([{ type: 'error', target: 'admission', rate: 1, seed: 4 }]);
    bus.setChaos([{ type: 'error', target: 'admission', rate: 1, seed: 4 }]);
    assert.equal(bus.getSubscriptionAudit({ action: 'chaos_revised' }).length, 1);
    assert.throws(() => bus.publish('t', 'a'), ChaosInjectionError);
    await bus.close();
  });

  it('a restart restores the unconfigured state', async () => {
    const bus = new EventBus();
    bus.subscribe('t', () => {});
    bus.setChaos([{ type: 'error', target: 'admission', rate: 1 }]);
    assert.throws(() => bus.publish('t', 'a'), ChaosInjectionError);
    await bus.close();
    // A fresh bus built from the same construction options holds no
    // injector: publishes go through again, and it can still be enabled.
    const fresh = new EventBus();
    fresh.subscribe('t', () => {});
    assert.equal(fresh.getStats().chaos, undefined);
    assert.equal(fresh.getSubscriptionAudit({ action: 'chaos_revised' }).length, 0);
    assert.equal(fresh.publish('t', 'a'), 1);
    fresh.setChaos([{ type: 'error', target: 'admission', rate: 1 }]);
    assert.throws(() => fresh.publish('t', 'b'), ChaosInjectionError);
    await fresh.close();
  });

  it('the enable audit carries the canonical scenarios and oldScenarios null', async () => {
    const bus = new EventBus();
    bus.setChaos([
      { type: 'latency', target: 'drain', rate: 0.5, latencyMs: 25 },
      { type: 'error', target: 'handler', rate: 1, seed: 7 },
    ]);
    const audit = bus.getSubscriptionAudit({ action: 'chaos_revised' });
    assert.equal(audit.length, 1);
    assert.deepEqual(audit[0].detail, {
      scenarios: [
        { type: 'latency', target: 'drain', rate: 0.5, seed: DEFAULT_SEED, latencyMs: 25 },
        { type: 'error', target: 'handler', rate: 1, seed: 7 },
      ],
      oldScenarios: null,
    });
    await bus.close();
  });

  it('a closed bus refuses the enable and the revision', async () => {
    const neverEnabled = new EventBus();
    await neverEnabled.close();
    assert.throws(
      () => neverEnabled.setChaos([{ type: 'error', target: 'admission', rate: 1 }]),
      (e: unknown) => e instanceof RangeError && /bus is closed/.test((e as Error).message),
    );
    const configured = new EventBus({
      chaos: { scenarios: [{ type: 'error', target: 'admission', rate: 1 }] },
    });
    await configured.close();
    assert.throws(
      () => configured.setChaos([{ type: 'drop', target: 'admission', rate: 1 }]),
      (e: unknown) => e instanceof RangeError && /bus is closed/.test((e as Error).message),
    );
  });
});
