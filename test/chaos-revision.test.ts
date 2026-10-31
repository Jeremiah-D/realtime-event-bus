/**
 * Bus-level chaos scenario revision (EB-300).
 *
 * `setChaos` replaces the fault-injection scenario list live — no
 * restart, no new injector. Only a bus that enabled `chaos` can be
 * revised; illegal scenarios fail closed with the live scenarios (and
 * their RNG streams) untouched; an identical revision is a no-op with
 * no audit. An empty list disables injection while keeping the
 * injector — and its counters — alive, so a later revision can re-arm
 * it. Each scenario's RNG stream restarts from its seed, so the fault
 * sequence restarts deterministically; the counters are retained, not
 * reset; an already-triggered in-flight fault is unaffected.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { ChaosInjectionError, FaultInjector, type ChaosScenario } from '../src/chaos.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** The default seed (`DEFAULT_SEED` in `src/chaos.ts`): an omitted seed equals this. */
const DEFAULT_SEED = 0x9e3779b9;

describe('chaos scenario revision (EB-300)', () => {
  it('setChaos fails closed when chaos is not enabled', () => {
    const plain = new EventBus();
    assert.throws(() => plain.setChaos([{ type: 'error', target: 'admission', rate: 1 }]), RangeError);
    assert.equal(plain.getSubscriptionAudit({ action: 'chaos_revised' }).length, 0);

    // Empty chaos at construction builds no injector at all.
    for (const bus of [new EventBus({ chaos: {} }), new EventBus({ chaos: { scenarios: [] } })]) {
      assert.throws(() => bus.setChaos([{ type: 'error', target: 'admission', rate: 1 }]), RangeError);
      assert.equal(bus.getSubscriptionAudit({ action: 'chaos_revised' }).length, 0);
      assert.equal(bus.getStats().chaos, undefined);
    }
  });

  it('setChaos rejects illegal scenarios and leaves the live scenarios untouched', () => {
    const bus = new EventBus({ chaos: { scenarios: [{ type: 'error', target: 'admission', rate: 1, seed: 4 }] } });
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
    }
    // The failed calls changed nothing: the live scenario still faults
    // every admission, and no audit was written.
    assert.throws(() => bus.publish('t', 'a'), ChaosInjectionError);
    assert.equal(bus.getStats().chaos?.byTarget.admission, 1);
    assert.equal(bus.getSubscriptionAudit({ action: 'chaos_revised' }).length, 0);
    // And the bus still revises afterwards.
    bus.setChaos([{ type: 'drop', target: 'admission', rate: 1, seed: 3 }]);
    assert.equal(bus.publish('t', 'b'), 0);
  });

  it('identical revision and absent revision are no-ops with no audit; omitted seed equals the default seed', () => {
    const bus = new EventBus({ chaos: { scenarios: [{ type: 'error', target: 'admission', rate: 1, seed: 4 }] } });
    bus.setChaos([{ type: 'error', target: 'admission', rate: 1, seed: 4 }]);
    bus.setChaos(null as never);
    assert.equal(bus.getSubscriptionAudit({ action: 'chaos_revised' }).length, 0);
    // The untouched scenarios still govern — and the RNG stream kept
    // running rather than restarting (a no-op retunes nothing).
    assert.throws(() => bus.publish('t', 'a'), ChaosInjectionError);

    // A scenario spelled without a seed is the default seed.
    const defaulted = new EventBus({ chaos: { scenarios: [{ type: 'error', target: 'admission', rate: 1 }] } });
    defaulted.setChaos([{ type: 'error', target: 'admission', rate: 1 }]);
    defaulted.setChaos([{ type: 'error', target: 'admission', rate: 1, seed: DEFAULT_SEED }]);
    assert.equal(defaulted.getSubscriptionAudit({ action: 'chaos_revised' }).length, 0);
  });

  it('scenario revision governs from the next evaluation, audit carries old and new', () => {
    const bus = new EventBus({ chaos: { scenarios: [{ type: 'error', target: 'admission', rate: 0, seed: 4 }] } });
    let calls = 0;
    bus.subscribe('t', () => {
      calls += 1;
    });
    assert.equal(bus.publish('t', 'a'), 1);
    bus.setChaos([{ type: 'error', target: 'admission', rate: 1, seed: 4 }]);
    assert.throws(() => bus.publish('t', 'b'), ChaosInjectionError);
    const audit = bus.getSubscriptionAudit({ action: 'chaos_revised' });
    assert.equal(audit.length, 1);
    assert.deepEqual(audit[0].detail, {
      scenarios: [{ type: 'error', target: 'admission', rate: 1, seed: 4 }],
      oldScenarios: [{ type: 'error', target: 'admission', rate: 0, seed: 4 }],
    });
    assert.equal(calls, 0);
  });

  it('empty list disables injection, counters retained, later revision re-arms', async () => {
    const bus = new EventBus({ chaos: { scenarios: [{ type: 'drop', target: 'admission', rate: 1, seed: 3 }] } });
    let calls = 0;
    bus.subscribe('t', () => {
      calls += 1;
    });
    assert.equal(bus.publish('t', 'a'), 0);
    assert.equal(bus.publish('t', 'b'), 0);
    await flush();
    assert.equal(calls, 0);
    assert.equal(bus.getStats().chaos?.injected, 2);
    // Disabling injection is a real revision, not a no-op: it audits.
    bus.setChaos([]);
    const audit = bus.getSubscriptionAudit({ action: 'chaos_revised' });
    assert.equal(audit.length, 1);
    assert.deepEqual(audit[0].detail, {
      scenarios: [],
      oldScenarios: [{ type: 'drop', target: 'admission', rate: 1, seed: 3 }],
    });
    // The injector stays alive: stats are still present, the counter
    // is retained, and publishes go through again.
    assert.equal(bus.publish('t', 'c'), 1);
    await flush();
    assert.equal(calls, 1);
    assert.equal(bus.getStats().chaos?.injected, 2);
    assert.notEqual(bus.getStats().chaos, undefined);
    // A later revision re-arms the same injector.
    bus.setChaos([{ type: 'error', target: 'admission', rate: 1, seed: 4 }]);
    assert.throws(() => bus.publish('t', 'd'), ChaosInjectionError);
    assert.equal(bus.getStats().chaos?.injected, 3);
  });

  it('retune restarts the fault sequence deterministically from the seed', () => {
    const scenarios = (seed: number): ChaosScenario[] => [
      { type: 'drop', target: 'admission', rate: 0.5, seed },
    ];
    const bus = new EventBus({ chaos: { scenarios: scenarios(99) } });
    bus.subscribe('t', () => {});
    const run = () => {
      const accepted: number[] = [];
      for (let i = 0; i < 20; i += 1) accepted.push(bus.publish('t', i));
      return accepted;
    };
    const first = run();
    // A different seed restarts the stream from that seed.
    bus.setChaos(scenarios(100));
    run();
    // Back to seed 99: the fault sequence reproduces bit for bit.
    bus.setChaos(scenarios(99));
    assert.deepEqual(run(), first);
    // Three revisions audited (100, then 99 — the identical 99 in the
    // first run was the construction state, not a revision).
    assert.equal(bus.getSubscriptionAudit({ action: 'chaos_revised' }).length, 2);
  });

  it('armed drain-latency fault is unaffected by a retune', async () => {
    const bus = new EventBus({
      chaos: { scenarios: [{ type: 'latency', target: 'drain', rate: 1, seed: 7, latencyMs: 40 }] },
    });
    const seen: unknown[] = [];
    bus.subscribe('t', (msg) => {
      seen.push(msg.payload);
    });
    bus.publish('t', 'hello');
    await flush();
    // The first drain round deferred instead of delivering.
    assert.deepEqual(seen, []);
    // Retune mid-deferral to an empty list: future injections stop,
    // but the already-armed deferral still fires its deferred drain.
    bus.setChaos([]);
    await sleep(120);
    await flush();
    assert.deepEqual(seen, ['hello']);
    assert.equal(bus.getStats().chaos?.byTarget.drain ?? 0, 1);
  });

  it('injector retune rebuilds RNG streams and retains counters; current() reports the live list', () => {
    const injector = new FaultInjector([{ type: 'drop', target: 'admission', rate: 1, seed: 3 }]);
    assert.deepEqual(injector.current(), [{ type: 'drop', target: 'admission', rate: 1, seed: 3 }]);
    assert.equal(injector.faultFor('admission')?.type, 'drop');
    assert.equal(injector.snapshot().injected, 1);
    injector.retune([{ type: 'error', target: 'admission', rate: 1, seed: 4 }]);
    assert.deepEqual(injector.current(), [{ type: 'error', target: 'admission', rate: 1, seed: 4 }]);
    assert.equal(injector.faultFor('admission')?.type, 'error');
    // The counter was retained, not reset, across the retune.
    assert.equal(injector.snapshot().injected, 2);
    injector.retune([]);
    assert.deepEqual(injector.current(), []);
    assert.equal(injector.faultFor('admission'), null);
    assert.equal(injector.snapshot().injected, 2);
  });
});
