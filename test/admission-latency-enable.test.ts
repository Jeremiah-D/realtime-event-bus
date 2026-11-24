/**
 * Admission-latency live enable (EB-549):
 * `setAdmissionLatency` on a bus that never enabled `publishLatency`
 * installs the tracker live — no restart, no lost subscriptions —
 * with the revision resolved through the construction-time rule
 * (`resolveAdmissionLatencyOptions`: `{}` takes every default, a
 * partial object falls back per field). The fresh tracker starts with
 * empty windows — publishes admitted before the enable were never
 * sampled, so nothing is sampled retroactively — and it is live-only:
 * a restart restores the unconfigured state. The enable is audited
 * like any revision, with `oldWindowSize` / `oldGateWindowSize` /
 * `oldSlowAdmissionThresholdMs` `null`.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

/**
 * Arms a schema validator that consumes `advanceMs` of bus-clock time
 * per publish, so each publish's total admission span is exactly
 * `advanceMs` (and the schema gate sample matches it). `advances` may
 * be a per-publish sequence; once exhausted it repeats the last value.
 */
function armClockBurningValidator(
  bus: EventBus,
  clock: { nowMs: number },
  advances: number[],
): void {
  let i = 0;
  bus.setTopicSchema('t', () => {
    clock.nowMs += advances[Math.min(i, advances.length - 1)];
    i += 1;
    return true;
  });
}

function totalFor(bus: EventBus, topic = 't') {
  return bus.getStats().publishLatency.find((s) => s.topic === topic)!;
}

function gateFor(bus: EventBus, gate: string, topic = 't') {
  return bus
    .getStats()
    .publishLatencyGates.find((g) => g.topic === topic && g.gate === gate)!;
}

function auditEntries(bus: EventBus) {
  return bus.getSubscriptionAudit({ action: 'admission_latency_revised' });
}

describe('admission-latency live enable (EB-549)', () => {
  it('the first enable with {} takes the construction defaults', () => {
    const bus = new EventBus();
    assert.deepEqual(bus.getStats().publishLatency, []);
    bus.setAdmissionLatency({});
    const audit = auditEntries(bus);
    assert.equal(audit.length, 1);
    assert.equal(audit[0].actor, 'system');
    assert.deepEqual(audit[0].detail, {
      windowSize: 1024,
      oldWindowSize: null,
      gateWindowSize: 1024,
      oldGateWindowSize: null,
      slowAdmissionThresholdMs: 100,
      oldSlowAdmissionThresholdMs: null,
    });
  });

  it('a partial enable falls back per field', () => {
    const bus = new EventBus();
    bus.setAdmissionLatency({ windowSize: 5 });
    assert.deepEqual(auditEntries(bus)[0].detail, {
      windowSize: 5,
      oldWindowSize: null,
      gateWindowSize: 1024,
      oldGateWindowSize: null,
      slowAdmissionThresholdMs: 100,
      oldSlowAdmissionThresholdMs: null,
    });
    const second = new EventBus();
    second.setAdmissionLatency({ slowAdmissionThresholdMs: 25 });
    assert.deepEqual(auditEntries(second)[0].detail, {
      windowSize: 1024,
      oldWindowSize: null,
      gateWindowSize: 1024,
      oldGateWindowSize: null,
      slowAdmissionThresholdMs: 25,
      oldSlowAdmissionThresholdMs: null,
    });
  });

  it('null on a never-enabled bus is a silent no-op with no audit', () => {
    const bus = new EventBus();
    bus.setAdmissionLatency(null);
    assert.equal(auditEntries(bus).length, 0);
    assert.deepEqual(bus.getStats().publishLatency, []);
    // The bus is still unconfigured: a later valid call enables it.
    bus.setAdmissionLatency({ windowSize: 4 });
    assert.equal(auditEntries(bus).length, 1);
  });

  it('a non-object revision on a never-enabled bus throws and leaves the bus unconfigured', () => {
    const bus = new EventBus();
    const bad: Array<unknown> = ['nope', 42, ['x']];
    for (const revision of bad) {
      assert.throws(
        () => bus.setAdmissionLatency(revision as never),
        RangeError,
        `expected RangeError for ${JSON.stringify(revision)}`,
      );
    }
    assert.equal(auditEntries(bus).length, 0);
    assert.deepEqual(bus.getStats().publishLatency, []);
    // A later valid call still enables it.
    bus.setAdmissionLatency({});
    assert.equal(auditEntries(bus).length, 1);
  });

  it('an illegal value on a never-enabled bus throws and a later valid call still enables', () => {
    const bus = new EventBus();
    const bad: Array<unknown> = [
      { windowSize: 0 },
      { windowSize: -1 },
      { windowSize: 1.5 },
      { gateWindowSize: 0 },
      { gateWindowSize: -1 },
      { gateWindowSize: 2.5 },
      { slowAdmissionThresholdMs: -1 },
      { slowAdmissionThresholdMs: Number.NaN },
      { slowAdmissionThresholdMs: Number.POSITIVE_INFINITY },
    ];
    for (const revision of bad) {
      assert.throws(
        () => bus.setAdmissionLatency(revision as never),
        RangeError,
        `expected RangeError for ${JSON.stringify(revision)}`,
      );
    }
    assert.equal(auditEntries(bus).length, 0);
    assert.deepEqual(bus.getStats().publishLatency, []);
    bus.setAdmissionLatency({ windowSize: 4 });
    assert.equal(auditEntries(bus).length, 1);
    assert.deepEqual(auditEntries(bus)[0].detail, {
      windowSize: 4,
      oldWindowSize: null,
      gateWindowSize: 1024,
      oldGateWindowSize: null,
      slowAdmissionThresholdMs: 100,
      oldSlowAdmissionThresholdMs: null,
    });
  });

  it('publishes before the enable are never sampled; the next publish after the enable is', () => {
    const clock = controllableClock();
    const bus = new EventBus({ now: clock.now });
    bus.subscribe('t', () => {});
    armClockBurningValidator(bus, clock, [10]);
    for (let i = 0; i < 3; i++) bus.publish('t', i);
    assert.deepEqual(bus.getStats().publishLatency, []);
    assert.deepEqual(bus.getStats().publishLatencyGates, []);
    bus.setAdmissionLatency({ windowSize: 4, gateWindowSize: 2, slowAdmissionThresholdMs: 50 });
    // Still nothing sampled: the enable itself records no sample.
    assert.deepEqual(bus.getStats().publishLatency, []);
    bus.publish('t', 3);
    assert.equal(totalFor(bus).samples, 1);
    assert.equal(totalFor(bus).p50Ms, 10);
    assert.equal(totalFor(bus).slowCount, 0);
    assert.equal(gateFor(bus, 'schema').samples, 1);
    assert.equal(gateFor(bus, 'schema').p50Ms, 10);
  });

  it('the enabled window bounds govern from the next publish', () => {
    const clock = controllableClock();
    const bus = new EventBus({ now: clock.now });
    bus.subscribe('t', () => {});
    armClockBurningValidator(bus, clock, [10, 20, 30]);
    bus.setAdmissionLatency({ windowSize: 2, gateWindowSize: 2, slowAdmissionThresholdMs: 50 });
    for (let i = 0; i < 3; i++) bus.publish('t', i);
    // Only the two newest total spans survive the enabled window.
    assert.equal(totalFor(bus).samples, 2);
    assert.equal(totalFor(bus).p50Ms, 20);
    assert.equal(totalFor(bus).p95Ms, 30);
    assert.equal(totalFor(bus).p99Ms, 30);
    assert.equal(totalFor(bus).minMs, 20);
    assert.equal(totalFor(bus).maxMs, 30);
    assert.equal(totalFor(bus).meanMs, 25);
    assert.equal(gateFor(bus, 'schema').samples, 2);
    assert.equal(gateFor(bus, 'schema').p50Ms, 20);
  });

  it('the enabled slow threshold counts slow admissions from the next sample', () => {
    const clock = controllableClock();
    const bus = new EventBus({ now: clock.now });
    bus.subscribe('t', () => {});
    armClockBurningValidator(bus, clock, [10, 20, 30]);
    bus.setAdmissionLatency({ slowAdmissionThresholdMs: 15 });
    for (let i = 0; i < 3; i++) bus.publish('t', i);
    assert.equal(totalFor(bus).samples, 3);
    // 10ms is under the enabled 15ms threshold; 20ms and 30ms count.
    assert.equal(totalFor(bus).slowCount, 2);
    assert.equal(totalFor(bus).p99Ms, 30);
    assert.equal(bus.getStats().slowAdmissionTopics[0].slowCount, 2);
  });

  it('a revision after the enable merges against the live enabled values (EB-304)', () => {
    const clock = controllableClock();
    const bus = new EventBus({ now: clock.now });
    bus.subscribe('t', () => {});
    armClockBurningValidator(bus, clock, [10]);
    bus.setAdmissionLatency({ windowSize: 4 });
    bus.setAdmissionLatency({ gateWindowSize: 2 });
    let audit = auditEntries(bus);
    assert.equal(audit.length, 2);
    assert.deepEqual(audit[1].detail, {
      windowSize: 4,
      oldWindowSize: 4,
      gateWindowSize: 2,
      oldGateWindowSize: 1024,
      slowAdmissionThresholdMs: 100,
      oldSlowAdmissionThresholdMs: 100,
    });
    // Re-affirming the live tuning is a no-op with no audit.
    bus.setAdmissionLatency({ windowSize: 4, gateWindowSize: 2, slowAdmissionThresholdMs: 100 });
    audit = auditEntries(bus);
    assert.equal(audit.length, 2);
    // The enabled tuning still governs.
    bus.publish('t', 1);
    assert.equal(totalFor(bus).samples, 1);
    assert.equal(gateFor(bus, 'schema').samples, 1);
  });

  it('a second enable call revises the live tuning instead of re-enabling', () => {
    const bus = new EventBus();
    bus.setAdmissionLatency({ windowSize: 5 });
    bus.setAdmissionLatency({ windowSize: 8 });
    const audit = auditEntries(bus);
    assert.equal(audit.length, 2);
    // The old side is real now — the enable half ran only once.
    assert.deepEqual(audit[1].detail, {
      windowSize: 8,
      oldWindowSize: 5,
      gateWindowSize: 1024,
      oldGateWindowSize: 1024,
      slowAdmissionThresholdMs: 100,
      oldSlowAdmissionThresholdMs: 100,
    });
  });

  it('the enable is live-only: a fresh bus is unconfigured again', () => {
    const clock = controllableClock();
    const bus = new EventBus({ now: clock.now });
    bus.setAdmissionLatency({ windowSize: 4 });
    assert.equal(auditEntries(bus).length, 1);
    // A restart restores the unconfigured state: a fresh bus built
    // from the same construction options holds no tracker.
    const fresh = new EventBus({ now: clock.now });
    assert.deepEqual(fresh.getStats().publishLatency, []);
    assert.equal(auditEntries(fresh).length, 0);
    // The fresh bus can still be enabled the same way.
    fresh.setAdmissionLatency({});
    assert.equal(auditEntries(fresh).length, 1);
  });

  it('a closed bus refuses the enable and the revision with RangeError', async () => {
    const unconfigured = new EventBus();
    await unconfigured.close();
    assert.throws(
      () => unconfigured.setAdmissionLatency({ windowSize: 10 }),
      (e: unknown) => e instanceof RangeError && /bus is closed/.test((e as Error).message),
    );
    assert.equal(auditEntries(unconfigured).length, 0);

    const configured = new EventBus({
      publishLatency: { windowSize: 4, gateWindowSize: 2, slowAdmissionThresholdMs: 50 },
    });
    await configured.close();
    assert.throws(
      () => configured.setAdmissionLatency({ windowSize: 10 }),
      (e: unknown) => e instanceof RangeError && /bus is closed/.test((e as Error).message),
    );
    assert.equal(auditEntries(configured).length, 0);
  });
});
