/**
 * CEP absence-step window live revision (EB-382).
 *
 * `updateCepAbsenceWindow` retunes one absence step's explicit
 * `absenceMs` without a clear + define rebuild: the new value governs
 * from the next arming, an already-armed deadline keeps its armed
 * window (EB-379), a chained absence step re-arms at the new value,
 * and `null` clears back to following the live `withinMs`. Unknown
 * ids, out-of-range steps, non-absence steps, and illegal values fail
 * closed with the live value untouched; a same-value revision is a
 * no-op with no audit.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage } from '../src/bus.ts';
import { CepEngine, type SequenceMatchEvent } from '../src/cep.ts';

/** Yields until the bus's async flush has delivered. */
const flush = () => new Promise<void>((resolve) => setTimeout(resolve, 20));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

function collect(bus: EventBus, pattern = '**'): BusMessage[] {
  const seen: BusMessage[] = [];
  bus.subscribe(pattern, (msg) => {
    seen.push(msg);
  });
  return seen;
}

/**
 * Deterministic engine harness (same precedent as
 * cep-absence.test.ts): injected fake timers plus a controllable bus
 * clock, so an armed deadline is exact instead of timing luck.
 */
function fakeEngine() {
  let now = 1_000;
  const timers: Array<() => void> = [];
  const matches: SequenceMatchEvent[] = [];
  const engine = new CepEngine({
    now: () => now,
    compile: (p: string) => new RegExp(`^${p.replace(/\./g, '\\.')}$`),
    emitMatch: (_id, match) => {
      matches.push(match);
    },
    setTimer: (fn) => {
      timers.push(fn);
      return timers.length;
    },
    clearTimer: () => {},
  });
  return {
    engine,
    matches,
    timers,
    advance: (ms: number) => {
      now += ms;
    },
    observe: (topic: string, key = 'k') =>
      engine.observe({ topic, seq: 1, key, payload: 1 } as never),
  };
}

test('updateAbsenceWindow rejects unknown id / out-of-range step / non-absence step / illegal value, live untouched', () => {
  const { engine } = fakeEngine();
  engine.define({
    id: 'p',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: 50 }],
    withinMs: 10_000,
  });
  assert.throws(() => engine.updateAbsenceWindow('ghost', 1, 80), RangeError);
  assert.throws(() => engine.updateAbsenceWindow('p', 2, 80), RangeError);
  assert.throws(() => engine.updateAbsenceWindow('p', -1, 80), RangeError);
  assert.throws(() => engine.updateAbsenceWindow('p', 1.5, 80), RangeError);
  assert.throws(() => engine.updateAbsenceWindow('p', 0, 80), RangeError);
  for (const bad of [0, -5, Number.NaN, Infinity, 'x', undefined]) {
    assert.throws(
      () => engine.updateAbsenceWindow('p', 1, bad as never),
      RangeError,
      `expected RangeError for ${String(bad)}`,
    );
  }
  // Every failed call left the live value exactly as defined.
  assert.equal(engine.getAbsenceWindow('p', 1), 50);
  assert.throws(() => engine.getAbsenceWindow('ghost', 1), RangeError);
  assert.throws(() => engine.getAbsenceWindow('p', 2), RangeError);
  assert.throws(() => engine.getAbsenceWindow('p', 0), RangeError);
});

test('updateCepAbsenceWindow fails closed at the bus with no patterns or a bad target, and audits nothing', () => {
  const bus = new EventBus();
  assert.throws(() => bus.updateCepAbsenceWindow('nope', 1, 80), RangeError);
  assert.throws(() => bus.updateCepAbsenceWindow('', 1, 80), RangeError);
  assert.throws(() => bus.getCepAbsenceWindow('nope', 1), RangeError);
  bus.defineSequence({
    id: 'p',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: 50 }],
    withinMs: 1000,
  });
  assert.throws(() => bus.updateCepAbsenceWindow('ghost', 1, 80), RangeError);
  assert.throws(() => bus.updateCepAbsenceWindow('p', 2, 80), RangeError);
  assert.throws(() => bus.updateCepAbsenceWindow('p', 0, 80), RangeError);
  assert.throws(() => bus.updateCepAbsenceWindow('p', 1, 0), RangeError);
  assert.throws(() => bus.updateCepAbsenceWindow('p', 1, -1), RangeError);
  assert.throws(() => bus.getCepAbsenceWindow('p', 0), RangeError);
  assert.equal(bus.getCepAbsenceWindow('p', 1), 50);
  assert.equal(bus.getSubscriptionAudit({ action: 'cep_absence_window_revised' }).length, 0);
});

test('tightening the window: a new partial derives at the new window', () => {
  const { engine, matches, timers, advance, observe } = fakeEngine();
  engine.define({
    id: 'p',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: 100 }],
    withinMs: 10_000,
  });
  const result = engine.updateAbsenceWindow('p', 1, 50);
  assert.deepEqual(result, { changed: true, stepIndex: 1, absenceMs: 50, oldAbsenceMs: 100 });
  observe('a');
  assert.equal(timers.length, 1);
  // Past the new 50 ms window, inside the old 100 ms one.
  advance(60);
  timers[0]();
  assert.equal(matches.length, 1);
  assert.equal(matches[0].absences![0].windowMs, 50);
  assert.equal(engine.snapshot().absenceMatches, 1);
});

test('loosening the window: a new partial still waits at the new window', () => {
  const { engine, matches, advance, observe } = fakeEngine();
  engine.define({
    id: 'p',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: 50 }],
    withinMs: 10_000,
  });
  engine.updateAbsenceWindow('p', 1, 200);
  observe('a');
  // Past the old 50 ms window, inside the new 200 ms one: the awaited
  // event still arrives in time and kills the partial.
  advance(60);
  observe('b');
  assert.equal(matches.length, 0);
  assert.equal(engine.snapshot().partialMatches, 0);
  assert.equal(engine.snapshot().absenceMatches, 0);
});

test('an already-armed deadline keeps its armed window (EB-379)', () => {
  const { engine, matches, timers, advance, observe } = fakeEngine();
  engine.define({
    id: 'p',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: 100 }],
    withinMs: 10_000,
  });
  observe('a', 'k1');
  observe('a', 'k2');
  engine.updateAbsenceWindow('p', 1, 50);
  // Past the new 50 ms window, inside the armed 100 ms one.
  advance(60);
  // k1: the awaited event still arrives before the armed deadline and
  // kills the partial — the revision did not rewrite the armed window.
  observe('b', 'k1');
  assert.equal(matches.length, 0);
  assert.equal(engine.snapshot().partialMatches, 1);
  // k2: the armed timer still derives at the armed 100 ms value.
  timers[1]();
  assert.equal(matches.length, 1);
  assert.equal(matches[0].absences![0].windowMs, 100);
  assert.equal(engine.snapshot().partialMatches, 0);
});

test('null clears the explicit value: the next arming follows the live withinMs', () => {
  const { engine, matches, timers, advance, observe } = fakeEngine();
  engine.define({
    id: 'p',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: 50 }],
    withinMs: 1000,
  });
  const result = engine.updateAbsenceWindow('p', 1, null);
  assert.deepEqual(result, { changed: true, stepIndex: 1, absenceMs: undefined, oldAbsenceMs: 50 });
  assert.equal(engine.getAbsenceWindow('p', 1), undefined);
  observe('a', 'k1');
  observe('a', 'k2');
  // Past the old 50 ms window, far inside the live 1000 ms one.
  advance(60);
  observe('b', 'k1');
  assert.equal(matches.length, 0, 'expected the cleared window to still be waiting');
  // The second partial waits the full live window before deriving.
  advance(1040);
  timers[1]();
  assert.equal(matches.length, 1);
  assert.equal(matches[0].absences![0].windowMs, 1000);
});

test('EB-297 shrink: a revised absenceMs wider than the live withinMs still drops', () => {
  const { engine, matches, timers, advance, observe } = fakeEngine();
  engine.define({
    id: 'p',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: 50 }],
    withinMs: 10_000,
  });
  engine.updateAbsenceWindow('p', 1, 2000);
  assert.equal(engine.getAbsenceWindow('p', 1), 2000);
  engine.update('p', { withinMs: 1000 });
  observe('a');
  advance(2100);
  timers[0]();
  // The armed 2000 ms window no longer fits the live 1000 ms window:
  // the EB-379 guard drops the partial instead of deriving.
  assert.equal(matches.length, 0);
  assert.equal(engine.snapshot().absenceMatches, 0);
  assert.equal(engine.snapshot().matches, 0);
  assert.equal(engine.snapshot().partialMatches, 0);
});

test('a chained absence step re-arms at the revised value', () => {
  const { engine, matches, timers, advance, observe } = fakeEngine();
  engine.define({
    id: 'p',
    steps: [
      { topic: 'a' },
      { topic: 'b', absence: true, absenceMs: 100 },
      { topic: 'c', absence: true, absenceMs: 100 },
    ],
    withinMs: 10_000,
  });
  observe('a');
  engine.updateAbsenceWindow('p', 2, 50);
  advance(110);
  timers[0]();
  assert.equal(matches.length, 0, 'expected the first absence to advance, not complete');
  assert.equal(engine.snapshot().absenceMatches, 1);
  assert.equal(timers.length, 2, 'expected the chained step to re-arm');
  // Past the revised 50 ms window of the re-armed second step, inside
  // its old 100 ms one: the arrival settles the absence first.
  advance(60);
  observe('c');
  assert.equal(matches.length, 1);
  assert.equal(matches[0].absences!.length, 2);
  assert.equal(matches[0].absences![0].windowMs, 100);
  assert.equal(matches[0].absences![1].windowMs, 50);
});

test('a same-value revision and clearing an already-following step are no-ops with no audit', () => {
  const bus = new EventBus();
  bus.defineSequence({
    id: 'p',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: 50 }],
    withinMs: 1000,
  });
  bus.updateCepAbsenceWindow('p', 1, 50);
  bus.updateCepAbsenceWindow('p', 1, null);
  bus.updateCepAbsenceWindow('p', 1, null);
  bus.updateCepAbsenceWindow('p', 1, 80);
  bus.updateCepAbsenceWindow('p', 1, 80);
  const audit = bus.getSubscriptionAudit({ action: 'cep_absence_window_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[0].detail, { stepIndex: 1, absenceMs: undefined, oldAbsenceMs: 50 });
  assert.deepEqual(audit[1].detail, { stepIndex: 1, absenceMs: 80, oldAbsenceMs: undefined });
});

test('audit records previous and current values; snapshot counts do not move', () => {
  const bus = new EventBus();
  bus.defineSequence({
    id: 'p',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: 50 }],
    withinMs: 1000,
  });
  const before = bus.getStats().cep!;
  bus.updateCepAbsenceWindow('p', 1, 80);
  const audit = bus.getSubscriptionAudit({ action: 'cep_absence_window_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].pattern, 'p');
  assert.deepEqual(audit[0].detail, { stepIndex: 1, absenceMs: 80, oldAbsenceMs: 50 });
  assert.deepEqual(bus.getStats().cep!, before);
  bus.updateCepAbsenceWindow('p', 1, null);
  const cleared = bus.getSubscriptionAudit({ action: 'cep_absence_window_revised' });
  assert.equal(cleared.length, 2);
  assert.deepEqual(cleared[1].detail, { stepIndex: 1, absenceMs: undefined, oldAbsenceMs: 80 });
  assert.deepEqual(bus.getStats().cep!, before);
});

test('getCepAbsenceWindow returns the live explicit value, or undefined while following withinMs', () => {
  const bus = new EventBus();
  bus.defineSequence({
    id: 'p',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: 50 }, { topic: 'c' }],
    withinMs: 1000,
  });
  assert.equal(bus.getCepAbsenceWindow('p', 1), 50);
  bus.updateCepAbsenceWindow('p', 1, 80);
  assert.equal(bus.getCepAbsenceWindow('p', 1), 80);
  bus.updateCepAbsenceWindow('p', 1, null);
  assert.equal(bus.getCepAbsenceWindow('p', 1), undefined);
  // Positive steps have no absence window to read.
  assert.throws(() => bus.getCepAbsenceWindow('p', 0), RangeError);
  assert.throws(() => bus.getCepAbsenceWindow('p', 2), RangeError);
  assert.throws(() => bus.getCepAbsenceWindow('p', 3), RangeError);
  assert.throws(() => bus.getCepAbsenceWindow('ghost', 1), RangeError);
});

test('a revision made while paused governs the re-armed deadline on resume (EB-381)', async () => {
  const bus = new EventBus();
  const seen = collect(bus, 'cep.no-pay');
  bus.defineSequence({
    id: 'no-pay',
    steps: [{ topic: 'order.placed' }, { topic: 'order.paid', absence: true, absenceMs: 80 }],
    withinMs: 10_000,
  });
  bus.publish('order.placed', { order: 1 }, { key: 'o1' });
  bus.setCepSequencePatternPaused('no-pay', true);
  bus.updateCepAbsenceWindow('no-pay', 1, 300);
  assert.equal(bus.getCepAbsenceWindow('no-pay', 1), 300);
  bus.setCepSequencePatternPaused('no-pay', false);
  // Past the old 80 ms window: nothing derives yet — the resume
  // re-armed at the revised 300 ms.
  await sleep(150);
  await flush();
  assert.equal(seen.length, 0);
  await sleep(250);
  await flush();
  assert.equal(seen.length, 1);
  const absences = (seen[0].payload as Record<string, unknown>).absences as Array<{
    stepIndex: number;
    windowMs: number;
  }>;
  assert.equal(absences[0].windowMs, 300);
});

test('close refuses the revision (EB-154) and the live value stays put', async () => {
  const bus = new EventBus();
  bus.defineSequence({
    id: 'p',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: 50 }],
    withinMs: 1000,
  });
  await bus.close();
  assert.throws(() => bus.updateCepAbsenceWindow('p', 1, 80), RangeError);
  assert.equal(bus.getCepAbsenceWindow('p', 1), 50);
  assert.equal(bus.getSubscriptionAudit({ action: 'cep_absence_window_revised' }).length, 0);
});
