/**
 * CEP sequence-pattern live pause/resume (EB-381).
 *
 * `setCepSequencePatternPaused(id, paused)` silences a defined pattern
 * without the clear + define rebuild: while paused the pattern is
 * skipped by `observe` — no sweep, no advance, no new partial, no
 * absence evaluation — its in-flight partials freeze in place, its
 * armed absence deadlines are disarmed, and resume re-arms them from
 * the resume moment at the step's `absenceMs` or the live `withinMs`.
 * Resume itself derives nothing; only the next observation evaluates.
 * Pause is not protection: `clearSequence` still discards a paused
 * pattern's partials, and `updateCepSequencePattern` still retunes a
 * paused pattern. A same-value pause is a no-op with no audit.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage } from '../src/bus.ts';
import { CepEngine, type SequenceMatchEvent } from '../src/cep.ts';
import { renderPrometheus } from '../src/metrics.ts';

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
 * Deterministic engine harness for the pause tests: injected fake
 * timers with a working clear (same precedent as
 * test/cep-absence.test.ts) plus a controllable bus clock, so a
 * disarmed deadline is exact instead of timing luck.
 */
function fakeEngine() {
  let now = 1_000;
  let nextHandle = 1;
  const timers = new Map<number, () => void>();
  const matches: SequenceMatchEvent[] = [];
  const engine = new CepEngine({
    now: () => now,
    compile: (p: string) => new RegExp(`^${p.replace(/\./g, '\\.')}$`),
    emitMatch: (_id, match) => {
      matches.push(match);
    },
    setTimer: (fn) => {
      const handle = nextHandle;
      nextHandle += 1;
      timers.set(handle, fn);
      return handle;
    },
    clearTimer: (handle) => {
      timers.delete(handle as number);
    },
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
    /** Fires every timer pending at call time (a re-armed deadline waits for the next call). */
    fireAll: () => {
      // A fired timer is gone, like a real setTimeout: delete before
      // invoking, so a re-arm during the callback lands as the only
      // pending entry.
      for (const [handle, fn] of [...timers.entries()]) {
        timers.delete(handle);
        fn();
      }
    },
  };
}

test('setCepSequencePatternPaused fails closed with no patterns or an unknown id', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setCepSequencePatternPaused('nope', true), RangeError);
  assert.throws(() => bus.isCepSequencePatternPaused('nope'), RangeError);
  assert.throws(() => bus.setCepSequencePatternPaused('', true), RangeError);
  assert.throws(() => bus.setCepSequencePatternPaused('p', 'yes' as never), RangeError);
  bus.defineSequence({
    id: 'p',
    steps: [{ topic: 'a' }, { topic: 'b' }],
    withinMs: 1000,
  });
  assert.throws(() => bus.setCepSequencePatternPaused('ghost', true), RangeError);
  assert.throws(() => bus.isCepSequencePatternPaused('ghost'), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'cep_sequence_pattern_paused' }).length, 0);
});

test('a paused pattern starts no new partial and completes nothing', async () => {
  let now = 1_000_000;
  const bus = new EventBus({ now: () => now });
  const seen = collect(bus, 'cep.p');
  bus.defineSequence({
    id: 'p',
    steps: [{ topic: 'a' }, { topic: 'b' }],
    withinMs: 10_000,
  });
  bus.publish('a', 1, { key: 'k' });
  assert.equal(bus.getStats().cep!.partialMatches, 1);
  bus.setCepSequencePatternPaused('p', true);
  assert.equal(bus.isCepSequencePatternPaused('p'), true);
  // Neither a fresh anchor nor the completing step is observed.
  bus.publish('a', 1, { key: 'k2' });
  bus.publish('b', 1, { key: 'k' });
  await flush();
  assert.equal(seen.length, 0);
  assert.equal(bus.getStats().cep!.partialMatches, 1);
  assert.equal(bus.getStats().cep!.matches, 0);
});

test('pause freezes an in-flight partial; resume advances it to completion', () => {
  const { engine, matches, advance, observe } = fakeEngine();
  engine.define({
    id: 'p',
    steps: [{ topic: 'a' }, { topic: 'b' }],
    withinMs: 1000,
  });
  observe('a');
  assert.equal(engine.snapshot().partialMatches, 1);
  engine.setPaused('p', true);
  assert.equal(engine.isPaused('p'), true);
  // Messages during the pause do not advance the partial.
  advance(100);
  observe('b');
  assert.equal(matches.length, 0);
  assert.equal(engine.snapshot().partialMatches, 1);
  // Resume derives nothing itself; the next message completes it.
  engine.setPaused('p', false);
  assert.equal(matches.length, 0);
  assert.equal(engine.isPaused('p'), false);
  observe('b');
  assert.equal(matches.length, 1);
  assert.equal(matches[0].durationMs, 100);
  assert.equal(engine.snapshot().partialMatches, 0);
});

test('an over-window partial does not expire while paused', () => {
  const { engine, matches, advance, observe } = fakeEngine();
  engine.define({
    id: 'p',
    steps: [{ topic: 'a' }, { topic: 'b' }],
    withinMs: 1000,
  });
  observe('a');
  engine.setPaused('p', true);
  // Far past withinMs — the sweep is skipped, so the partial survives.
  advance(4000);
  observe('b');
  observe('a', 'k2');
  assert.equal(matches.length, 0);
  assert.equal(engine.snapshot().partialMatches, 1);
});

test('after resume the next observation settles the over-window partial against the live window', () => {
  const { engine, matches, advance, observe } = fakeEngine();
  engine.define({
    id: 'p',
    steps: [{ topic: 'a' }, { topic: 'b' }],
    withinMs: 1000,
  });
  observe('a');
  engine.setPaused('p', true);
  advance(1500);
  engine.setPaused('p', false);
  // The resume itself expired nothing; this observation sweeps the
  // partial (1500ms old against the live 1000ms window) instead of
  // completing it.
  observe('b');
  assert.equal(matches.length, 0);
  assert.equal(engine.snapshot().partialMatches, 0);
  // A fresh anchor after resume behaves normally.
  observe('a');
  observe('b');
  assert.equal(matches.length, 1);
});

test('an absence timer does not fire while paused', () => {
  const { engine, matches, timers, advance, observe, fireAll } = fakeEngine();
  engine.define({
    id: 'p',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: 50 }, { topic: 'c' }],
    withinMs: 10_000,
  });
  observe('a');
  assert.equal(timers.size, 1, 'absence deadline armed on the anchor');
  engine.setPaused('p', true);
  assert.equal(timers.size, 0, 'pause disarms the armed deadline');
  // Past the deadline, with the timer disarmed, nothing derives.
  advance(100);
  fireAll();
  assert.equal(matches.length, 0);
  assert.equal(engine.snapshot().absenceMatches, 0);
  assert.equal(engine.snapshot().partialMatches, 1);
});

test('resume re-arms the absence deadline and it derives', () => {
  const { engine, matches, timers, advance, observe, fireAll } = fakeEngine();
  engine.define({
    id: 'p',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: 50 }, { topic: 'c' }],
    withinMs: 10_000,
  });
  observe('a');
  engine.setPaused('p', true);
  advance(100);
  engine.setPaused('p', false);
  assert.equal(timers.size, 1, 'resume re-arms the deadline from the resume moment');
  // The pre-pause 100ms do not count: the fresh window runs 50ms.
  advance(60);
  fireAll();
  assert.equal(engine.snapshot().absenceMatches, 1);
  assert.equal(matches.length, 0, 'absence alone does not complete a 3-step pattern');
  observe('c');
  assert.equal(matches.length, 1);
  assert.deepEqual(matches[0].absences, [{ stepIndex: 1, topic: 'b', windowMs: 50 }]);
});

test('chained absence steps re-arm after resume', () => {
  const { engine, matches, timers, advance, observe, fireAll } = fakeEngine();
  engine.define({
    id: 'p',
    steps: [
      { topic: 'a' },
      { topic: 'b', absence: true, absenceMs: 50 },
      { topic: 'c', absence: true, absenceMs: 50 },
      { topic: 'd' },
    ],
    withinMs: 10_000,
  });
  observe('a');
  engine.setPaused('p', true);
  assert.equal(timers.size, 0);
  engine.setPaused('p', false);
  assert.equal(timers.size, 1, 'resume re-arms the first absence step');
  advance(60);
  fireAll();
  assert.equal(engine.snapshot().absenceMatches, 1);
  assert.equal(timers.size, 1, 'settling re-arms the chained absence step');
  advance(60);
  fireAll();
  assert.equal(engine.snapshot().absenceMatches, 2);
  assert.equal(timers.size, 0);
  observe('d');
  assert.equal(matches.length, 1);
  assert.equal((matches[0].absences ?? []).length, 2);
});

test('matches and absenceMatches counters do not move while paused', async () => {
  const bus = new EventBus();
  const seen = collect(bus, 'cep.neg');
  bus.defineSequence({
    id: 'neg',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: 60 }],
    withinMs: 10_000,
  });
  bus.publish('a', 1, { key: 'k' });
  bus.setCepSequencePatternPaused('neg', true);
  // Past the absence window, with the deadline disarmed: nothing derives.
  await sleep(150);
  await flush();
  assert.equal(seen.length, 0);
  assert.equal(bus.getStats().cep!.matches, 0);
  assert.equal(bus.getStats().cep!.absenceMatches, 0);
  assert.equal(bus.getStats().cep!.partialMatches, 1);
  // Resume re-arms the deadline; it fires on the fresh window.
  bus.setCepSequencePatternPaused('neg', false);
  await sleep(150);
  await flush();
  assert.equal(seen.length, 1);
  assert.equal(bus.getStats().cep!.matches, 1);
  assert.equal(bus.getStats().cep!.absenceMatches, 1);
  assert.equal(bus.getStats().cep!.partialMatches, 0);
});

test('snapshot pausedPatterns counts paused patterns', () => {
  const bus = new EventBus();
  bus.defineSequence({ id: 'p1', steps: [{ topic: 'a' }, { topic: 'b' }], withinMs: 1000 });
  bus.defineSequence({ id: 'p2', steps: [{ topic: 'a' }, { topic: 'b' }], withinMs: 1000 });
  assert.equal(bus.getStats().cep!.pausedPatterns, 0);
  bus.setCepSequencePatternPaused('p1', true);
  assert.equal(bus.getStats().cep!.pausedPatterns, 1);
  assert.equal(bus.getStats().cep!.activePatterns, 2);
  bus.setCepSequencePatternPaused('p2', true);
  assert.equal(bus.getStats().cep!.pausedPatterns, 2);
  bus.setCepSequencePatternPaused('p1', false);
  assert.equal(bus.getStats().cep!.pausedPatterns, 1);
  const out = renderPrometheus(bus.getStats());
  assert.ok(out.includes('eventbus_cep_paused_patterns 1'), 'paused patterns gauge');
});

test('a same-value pause is a no-op with no audit', () => {
  const bus = new EventBus();
  bus.defineSequence({
    id: 'p',
    steps: [{ topic: 'a' }, { topic: 'b' }],
    withinMs: 1000,
  });
  bus.setCepSequencePatternPaused('p', true);
  bus.setCepSequencePatternPaused('p', true);
  bus.setCepSequencePatternPaused('p', false);
  bus.setCepSequencePatternPaused('p', false);
  const audit = bus.getSubscriptionAudit({ action: 'cep_sequence_pattern_paused' });
  assert.equal(audit.length, 2);
  assert.equal(audit[0].pattern, 'p');
  assert.deepEqual(audit[0].detail, { paused: true });
  assert.equal(audit[1].pattern, 'p');
  assert.deepEqual(audit[1].detail, { paused: false });
});

test('clearSequence discards a paused pattern and its frozen partials', async () => {
  const bus = new EventBus();
  const seen = collect(bus, 'cep.tmp');
  bus.defineSequence({
    id: 'tmp',
    steps: [{ topic: 'a' }, { topic: 'b' }],
    withinMs: 10_000,
  });
  bus.publish('a', 1, { key: 'k' });
  bus.setCepSequencePatternPaused('tmp', true);
  assert.equal(bus.clearSequence('tmp'), true);
  assert.equal(bus.getStats().cep!.partialMatches, 0);
  assert.equal(bus.getStats().cep!.pausedPatterns, 0);
  assert.equal(bus.getStats().cep!.activePatterns, 0);
  // Pause is not protection: the frozen partial is gone, not resumed.
  bus.publish('a', 1, { key: 'k' });
  bus.publish('b', 1, { key: 'k' });
  await flush();
  assert.equal(seen.length, 0);
});

test('a revision of a paused pattern governs the re-armed absence deadline (EB-297)', () => {
  const { engine, matches, timers, advance, observe, fireAll } = fakeEngine();
  engine.define({
    id: 'p',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true }],
    withinMs: 10_000,
  });
  observe('a');
  assert.equal(timers.size, 1, 'absence armed at the 10s window');
  engine.setPaused('p', true);
  // Retune while paused: the window shrinks to 60ms.
  const result = engine.update('p', { withinMs: 60 });
  assert.equal(result.changed, true);
  engine.setPaused('p', false);
  assert.equal(timers.size, 1, 'resume re-arms at the live window');
  advance(70);
  fireAll();
  assert.equal(matches.length, 1);
  assert.equal(matches[0].durationMs, 70);
  assert.deepEqual(matches[0].absences, [{ stepIndex: 1, topic: 'b', windowMs: 60 }]);
});
