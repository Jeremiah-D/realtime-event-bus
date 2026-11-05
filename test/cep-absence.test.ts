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

test('CEP absence: A then no B within the window derives an absence match', async () => {
  const bus = new EventBus();
  const seen = collect(bus, 'cep.no-pay');
  bus.defineSequence({
    id: 'no-pay',
    steps: [{ topic: 'order.placed' }, { topic: 'order.paid', absence: true, absenceMs: 80 }],
    withinMs: 10_000,
  });
  bus.publish('order.placed', { order: 1 }, { key: 'o1' });
  await sleep(250);
  await flush();
  assert.equal(seen.length, 1);
  const payload = seen[0].payload as Record<string, unknown>;
  assert.equal(payload.patternId, 'no-pay');
  assert.equal(payload.key, 'o1');
  assert.equal((payload.events as unknown[]).length, 1);
  assert.equal((payload.events as Array<{ topic: string }>)[0].topic, 'order.placed');
  const absences = payload.absences as Array<{ stepIndex: number; topic: string; windowMs: number }>;
  assert.equal(absences.length, 1);
  assert.equal(absences[0].stepIndex, 1);
  assert.equal(absences[0].topic, 'order.paid');
  assert.equal(absences[0].windowMs, 80);
  const stats = bus.getStats().cep!;
  assert.equal(stats.absencePatterns, 1);
  assert.equal(stats.absenceMatches, 1);
  assert.equal(stats.matches, 1);
  assert.equal(stats.partialMatches, 0);
});

test('CEP absence: the awaited event kills the partial — no absence derived', async () => {
  const bus = new EventBus();
  const seen = collect(bus, 'cep.no-pay');
  bus.defineSequence({
    id: 'no-pay',
    steps: [{ topic: 'order.placed' }, { topic: 'order.paid', absence: true, absenceMs: 80 }],
    withinMs: 10_000,
  });
  bus.publish('order.placed', { order: 1 }, { key: 'o1' });
  bus.publish('order.paid', { order: 1 }, { key: 'o1' });
  await sleep(250);
  await flush();
  assert.equal(seen.length, 0);
  const stats = bus.getStats().cep!;
  assert.equal(stats.absenceMatches, 0);
  assert.equal(stats.matches, 0);
  assert.equal(stats.partialMatches, 0);
});

test('CEP absence: per-key isolation — B on another key does not kill', async () => {
  const bus = new EventBus();
  const seen = collect(bus, 'cep.no-pay');
  bus.defineSequence({
    id: 'no-pay',
    steps: [{ topic: 'order.placed' }, { topic: 'order.paid', absence: true, absenceMs: 80 }],
    withinMs: 10_000,
  });
  bus.publish('order.placed', { order: 1 }, { key: 'o1' });
  bus.publish('order.paid', { order: 2 }, { key: 'o2' });
  await sleep(250);
  await flush();
  assert.equal(seen.length, 1);
  const payload = seen[0].payload as Record<string, unknown>;
  assert.equal(payload.key, 'o1');
  assert.equal((payload.absences as unknown[]).length, 1);
});

test('CEP absence: composes mid-pattern — A, not B, then C completes', async () => {
  const bus = new EventBus();
  const seen = collect(bus, 'cep.flow');
  bus.defineSequence({
    id: 'flow',
    steps: [
      { topic: 'a' },
      { topic: 'b', absence: true, absenceMs: 80 },
      { topic: 'c' },
    ],
    withinMs: 10_000,
  });
  bus.publish('a', 1, { key: 'k' });
  await sleep(200); // absence window elapses; the partial advances past NOT b
  bus.publish('c', 3, { key: 'k' });
  await flush();
  assert.equal(seen.length, 1);
  const payload = seen[0].payload as Record<string, unknown>;
  assert.deepEqual(
    (payload.events as Array<{ topic: string }>).map((e) => e.topic),
    ['a', 'c'],
  );
  const absences = payload.absences as Array<{ stepIndex: number }>;
  assert.equal(absences.length, 1);
  assert.equal(absences[0].stepIndex, 1);
  const stats = bus.getStats().cep!;
  assert.equal(stats.absenceMatches, 1);
  assert.equal(stats.matches, 1);
});

test('CEP absence: mid-pattern negation violated — later C cannot complete', async () => {
  const bus = new EventBus();
  const seen = collect(bus, 'cep.flow');
  bus.defineSequence({
    id: 'flow',
    steps: [
      { topic: 'a' },
      { topic: 'b', absence: true, absenceMs: 200 },
      { topic: 'c' },
    ],
    withinMs: 10_000,
  });
  bus.publish('a', 1, { key: 'k' });
  bus.publish('b', 2, { key: 'k' }); // kills the partial mid-pattern
  bus.publish('c', 3, { key: 'k' });
  await sleep(300);
  await flush();
  assert.equal(seen.length, 0);
  const stats = bus.getStats().cep!;
  assert.equal(stats.absenceMatches, 0);
  assert.equal(stats.matches, 0);
});

test('CEP absence: consecutive absence steps chain without another anchor', async () => {
  const bus = new EventBus();
  const seen = collect(bus, 'cep.chain');
  bus.defineSequence({
    id: 'chain',
    steps: [
      { topic: 'a' },
      { topic: 'b', absence: true, absenceMs: 60 },
      { topic: 'c', absence: true, absenceMs: 60 },
    ],
    withinMs: 10_000,
  });
  bus.publish('a', 1, { key: 'k' });
  await sleep(250);
  await flush();
  assert.equal(seen.length, 1);
  const payload = seen[0].payload as Record<string, unknown>;
  const absences = payload.absences as Array<{ stepIndex: number; topic: string }>;
  assert.equal(absences.length, 2);
  assert.deepEqual(
    absences.map((a) => a.stepIndex),
    [1, 2],
  );
  assert.deepEqual(
    absences.map((a) => a.topic),
    ['b', 'c'],
  );
  assert.equal(bus.getStats().cep!.absenceMatches, 2);
});

test('CEP absence: absenceMs defaults to the pattern withinMs', async () => {
  const bus = new EventBus();
  const seen = collect(bus, 'cep.d');
  bus.defineSequence({
    id: 'd',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true }],
    withinMs: 80,
  });
  bus.publish('a', 1, { key: 'k' });
  await sleep(250);
  await flush();
  assert.equal(seen.length, 1);
  const absences = (seen[0].payload as Record<string, unknown>).absences as Array<{
    windowMs: number;
  }>;
  assert.equal(absences[0].windowMs, 80);
});

test('CEP absence: clearSequence disarms pending deadlines', async () => {
  const bus = new EventBus();
  const seen = collect(bus, 'cep.gone');
  bus.defineSequence({
    id: 'gone',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: 80 }],
    withinMs: 10_000,
  });
  bus.publish('a', 1, { key: 'k' });
  assert.equal(bus.clearSequence('gone'), true);
  await sleep(250);
  await flush();
  assert.equal(seen.length, 0);
  assert.equal(bus.getStats().cep!.absenceMatches, 0);
});

test('CEP absence: a partial older than withinMs derives nothing on an idle bus', async () => {
  const bus = new EventBus();
  const seen = collect(bus, 'cep.stale');
  bus.defineSequence({
    id: 'stale',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: 200 }],
    withinMs: 60,
  });
  bus.publish('a', 1, { key: 'k' });
  // No further traffic: the lazy sweep never runs. The timer fires at
  // 200ms, sees the partial outlived withinMs, and drops it silently.
  await sleep(350);
  await flush();
  assert.equal(seen.length, 0);
  const stats = bus.getStats().cep!;
  assert.equal(stats.absenceMatches, 0);
  assert.equal(stats.matches, 0);
  assert.equal(stats.partialMatches, 0);
});

test('CEP absence: validation fails fast', () => {
  const bus = new EventBus();
  // First step cannot be an absence step — no anchor for the window.
  assert.throws(
    () =>
      bus.defineSequence({
        steps: [
          { topic: 'a', absence: true },
          { topic: 'b' },
        ],
        withinMs: 1000,
      }),
    RangeError,
  );
  // absenceMs must be a positive finite number.
  for (const bad of [0, -5, Number.NaN, Number.POSITIVE_INFINITY]) {
    assert.throws(
      () =>
        bus.defineSequence({
          steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: bad }],
          withinMs: 1000,
        }),
      RangeError,
      `absenceMs=${String(bad)}`,
    );
  }
  // absence must be a boolean when present.
  assert.throws(
    () =>
      bus.defineSequence({
        steps: [{ topic: 'a' }, { topic: 'b', absence: 'yes' as never }],
        withinMs: 1000,
      }),
    RangeError,
  );
  // Nothing was registered — the engine exists but holds zero patterns.
  assert.deepEqual(bus.getStats().cep, {
    activePatterns: 0,
    partialMatches: 0,
    matches: 0,
    absencePatterns: 0,
    absenceMatches: 0,
    pausedPatterns: 0,
  });
});

test('CEP absence: absencePatterns counts only patterns with absence steps', () => {
  const bus = new EventBus();
  bus.defineSequence({ id: 'p1', steps: [{ topic: 'a' }, { topic: 'b' }], withinMs: 1000 });
  assert.equal(bus.getStats().cep!.absencePatterns, 0);
  bus.defineSequence({
    id: 'p2',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: 50 }],
    withinMs: 1000,
  });
  assert.equal(bus.getStats().cep!.absencePatterns, 1);
  assert.equal(bus.getStats().cep!.activePatterns, 2);
});

test('CEP absence: absence matches never re-enter pattern detection', async () => {
  const bus = new EventBus();
  const seen = collect(bus, 'cep.**');
  bus.defineSequence({
    id: 'neg',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: 60 }],
    withinMs: 10_000,
  });
  // A second pattern watching the derived topic must not see it.
  bus.defineSequence({
    id: 'watcher',
    steps: [{ topic: 'cep.neg' }, { topic: 'c' }],
    withinMs: 10_000,
  });
  bus.publish('a', 1, { key: 'k' });
  await sleep(250);
  await flush();
  // Exactly one derived event (from 'neg'); the watcher never fired.
  assert.equal(seen.length, 1);
  assert.equal((seen[0].payload as Record<string, unknown>).patternId, 'neg');
});

test('CEP absence: engine-level timer injection is deterministic', () => {
  // Direct engine test with fake timers: no wall-clock waiting.
  let now = 1_000;
  const fired: Array<() => void> = [];
  const engine = new CepEngine({
    now: () => now,
    compile: (p: string) => new RegExp(`^${p.replace(/\./g, '\\.')}$`),
    emitMatch: () => {},
    setTimer: (fn) => {
      fired.push(fn);
      return fired.length;
    },
    clearTimer: () => {},
  });
  engine.define({
    id: 't',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: 50 }],
    withinMs: 10_000,
  });
  engine.observe({ topic: 'a', seq: 1, key: 'k', payload: 1 } as never);
  assert.equal(fired.length, 1);
  assert.equal(engine.snapshot().partialMatches, 1);
  // Advance the clock past the deadline and fire the timer manually.
  now += 60;
  fired[0]();
  const snap = engine.snapshot();
  assert.equal(snap.absenceMatches, 1);
  assert.equal(snap.matches, 1);
  assert.equal(snap.partialMatches, 0);
});

test('CEP absence: metrics render the new series', () => {
  const bus = new EventBus();
  bus.defineSequence({
    id: 'm',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: 50 }],
    withinMs: 1000,
  });
  const out = renderPrometheus(bus.getStats());
  assert.ok(out.includes('eventbus_cep_absence_patterns 1'), 'absence patterns gauge');
  assert.ok(out.includes('eventbus_cep_absence_matches_total 0'), 'absence matches counter');
});

/**
 * Deterministic engine harness for the EB-379 boundary tests: injected
 * fake timers (same precedent as the test above) plus a controllable bus
 * clock, so a late-firing deadline is exact instead of timing luck.
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

test('CEP absence (EB-379): default window, timer fires 5ms late, still derives', () => {
  const { engine, matches, timers, advance, observe } = fakeEngine();
  engine.define({
    id: 'late5',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true }],
    withinMs: 80,
  });
  observe('a');
  assert.equal(engine.snapshot().partialMatches, 1);
  // The timer was armed for 80ms but the bus was idle and delivery ran
  // 5ms late: the event-free window really elapsed, so it still derives.
  advance(85);
  timers[0]();
  assert.equal(matches.length, 1);
  assert.equal(matches[0].durationMs, 85);
  assert.deepEqual(matches[0].absences, [{ stepIndex: 1, topic: 'b', windowMs: 80 }]);
  const snap = engine.snapshot();
  assert.equal(snap.absenceMatches, 1);
  assert.equal(snap.matches, 1);
  assert.equal(snap.partialMatches, 0);
});

test('CEP absence (EB-379): default window, timer fires 500ms late, still derives', () => {
  const { engine, matches, timers, advance, observe } = fakeEngine();
  engine.define({
    id: 'late500',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true }],
    withinMs: 80,
  });
  observe('a');
  advance(580);
  timers[0]();
  assert.equal(matches.length, 1);
  assert.equal(matches[0].durationMs, 580);
  assert.equal(matches[0].absences![0].windowMs, 80);
  assert.equal(engine.snapshot().absenceMatches, 1);
});

test('CEP absence (EB-379): explicit absenceMs wider than withinMs, late fire still drops', () => {
  const { engine, matches, timers, advance, observe } = fakeEngine();
  engine.define({
    id: 'wide',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: 200 }],
    withinMs: 60,
  });
  observe('a');
  advance(205);
  timers[0]();
  assert.equal(matches.length, 0);
  const snap = engine.snapshot();
  assert.equal(snap.absenceMatches, 0);
  assert.equal(snap.matches, 0);
  assert.equal(snap.partialMatches, 0);
});

test('CEP absence (EB-379): EB-297 shrink below the armed window drops on fire', () => {
  const { engine, matches, timers, advance, observe } = fakeEngine();
  engine.define({
    id: 'shrink',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true }],
    withinMs: 80,
  });
  observe('a');
  // Live window shrinks under the armed 80ms before the timer fires.
  assert.equal(engine.update('shrink', { withinMs: 50 }).changed, true);
  advance(85);
  timers[0]();
  assert.equal(matches.length, 0);
  const snap = engine.snapshot();
  assert.equal(snap.absenceMatches, 0);
  assert.equal(snap.matches, 0);
  assert.equal(snap.partialMatches, 0);
});

test('CEP absence (EB-379): EB-297 widen above the armed window still derives', () => {
  const { engine, matches, timers, advance, observe } = fakeEngine();
  engine.define({
    id: 'widen',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true }],
    withinMs: 80,
  });
  observe('a');
  assert.equal(engine.update('widen', { withinMs: 10_000 }).changed, true);
  advance(85);
  timers[0]();
  assert.equal(matches.length, 1);
  // The recorded window is the armed 80ms, not the widened live window.
  assert.equal(matches[0].absences![0].windowMs, 80);
  assert.equal(engine.snapshot().absenceMatches, 1);
});

test('CEP absence (EB-379): explicit small window, very late fire derives', () => {
  const { engine, matches, timers, advance, observe } = fakeEngine();
  engine.define({
    id: 'small',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: 50 }],
    withinMs: 10_000,
  });
  observe('a');
  advance(550);
  timers[0]();
  assert.equal(matches.length, 1);
  assert.equal(matches[0].durationMs, 550);
  assert.equal(matches[0].absences![0].windowMs, 50);
  assert.equal(engine.snapshot().absenceMatches, 1);
});

test('CEP absence (EB-379): chained [A, NOT B, NOT C] completes on late fires', () => {
  const { engine, matches, timers, advance, observe } = fakeEngine();
  engine.define({
    id: 'chain-late',
    steps: [
      { topic: 'a' },
      { topic: 'b', absence: true, absenceMs: 60 },
      { topic: 'c', absence: true, absenceMs: 60 },
    ],
    withinMs: 10_000,
  });
  observe('a');
  advance(65);
  timers[0](); // NOT b fires 5ms late, arms NOT c immediately
  assert.equal(timers.length, 2);
  assert.equal(engine.snapshot().absenceMatches, 1);
  assert.equal(engine.snapshot().matches, 0);
  advance(65);
  timers[1](); // NOT c fires 5ms late, completes the pattern
  assert.equal(matches.length, 1);
  assert.equal(matches[0].absences!.length, 2);
  assert.deepEqual(
    matches[0].absences!.map((a) => a.stepIndex),
    [1, 2],
  );
  assert.deepEqual(
    matches[0].absences!.map((a) => a.windowMs),
    [60, 60],
  );
  const snap = engine.snapshot();
  assert.equal(snap.absenceMatches, 2);
  assert.equal(snap.matches, 1);
  assert.equal(snap.partialMatches, 0);
});

test('CEP absence (EB-379): absences windowMs records the armed value, unaffected by revise', () => {
  const { engine, matches, timers, advance, observe } = fakeEngine();
  engine.define({
    id: 'armed-value',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: 50 }],
    withinMs: 1000,
  });
  observe('a');
  // Widen the live window after arming: the recorded window stays 50.
  assert.equal(engine.update('armed-value', { withinMs: 2000 }).changed, true);
  advance(55);
  timers[0]();
  assert.equal(matches.length, 1);
  assert.equal(matches[0].absences![0].windowMs, 50);
  assert.equal(matches[0].durationMs, 55);
});

test('CEP absence (EB-379): B arriving before the deadline still kills', () => {
  const { engine, matches, timers, advance, observe } = fakeEngine();
  engine.define({
    id: 'kill',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: 50 }],
    withinMs: 10_000,
  });
  observe('a');
  observe('b'); // arrives inside the window — the absence can never fire
  const snap = engine.snapshot();
  assert.equal(snap.partialMatches, 0);
  assert.equal(snap.matches, 0);
  assert.equal(snap.absenceMatches, 0);
  // The stale timer is a no-op once the partial is resolved.
  advance(60);
  timers[0]();
  assert.equal(matches.length, 0);
  assert.equal(engine.snapshot().absenceMatches, 0);
});

test('CEP absence (EB-379): counters track the late-fire lifecycle', () => {
  const { engine, matches, timers, advance, observe } = fakeEngine();
  engine.define({
    id: 'counts',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true }],
    withinMs: 80,
  });
  observe('a');
  let snap = engine.snapshot();
  assert.equal(snap.partialMatches, 1);
  assert.equal(snap.matches, 0);
  assert.equal(snap.absenceMatches, 0);
  advance(85);
  timers[0]();
  assert.equal(matches.length, 1);
  snap = engine.snapshot();
  assert.equal(snap.partialMatches, 0);
  assert.equal(snap.matches, 1);
  assert.equal(snap.absenceMatches, 1);
});

test('CEP absence (EB-379): shrink below an explicit absenceMs drops on fire', () => {
  const { engine, matches, timers, advance, observe } = fakeEngine();
  engine.define({
    id: 'shrink-explicit',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: 200 }],
    withinMs: 1000,
  });
  observe('a');
  assert.equal(engine.update('shrink-explicit', { withinMs: 100 }).changed, true);
  advance(205);
  timers[0]();
  assert.equal(matches.length, 0);
  const snap = engine.snapshot();
  assert.equal(snap.absenceMatches, 0);
  assert.equal(snap.matches, 0);
  assert.equal(snap.partialMatches, 0);
});

test('CEP absence (EB-379): real timer, default window derives despite fire jitter', async () => {
  const bus = new EventBus();
  const seen = collect(bus, 'cep.jitter');
  bus.defineSequence({
    id: 'jitter',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true }],
    withinMs: 40,
  });
  bus.publish('a', 1, { key: 'k' });
  // A 40ms timer almost always fires at 41ms+ on a real event loop; the
  // old fire-time re-check (`now - startedAt > withinMs`) dropped this
  // deterministically. The armed-window guard derives it every time.
  await sleep(200);
  await flush();
  assert.equal(seen.length, 1);
  const payload = seen[0].payload as Record<string, unknown>;
  const absences = payload.absences as Array<{ windowMs: number }>;
  assert.equal(absences[0].windowMs, 40);
  assert.equal((payload.durationMs as number) >= 40, true);
  const stats = bus.getStats().cep!;
  assert.equal(stats.absenceMatches, 1);
  assert.equal(stats.matches, 1);
  assert.equal(stats.partialMatches, 0);
});
