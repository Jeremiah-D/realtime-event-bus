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
 * Deterministic engine harness for the EB-380 observe-side tests: the
 * EB-379 fake-timer + controllable-clock precedent (see
 * test/cep-absence.test.ts), plus a recording clearTimer so the disarm
 * of the stale timer is itself asserted. The fake timers never fire on
 * their own — a message observed after the armed deadline is exactly
 * the "late timer, gap arrival" the feature covers.
 */
function fakeEngine() {
  let now = 1_000;
  const timers: Array<() => void> = [];
  const cleared: unknown[] = [];
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
    clearTimer: (handle) => {
      cleared.push(handle);
    },
  });
  return {
    engine,
    matches,
    timers,
    cleared,
    advance: (ms: number) => {
      now += ms;
    },
    observe: (topic: string, key = 'k') =>
      engine.observe({ topic, seq: 1, key, payload: 1 } as never),
  };
}

test('CEP absence (EB-380): explicit small window, B after the deadline derives', () => {
  const { engine, matches, advance, observe } = fakeEngine();
  engine.define({
    id: 'small-gap',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: 50 }],
    withinMs: 10_000,
  });
  observe('a');
  assert.equal(engine.snapshot().partialMatches, 1);
  // B arrives at +100ms: the 50ms absence window elapsed event-free
  // long ago, but the (fake) timer has not fired yet. Before EB-380
  // this killed the partial; now the absence settles first.
  advance(100);
  observe('b');
  assert.equal(matches.length, 1);
  assert.equal(matches[0].durationMs, 100);
  assert.deepEqual(matches[0].absences, [{ stepIndex: 1, topic: 'b', windowMs: 50 }]);
  const snap = engine.snapshot();
  assert.equal(snap.absenceMatches, 1);
  assert.equal(snap.matches, 1);
  assert.equal(snap.partialMatches, 0);
});

test('CEP absence (EB-380): default window, B at the deadline derives', () => {
  const { engine, matches, advance, observe } = fakeEngine();
  engine.define({
    id: 'default-gap',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true }],
    withinMs: 80,
  });
  observe('a');
  // Exactly at the armed deadline (armed at 1000, window 80): the
  // sweep still keeps the partial (80 <= withinMs) and the deadline
  // evaluation settles it before B is matched.
  advance(80);
  observe('b');
  assert.equal(matches.length, 1);
  assert.equal(matches[0].durationMs, 80);
  assert.equal(matches[0].absences![0].windowMs, 80);
  const snap = engine.snapshot();
  assert.equal(snap.absenceMatches, 1);
  assert.equal(snap.matches, 1);
  assert.equal(snap.partialMatches, 0);
});

test('CEP absence (EB-380): B arriving before the deadline still kills', () => {
  const { engine, matches, timers, advance, observe } = fakeEngine();
  engine.define({
    id: 'kill-before',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: 50 }],
    withinMs: 10_000,
  });
  observe('a');
  advance(30);
  observe('b'); // inside the window — the absence can never fire
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

test('CEP absence (EB-380): B exactly at the deadline derives (boundary >=)', () => {
  const { engine, matches, advance, observe } = fakeEngine();
  engine.define({
    id: 'boundary',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: 50 }],
    withinMs: 10_000,
  });
  observe('a');
  advance(50); // now == armedAt + windowMs exactly
  observe('b');
  assert.equal(matches.length, 1);
  assert.equal(matches[0].durationMs, 50);
  assert.equal(matches[0].absences![0].windowMs, 50);
  assert.equal(engine.snapshot().absenceMatches, 1);
});

test('CEP absence (EB-380): B one millisecond before the deadline still kills', () => {
  const { engine, matches, advance, observe } = fakeEngine();
  engine.define({
    id: 'boundary-kill',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: 50 }],
    withinMs: 10_000,
  });
  observe('a');
  advance(49);
  observe('b');
  assert.equal(matches.length, 0);
  assert.equal(engine.snapshot().partialMatches, 0);
  assert.equal(engine.snapshot().absenceMatches, 0);
});

test('CEP absence (EB-380): chained [A, NOT B, NOT C] re-arms the second absence on observe', () => {
  const { engine, matches, timers, advance, observe } = fakeEngine();
  engine.define({
    id: 'chain-observe',
    steps: [
      { topic: 'a' },
      { topic: 'b', absence: true, absenceMs: 60 },
      { topic: 'c', absence: true, absenceMs: 60 },
    ],
    withinMs: 10_000,
  });
  observe('a');
  advance(65);
  // B arrives after the first deadline: NOT b settles, NOT c re-arms
  // from now (1065), and B is then evaluated against the new step —
  // topic 'b' does not match 'c', so the partial survives.
  observe('b');
  assert.equal(timers.length, 2);
  assert.equal(engine.snapshot().absenceMatches, 1);
  assert.equal(engine.snapshot().matches, 0);
  assert.equal(engine.snapshot().partialMatches, 1);
  // The re-armed window runs from the settle moment: firing it derives.
  advance(65);
  timers[1]();
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

test('CEP absence (EB-380): re-armed chained absence still kills on arrival inside its window', () => {
  const { engine, matches, advance, observe } = fakeEngine();
  engine.define({
    id: 'chain-rekill',
    steps: [
      { topic: 'a' },
      { topic: 'b', absence: true, absenceMs: 60 },
      { topic: 'b', absence: true, absenceMs: 60 },
    ],
    withinMs: 10_000,
  });
  observe('a');
  advance(65);
  // B arrives after the first deadline: NOT b settles, the second NOT b
  // re-arms from now — and B, arriving inside the fresh window, kills
  // the partial against the new step.
  observe('b');
  assert.equal(engine.snapshot().absenceMatches, 1);
  assert.equal(engine.snapshot().matches, 0);
  assert.equal(engine.snapshot().partialMatches, 0);
  assert.equal(matches.length, 0);
});

test('CEP absence (EB-380): last-step absence settles and the same message starts a new partial', () => {
  const { engine, matches, timers, advance, observe } = fakeEngine();
  engine.define({
    id: 'restart',
    steps: [{ topic: 'b' }, { topic: 'b', absence: true, absenceMs: 50 }],
    withinMs: 10_000,
  });
  observe('b'); // starts a partial, arms NOT b
  advance(60);
  // The arriving 'b' settles the old absence (completing the old
  // partial) and then matches step 0, starting a fresh partial.
  observe('b');
  assert.equal(matches.length, 1);
  assert.equal(matches[0].absences![0].windowMs, 50);
  assert.equal(matches[0].durationMs, 60);
  const snap = engine.snapshot();
  assert.equal(snap.absenceMatches, 1);
  assert.equal(snap.matches, 1);
  assert.equal(snap.partialMatches, 1);
  // The new partial armed its own deadline.
  assert.equal(timers.length, 2);
});

test('CEP absence (EB-380): [A, NOT B, C] settles, then C completes', () => {
  const { engine, matches, advance, observe } = fakeEngine();
  engine.define({
    id: 'mid-observe',
    steps: [
      { topic: 'a' },
      { topic: 'b', absence: true, absenceMs: 50 },
      { topic: 'c' },
    ],
    withinMs: 10_000,
  });
  observe('a');
  advance(60);
  // B arrives after the deadline: the absence settles instead of
  // killing, and B does not match the advanced step 'c'.
  observe('b');
  assert.equal(engine.snapshot().absenceMatches, 1);
  assert.equal(engine.snapshot().matches, 0);
  assert.equal(engine.snapshot().partialMatches, 1);
  observe('c');
  assert.equal(matches.length, 1);
  assert.deepEqual(
    matches[0].events.map((e) => e.topic),
    ['a', 'c'],
  );
  assert.equal(matches[0].absences!.length, 1);
  assert.equal(matches[0].absences![0].stepIndex, 1);
  assert.equal(matches[0].durationMs, 60);
  assert.equal(engine.snapshot().partialMatches, 0);
});

test('CEP absence (EB-380): EB-297 shrink drops instead of deriving', () => {
  const { engine, matches, advance, observe } = fakeEngine();
  engine.define({
    id: 'shrink-observe',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: 50 }],
    withinMs: 10_000,
  });
  observe('a');
  // Live window shrinks below the armed 50ms before B arrives.
  assert.equal(engine.update('shrink-observe', { withinMs: 30 }).changed, true);
  advance(60);
  observe('b');
  // The lazy sweep drops the partial first (60 > live 30) — the two
  // guards agree: no derive, no absence recorded.
  assert.equal(matches.length, 0);
  const snap = engine.snapshot();
  assert.equal(snap.absenceMatches, 0);
  assert.equal(snap.matches, 0);
  assert.equal(snap.partialMatches, 0);
});

test('CEP absence (EB-380): EB-297 widen still derives on observe', () => {
  const { engine, matches, advance, observe } = fakeEngine();
  engine.define({
    id: 'widen-observe',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: 50 }],
    withinMs: 100,
  });
  observe('a');
  assert.equal(engine.update('widen-observe', { withinMs: 10_000 }).changed, true);
  advance(60);
  observe('b');
  assert.equal(matches.length, 1);
  // The recorded window is the armed 50ms, not the widened live window.
  assert.equal(matches[0].absences![0].windowMs, 50);
  assert.equal(matches[0].durationMs, 60);
  assert.equal(engine.snapshot().absenceMatches, 1);
});

test('CEP absence (EB-380): firing the original timer after observe-settle records nothing twice', () => {
  const { engine, matches, timers, cleared, advance, observe } = fakeEngine();
  engine.define({
    id: 'disarm',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: 50 }],
    withinMs: 10_000,
  });
  observe('a');
  advance(60);
  observe('b'); // settles via observe
  assert.equal(matches.length, 1);
  assert.equal(engine.snapshot().absenceMatches, 1);
  // The pending timer was disarmed by the settle: its handle (1) was
  // cleared, so a stale fire afterwards is a no-op.
  assert.deepEqual(cleared, [1]);
  timers[0]();
  assert.equal(matches.length, 1);
  assert.equal(engine.snapshot().absenceMatches, 1);
  assert.equal(engine.snapshot().matches, 1);
});

test('CEP absence (EB-380): per-key isolation — one key derives in the gap, the other kills', () => {
  const { engine, matches, advance, observe } = fakeEngine();
  engine.define({
    id: 'keys',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: 50 }],
    withinMs: 10_000,
  });
  observe('a', 'k1');
  observe('a', 'k2');
  advance(30);
  observe('b', 'k2'); // inside k2's window — kills k2's partial
  assert.equal(engine.snapshot().partialMatches, 1);
  assert.equal(engine.snapshot().absenceMatches, 0);
  advance(30); // now past k1's deadline
  observe('b', 'k1'); // settles k1's absence instead of killing
  assert.equal(matches.length, 1);
  assert.equal(matches[0].key, 'k1');
  assert.equal(matches[0].absences![0].windowMs, 50);
  const snap = engine.snapshot();
  assert.equal(snap.absenceMatches, 1);
  assert.equal(snap.matches, 1);
  assert.equal(snap.partialMatches, 0);
});

test('CEP absence (EB-380): counters agree across the observe-settle and timer paths', () => {
  // Observe-settle path.
  const viaObserve = fakeEngine();
  viaObserve.engine.define({
    id: 'counts',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: 50 }],
    withinMs: 10_000,
  });
  viaObserve.observe('a');
  viaObserve.advance(60);
  viaObserve.observe('b');
  // Timer path, same shape.
  const viaTimer = fakeEngine();
  viaTimer.engine.define({
    id: 'counts',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: 50 }],
    withinMs: 10_000,
  });
  viaTimer.observe('a');
  viaTimer.advance(60);
  viaTimer.timers[0]();
  assert.deepEqual(viaObserve.engine.snapshot(), viaTimer.engine.snapshot());
  assert.deepEqual(viaObserve.matches, viaTimer.matches);
});

test('CEP absence (EB-380): B after the deadline and after withinMs is swept, not derived', () => {
  const { engine, matches, advance, observe } = fakeEngine();
  engine.define({
    id: 'swept',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true }],
    withinMs: 80,
  });
  observe('a');
  // Past both the armed deadline (80) and the live window: the lazy
  // sweep drops the partial before any deadline evaluation.
  advance(85);
  observe('b');
  assert.equal(matches.length, 0);
  const snap = engine.snapshot();
  assert.equal(snap.absenceMatches, 0);
  assert.equal(snap.matches, 0);
  assert.equal(snap.partialMatches, 0);
});

test('CEP absence (EB-380): real timer still derives after the refactor', async () => {
  const bus = new EventBus();
  const seen = collect(bus, 'cep.gap');
  bus.defineSequence({
    id: 'gap',
    steps: [{ topic: 'a' }, { topic: 'b', absence: true, absenceMs: 40 }],
    withinMs: 10_000,
  });
  bus.publish('a', 1, { key: 'k' });
  // The real timer fires at ~40ms and derives; a 'b' arriving after
  // that finds no partial waiting — exactly one match, no duplicate.
  await sleep(150);
  bus.publish('b', 2, { key: 'k' });
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
