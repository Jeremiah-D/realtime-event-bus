import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage } from '../src/bus.ts';
import { CepEngine } from '../src/cep.ts';
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
