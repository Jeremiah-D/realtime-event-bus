import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage } from '../src/bus.ts';
import { CepEngine } from '../src/cep.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's async flush has delivered. */
const flush = () => new Promise<void>((resolve) => setTimeout(resolve, 20));

function collect(bus: EventBus, pattern = '**'): BusMessage[] {
  const seen: BusMessage[] = [];
  bus.subscribe(pattern, (msg) => {
    seen.push(msg);
  });
  return seen;
}

test('CEP: A->B within window emits a match on cep.<patternId>', async () => {
  const bus = new EventBus();
  const seen = collect(bus, 'cep.login-flow');
  const id = bus.defineSequence({
    id: 'login-flow',
    steps: [{ topic: 'user.login' }, { topic: 'user.purchase' }],
    withinMs: 10_000,
  });
  assert.equal(id, 'login-flow');
  bus.publish('user.login', { u: 1 }, { key: 'k1' });
  bus.publish('user.purchase', { u: 1, amt: 5 }, { key: 'k1' });
  await flush();
  assert.equal(seen.length, 1);
  const payload = seen[0].payload as Record<string, unknown>;
  assert.equal(payload.patternId, 'login-flow');
  assert.equal(payload.key, 'k1');
  assert.equal((payload.events as unknown[]).length, 2);
  assert.deepEqual(
    (payload.events as Array<{ topic: string }>).map((e) => e.topic),
    ['user.login', 'user.purchase'],
  );
  assert.ok((payload.durationMs as number) >= 0);
  assert.ok((payload.at as number) > 0);
  const stats = bus.getStats().cep!;
  assert.equal(stats.activePatterns, 1);
  assert.equal(stats.partialMatches, 0);
  assert.equal(stats.matches, 1);
});

test('CEP: per-key isolation — A on k1, B on k2 never matches', async () => {
  const bus = new EventBus();
  const seen = collect(bus, 'cep.**');
  bus.defineSequence({
    id: 'iso',
    steps: [{ topic: 'a' }, { topic: 'b' }],
    withinMs: 10_000,
  });
  bus.publish('a', 1, { key: 'k1' });
  bus.publish('b', 1, { key: 'k2' });
  await flush();
  assert.equal(seen.length, 0);
  assert.equal(bus.getStats().cep!.matches, 0);
  // A partial is still tracked for k1 (window not elapsed).
  assert.equal(bus.getStats().cep!.partialMatches, 1);
});

test('CEP: window expiry drops the partial (injectable clock)', async () => {
  let now = 1_000_000;
  const bus = new EventBus({ now: () => now });
  const seen = collect(bus, 'cep.**');
  bus.defineSequence({ id: 'win', steps: [{ topic: 'a' }, { topic: 'b' }], withinMs: 1000 });
  bus.publish('a', 1, { key: 'k' });
  assert.equal(bus.getStats().cep!.partialMatches, 1);
  now += 2000; // past the window
  bus.publish('b', 1, { key: 'k' });
  await flush();
  assert.equal(seen.length, 0);
  assert.equal(bus.getStats().cep!.partialMatches, 0);
  assert.equal(bus.getStats().cep!.matches, 0);
});

test('CEP: wildcard steps', async () => {
  const bus = new EventBus();
  const seen = collect(bus, 'cep.wc');
  bus.defineSequence({
    id: 'wc',
    steps: [{ topic: 'orders.*' }, { topic: 'payments.**' }],
    withinMs: 10_000,
  });
  bus.publish('orders.created', { o: 1 }, { key: 'k' });
  bus.publish('payments.eu.settled', { o: 1 }, { key: 'k' });
  await flush();
  assert.equal(seen.length, 1);
});

test('CEP: repeated step topics (A->A)', async () => {
  const bus = new EventBus();
  const seen = collect(bus, 'cep.rep');
  bus.defineSequence({ id: 'rep', steps: [{ topic: 'tick' }, { topic: 'tick' }], withinMs: 10_000 });
  bus.publish('tick', 1, { key: 'k' });
  bus.publish('tick', 2, { key: 'k' });
  await flush();
  assert.equal(seen.length, 1);
  const payload = seen[0].payload as { events: Array<{ payload: unknown }> };
  assert.deepEqual(
    payload.events.map((e) => e.payload),
    [1, 2],
  );
});

test('CEP: maxPartialMatches bounds memory (oldest evicted first)', () => {
  const bus = new EventBus();
  bus.defineSequence({
    id: 'bound',
    steps: [{ topic: 'a' }, { topic: 'b' }],
    withinMs: 60_000,
    maxPartialMatches: 3,
  });
  for (let i = 0; i < 5; i += 1) bus.publish('a', i, { key: `k${i}` });
  assert.equal(bus.getStats().cep!.partialMatches, 3);
  // The two oldest partials were evicted: k3/k4 survive, k0-k2 do not.
  bus.publish('b', 'x', { key: 'k4' });
  assert.equal(bus.getStats().cep!.matches, 1);
  bus.publish('b', 'x', { key: 'k0' });
  assert.equal(bus.getStats().cep!.matches, 1);
});

test('CEP: custom keyOf correlates on a payload field', async () => {
  const bus = new EventBus();
  const seen = collect(bus, 'cep.ck');
  bus.defineSequence({
    id: 'ck',
    steps: [{ topic: 'a' }, { topic: 'b' }],
    withinMs: 10_000,
    keyOf: (msg) => (msg.payload as { session?: string }).session,
  });
  // No publish keys at all — correlation rides the payload field.
  bus.publish('a', { session: 's1' });
  bus.publish('b', { session: 's1' });
  await flush();
  assert.equal(seen.length, 1);
  assert.equal((seen[0].payload as { key: string }).key, 's1');
});

test('CEP: match events never re-enter pattern detection', async () => {
  const bus = new EventBus();
  const inner = collect(bus, 'cep.inner');
  const chained = collect(bus, 'cep.chained');
  bus.defineSequence({ id: 'inner', steps: [{ topic: 'a' }, { topic: 'b' }], withinMs: 10_000 });
  // A pattern that watches the *derived* topic: the engine-emitted match
  // must not feed it, but a direct publish to the same topic may.
  bus.defineSequence({ id: 'chained', steps: [{ topic: 'cep.inner' }, { topic: 'c' }], withinMs: 10_000 });
  bus.publish('a', 1, { key: 'k' });
  bus.publish('b', 1, { key: 'k' });
  bus.publish('c', 1, { key: 'k' });
  await flush();
  assert.equal(inner.length, 1);
  assert.equal(chained.length, 0);
  assert.equal(bus.getStats().cep!.matches, 1);
  // Direct publishes to the derived topic are ordinary traffic: they match.
  bus.publish('cep.inner', { manual: true });
  bus.publish('c', 2);
  await flush();
  assert.equal(chained.length, 1);
  assert.equal(bus.getStats().cep!.matches, 2);
});

test('CEP: clearSequence removes the pattern', async () => {
  const bus = new EventBus();
  const seen = collect(bus, 'cep.**');
  bus.defineSequence({ id: 'tmp', steps: [{ topic: 'a' }, { topic: 'b' }], withinMs: 10_000 });
  assert.equal(bus.clearSequence('tmp'), true);
  assert.equal(bus.clearSequence('tmp'), false);
  bus.publish('a', 1);
  bus.publish('b', 1);
  await flush();
  assert.equal(seen.length, 0);
  // The engine stays (its counters survive), but no pattern is active.
  assert.deepEqual(bus.getStats().cep, {
    activePatterns: 0,
    partialMatches: 0,
    matches: 0,
    absencePatterns: 0,
    absenceMatches: 0,
  });
});

test('CEP: validation fails fast', () => {
  const bus = new EventBus();
  assert.throws(() => bus.defineSequence({ steps: [{ topic: 'a' }], withinMs: 1000 } as never), RangeError);
  assert.throws(() => bus.defineSequence({ steps: [], withinMs: 1000 }), RangeError);
  assert.throws(
    () => bus.defineSequence({ steps: [{ topic: 'a' }, { topic: 'b' }], withinMs: 0 }),
    RangeError,
  );
  assert.throws(
    () => bus.defineSequence({ steps: [{ topic: '' }, { topic: 'b' }], withinMs: 1000 }),
    RangeError,
  );
  assert.throws(
    () => bus.defineSequence({ id: 'bad id!', steps: [{ topic: 'a' }, { topic: 'b' }], withinMs: 1000 }),
    RangeError,
  );
  assert.throws(
    () =>
      bus.defineSequence({
        steps: [{ topic: 'a' }, { topic: 'b' }],
        withinMs: 1000,
        maxPartialMatches: 0,
      }),
    RangeError,
  );
  bus.defineSequence({ id: 'dup', steps: [{ topic: 'a' }, { topic: 'b' }], withinMs: 1000 });
  assert.throws(
    () => bus.defineSequence({ id: 'dup', steps: [{ topic: 'a' }, { topic: 'b' }], withinMs: 1000 }),
    RangeError,
  );
});

test('CEP: a throwing keyOf is error-isolated (pattern skips the message)', async () => {
  const bus = new EventBus();
  const seen = collect(bus, 'cep.**');
  bus.defineSequence({
    id: 'boom',
    steps: [{ topic: 'a' }, { topic: 'b' }],
    withinMs: 10_000,
    keyOf: () => {
      throw new Error('broken extractor');
    },
  });
  bus.publish('a', 1);
  bus.publish('b', 1);
  await flush();
  assert.equal(seen.length, 0);
  assert.equal(bus.getStats().cep!.matches, 0);
});

test('CEP: stats shape is unchanged without patterns; metrics gated', () => {
  const plain = new EventBus();
  assert.equal(plain.getStats().cep, undefined);
  const plainProm = renderPrometheus(plain.getStats());
  assert.ok(!plainProm.includes('eventbus_cep_'));

  const bus = new EventBus();
  bus.defineSequence({ steps: [{ topic: 'a' }, { topic: 'b' }], withinMs: 1000 });
  bus.publish('a', 1);
  const prom = renderPrometheus(bus.getStats());
  assert.ok(prom.includes('eventbus_cep_patterns 1'));
  assert.ok(prom.includes('eventbus_cep_partial_matches 1'));
  assert.ok(prom.includes('eventbus_cep_matches_total 0'));
});

test('CEP: matchTopic helper', () => {
  assert.equal(CepEngine.matchTopic('foo'), 'cep.foo');
});
