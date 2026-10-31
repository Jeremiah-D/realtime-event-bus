/**
 * CEP sequence-pattern revision (EB-297).
 *
 * `updateCepSequencePattern` retunes a defined pattern's window and
 * partial bound live — the steps, the correlation key, and the pattern
 * id are immutable. Only a defined pattern can be revised; illegal
 * values fail closed with the live values and the audit log untouched;
 * an identical (or empty) revision is a no-op with no audit.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage } from '../src/bus.ts';

/** Yields until the bus's async flush has delivered. */
const flush = () => new Promise<void>((resolve) => setTimeout(resolve, 20));

function collect(bus: EventBus, pattern = '**'): BusMessage[] {
  const seen: BusMessage[] = [];
  bus.subscribe(pattern, (msg) => {
    seen.push(msg);
  });
  return seen;
}

test('updateCepSequencePattern fails closed with no patterns or an unknown id', () => {
  const bus = new EventBus();
  assert.throws(() => bus.updateCepSequencePattern('nope', { withinMs: 100 }), RangeError);
  assert.throws(() => bus.updateCepSequencePattern('', { withinMs: 100 }), RangeError);
  bus.defineSequence({
    id: 'p',
    steps: [{ topic: 'a' }, { topic: 'b' }],
    withinMs: 1000,
  });
  assert.throws(() => bus.updateCepSequencePattern('ghost', { withinMs: 100 }), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'cep_sequence_pattern_revised' }).length, 0);
});

test('updateCepSequencePattern rejects illegal values and leaves the live values untouched', async () => {
  let now = 1_000_000;
  const bus = new EventBus({ now: () => now });
  const seen = collect(bus, 'cep.p');
  bus.defineSequence({
    id: 'p',
    steps: [{ topic: 'a' }, { topic: 'b' }],
    withinMs: 1000,
    maxPartialMatches: 100,
  });
  const bad: Array<unknown> = [
    { withinMs: 0 },
    { withinMs: -1 },
    { withinMs: Number.NaN },
    { withinMs: Infinity },
    { maxPartialMatches: 0 },
    { maxPartialMatches: 1.5 },
    { maxPartialMatches: -1 },
    'nope',
    42,
  ];
  for (const revision of bad) {
    assert.throws(
      () => bus.updateCepSequencePattern('p', revision as never),
      RangeError,
      `expected RangeError for ${JSON.stringify(revision)}`,
    );
  }
  // The failed calls changed nothing: the original window still applies.
  bus.publish('a', 1, { key: 'k' });
  now += 500;
  bus.publish('b', 1, { key: 'k' });
  await flush();
  assert.equal(seen.length, 1);
  assert.equal(bus.getSubscriptionAudit({ action: 'cep_sequence_pattern_revised' }).length, 0);
});

test('updateCepSequencePattern with identical values or an empty revision is a no-op with no audit', () => {
  const bus = new EventBus();
  bus.defineSequence({
    id: 'p',
    steps: [{ topic: 'a' }, { topic: 'b' }],
    withinMs: 1000,
    maxPartialMatches: 50,
  });
  bus.updateCepSequencePattern('p', { withinMs: 1000 });
  bus.updateCepSequencePattern('p', { maxPartialMatches: 50 });
  bus.updateCepSequencePattern('p', { withinMs: 1000, maxPartialMatches: 50 });
  bus.updateCepSequencePattern('p', {});
  bus.updateCepSequencePattern('p', null as never);
  assert.equal(bus.getSubscriptionAudit({ action: 'cep_sequence_pattern_revised' }).length, 0);
});

test('withinMs shrink expires the in-flight partial on the next observation', async () => {
  let now = 1_000_000;
  const bus = new EventBus({ now: () => now });
  const seen = collect(bus, 'cep.win');
  bus.defineSequence({
    id: 'win',
    steps: [{ topic: 'a' }, { topic: 'b' }],
    withinMs: 10_000,
  });
  bus.publish('a', 1, { key: 'k' });
  assert.equal(bus.getStats().cep!.partialMatches, 1);
  bus.updateCepSequencePattern('win', { withinMs: 100 });
  now += 200; // past the shrunken window, inside the old one
  bus.publish('b', 1, { key: 'k' });
  await flush();
  assert.equal(seen.length, 0);
  assert.equal(bus.getStats().cep!.partialMatches, 0);
  assert.equal(bus.getStats().cep!.matches, 0);
  const audit = bus.getSubscriptionAudit({ action: 'cep_sequence_pattern_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].pattern, 'win');
  assert.deepEqual(audit[0].detail, {
    withinMs: 100,
    oldWithinMs: 10_000,
    maxPartialMatches: 1000,
    oldMaxPartialMatches: 1000,
  });
});

test('withinMs widen extends the in-flight partial past the old window', async () => {
  let now = 1_000_000;
  const bus = new EventBus({ now: () => now });
  const seen = collect(bus, 'cep.win');
  bus.defineSequence({
    id: 'win',
    steps: [{ topic: 'a' }, { topic: 'b' }],
    withinMs: 100,
  });
  bus.publish('a', 1, { key: 'k' });
  bus.updateCepSequencePattern('win', { withinMs: 10_000 });
  now += 200; // past the old window, inside the new one
  bus.publish('b', 1, { key: 'k' });
  await flush();
  assert.equal(seen.length, 1);
  assert.equal(bus.getStats().cep!.matches, 1);
});

test('maxPartialMatches tightening evicts the oldest partials on the next new partial', async () => {
  const bus = new EventBus();
  const seen = collect(bus, 'cep.cap');
  bus.defineSequence({
    id: 'cap',
    steps: [{ topic: 'a' }, { topic: 'b' }],
    withinMs: 10_000,
    maxPartialMatches: 3,
  });
  bus.publish('a', 1, { key: 'k1' });
  bus.publish('a', 1, { key: 'k2' });
  bus.publish('a', 1, { key: 'k3' });
  assert.equal(bus.getStats().cep!.partialMatches, 3);
  bus.updateCepSequencePattern('cap', { maxPartialMatches: 1 });
  // Tightening alone evicts nothing — the next new partial enforces it:
  // the three older partials are evicted, oldest first, leaving only k4.
  assert.equal(bus.getStats().cep!.partialMatches, 3);
  bus.publish('a', 1, { key: 'k4' });
  assert.equal(bus.getStats().cep!.partialMatches, 1);
  bus.publish('b', 1, { key: 'k2' });
  await flush();
  assert.equal(seen.length, 0, 'expected k2 (evicted) to complete nothing');
  bus.publish('b', 1, { key: 'k4' });
  await flush();
  assert.equal(seen.length, 1);
  assert.equal(bus.getStats().cep!.matches, 1);
  const audit = bus.getSubscriptionAudit({ action: 'cep_sequence_pattern_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    withinMs: 10_000,
    oldWithinMs: 10_000,
    maxPartialMatches: 1,
    oldMaxPartialMatches: 3,
  });
});

test('partial revision moves only the provided field', () => {
  const bus = new EventBus();
  bus.defineSequence({
    id: 'p',
    steps: [{ topic: 'a' }, { topic: 'b' }],
    withinMs: 1000,
    maxPartialMatches: 25,
  });
  bus.updateCepSequencePattern('p', { maxPartialMatches: 5 });
  const audit = bus.getSubscriptionAudit({ action: 'cep_sequence_pattern_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    withinMs: 1000,
    oldWithinMs: 1000,
    maxPartialMatches: 5,
    oldMaxPartialMatches: 25,
  });
});

test('revision keeps the steps and the correlation key: matches still complete on the original definition', async () => {
  let now = 1_000_000;
  const bus = new EventBus({ now: () => now });
  const seen = collect(bus, 'cep.keyed');
  bus.defineSequence({
    id: 'keyed',
    steps: [{ topic: 'a' }, { topic: 'b' }],
    withinMs: 1000,
    keyOf: (msg) => String((msg.payload as { u: number }).u),
  });
  bus.publish('a', { u: 7 });
  bus.updateCepSequencePattern('keyed', { withinMs: 5000 });
  // The key still comes from the payload field, not the publish key.
  bus.publish('b', { u: 7 }, { key: 'other' });
  await flush();
  assert.equal(seen.length, 1);
  const payload = seen[0].payload as { key: string };
  assert.equal(payload.key, '7');
});
