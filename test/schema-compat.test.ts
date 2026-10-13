import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const isV0 = (payload: unknown) => (payload as { v?: number })?.v === 0;
/** A breaking v1 validator: rejects every old-version payload outright. */
const isStrictlyV1 = (payload: unknown) => (payload as { v?: number })?.v === 1;
/** A compatible v1 validator: still accepts the old v0 shape. */
const isV1Compat = (payload: unknown) => {
  const v = (payload as { v?: number })?.v;
  return v === 0 || v === 1;
};
/** Payloads the v0 validator historically accepted. */
const v0Corpus = [{ v: 0 }, { v: 0, extra: 'x' }];

function schemaRow(bus: EventBus, rule: string) {
  const row = bus.getStats().topicSchemas.find((r) => r.rule === rule);
  assert.ok(row, `expected a topicSchemas row for "${rule}"`);
  return row;
}

test('EB-95: breaking validator fails the gate and registers nothing', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.setTopicSchemaVersion('t', 0, isV0);
  assert.throws(
    () =>
      bus.setTopicSchemaVersion('t', 1, isStrictlyV1, {
        compatibilityCorpus: v0Corpus,
      }),
    (err: unknown) =>
      err instanceof RangeError &&
      /corpus sample \d+/.test(err.message),
  );
  // Zero side effects: v1 never became visible, v0 keeps its state —
  // no supersededAt stamped, latest still 0, and a publish pinned to
  // version 1 is rejected exactly like a never-registered version.
  const row = schemaRow(bus, 't');
  assert.equal(row.latest, 0);
  assert.equal(row.versions.length, 1);
  assert.equal(row.versions[0].version, 0);
  assert.equal(row.versions[0].compatGate, 'not-tested');
  assert.equal(row.versions[0].supersededAt, undefined);
  assert.equal(bus.publish('t', { v: 1 }, { schemaVersion: 1 }), 0);
  assert.equal(bus.getStats().topics[0].rejectedMessages, 1);
  // Retrying with a compatible validator after the failure registers fine.
  bus.setTopicSchemaVersion('t', 1, isV1Compat, {
    compatibilityCorpus: v0Corpus,
  });
  const after = schemaRow(bus, 't');
  assert.equal(after.latest, 1);
  assert.equal(after.versions[1].compatGate, 'passed');
  assert.equal(after.versions[1].compatCorpusSize, 2);
  assert.ok(after.versions[0].supersededAt !== undefined);
  assert.equal(bus.publish('t', { v: 0 }), 1);
});

test('EB-95: gate failure on a fresh pattern leaves no rule set behind', () => {
  const bus = new EventBus();
  assert.equal(bus.getStats().topicSchemas.length, 0);
  assert.throws(
    () =>
      bus.setTopicSchemaVersion('fresh', 0, isStrictlyV1, {
        compatibilityCorpus: v0Corpus,
      }),
    RangeError,
  );
  // Not even an empty rule set may linger — the gate ran before any
  // registration state was touched.
  assert.equal(bus.getStats().topicSchemas.length, 0);
});

test('EB-95: throwing validator on a corpus sample fails the gate', () => {
  const bus = new EventBus();
  const exploding = (payload: unknown) => {
    if ((payload as { bad?: boolean })?.bad === true) throw new Error('boom');
    return true;
  };
  assert.throws(
    () =>
      bus.setTopicSchemaVersion('t', 0, exploding, {
        compatibilityCorpus: [{ ok: 1 }, { bad: true }],
      }),
    (err: unknown) =>
      err instanceof RangeError && /threw on corpus sample 1/.test(err.message),
  );
  assert.equal(bus.getStats().topicSchemas.length, 0);
});

test('EB-95: backfilled lower version is gated the same as a new latest', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicSchemaVersion('t', 1, isV1Compat);
  const v1RegisteredAt = schemaRow(bus, 't').versions[0].registeredAt;
  // Backfill v0 with a validator that cannot accept the old corpus:
  // rejected, with v1 untouched.
  assert.throws(
    () =>
      bus.setTopicSchemaVersion('t', 0, isStrictlyV1, {
        compatibilityCorpus: v0Corpus,
      }),
    RangeError,
  );
  const row = schemaRow(bus, 't');
  assert.equal(row.latest, 1);
  assert.equal(row.versions.length, 1);
  assert.equal(row.versions[0].version, 1);
  assert.equal(row.versions[0].registeredAt, v1RegisteredAt);
  assert.equal(row.versions[0].supersededAt, undefined);
  // Backfill v0 with a compatible validator passes the gate; it is born
  // superseded (a greater version already exists) and reports 'passed'.
  bus.setTopicSchemaVersion('t', 0, isV0, {
    compatibilityCorpus: v0Corpus,
  });
  const after = schemaRow(bus, 't');
  assert.equal(after.latest, 1);
  const backfill = after.versions.find((v) => v.version === 0);
  assert.ok(backfill);
  assert.equal(backfill.compatGate, 'passed');
  assert.equal(backfill.compatCorpusSize, 2);
  assert.ok(backfill.supersededAt !== undefined);
});

test('EB-95: non-array corpus is rejected before anything registers', () => {
  const bus = new EventBus();
  for (const bad of ['nope', 42, { v: 0 }, null]) {
    assert.throws(
      () =>
        bus.setTopicSchemaVersion('t', 0, isV0, {
          compatibilityCorpus: bad as never,
        }),
      (err: unknown) =>
        err instanceof RangeError &&
        /compatibilityCorpus must be an array/.test(err.message),
    );
  }
  assert.equal(bus.getStats().topicSchemas.length, 0);
});

test('EB-95: duplicate version still throws the duplicate error with a corpus', () => {
  const bus = new EventBus();
  bus.setTopicSchemaVersion('t', 0, isV0, { compatibilityCorpus: v0Corpus });
  // The corpus would pass, so the error must come from the duplicate check.
  assert.throws(
    () =>
      bus.setTopicSchemaVersion('t', 0, isV0, {
        compatibilityCorpus: v0Corpus,
      }),
    (err: unknown) =>
      err instanceof RangeError && /already registered/.test(err.message),
  );
  assert.equal(schemaRow(bus, 't').versions.length, 1);
});

test('EB-95: gate invokes the validator with the registration pattern as topic', () => {
  const bus = new EventBus();
  const seenTopics: unknown[] = [];
  bus.setTopicSchemaVersion('orders.*', 0, (payload, topic) => {
    seenTopics.push(topic);
    return isV0(payload);
  }, { compatibilityCorpus: v0Corpus });
  assert.deepEqual(seenTopics, ['orders.*', 'orders.*']);
  assert.equal(schemaRow(bus, 'orders.*').versions[0].compatGate, 'passed');
});

test('EB-95: empty corpus passes trivially and reports zero samples', () => {
  const bus = new EventBus();
  bus.setTopicSchemaVersion('t', 0, isStrictlyV1, { compatibilityCorpus: [] });
  const version = schemaRow(bus, 't').versions[0];
  assert.equal(version.compatGate, 'passed');
  assert.equal(version.compatCorpusSize, 0);
});

test('EB-95: versions without a corpus report not-tested with zero samples', () => {
  const bus = new EventBus();
  bus.setTopicSchemaVersion('a', 0, isV0);
  bus.setTopicSchemaVersion('b', 0, isV0, {});
  bus.setTopicSchemaVersion('c', 0, isV0, { compatibilityCorpus: undefined });
  for (const rule of ['a', 'b', 'c']) {
    const version = schemaRow(bus, rule).versions[0];
    assert.equal(version.compatGate, 'not-tested');
    assert.equal(version.compatCorpusSize, 0);
  }
});
