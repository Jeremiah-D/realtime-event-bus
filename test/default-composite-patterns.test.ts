import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  type BusMessage,
  type CompositeSubscribeOptions,
} from '../src/bus.ts';

/**
 * Bus-level default composite subscription patterns (EB-510):
 * `setDefaultCompositePatterns` installs the `patterns` / `mode` /
 * `exclude` new opt-in `patterns` subscribers fall back to when they
 * omit a field — the priority chain is subscriber-explicit > bus
 * default > required (a missing `patterns` still throws) — and the
 * default never enables composite matching: a subscriber that does
 * not opt into `patterns` still receives every message at once.
 *
 * Assertions follow `composite-patterns.test.ts`: the base pattern is
 * `'**'` and the composite gate decides delivery, so the boundaries
 * assert exactly with no wall-clock waiting.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const collect = (
  bus: EventBus,
  pattern: string,
  opts?: Parameters<EventBus['subscribe']>[2],
) => {
  const received: BusMessage[] = [];
  const sub = bus.subscribe(pattern, (msg) => received.push(msg), opts);
  return { received, sub };
};

const topics = (received: BusMessage[]): string[] => received.map((m) => m.topic);

/** Canonical default: `all` over two overlapping includes, one excluded leg. */
const fleetDefault: CompositeSubscribeOptions = {
  patterns: ['a.**', 'a.b.*'],
  mode: 'all',
  exclude: ['a.b.x'],
};

test('EB-510: the factory bus default is unset (getter undefined, clearing audits nothing)', () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultCompositePatterns(), undefined);
  bus.setDefaultCompositePatterns(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_composite_patterns_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
});

test('EB-510: `patterns: {}` opts in and takes the whole default — all-mode match plus exclude, precisely', async () => {
  const bus = new EventBus();
  bus.setDefaultCompositePatterns(fleetDefault);
  const { received } = collect(bus, '**', { patterns: {} });
  bus.publish('a.b.1', 1);
  bus.publish('a.c.1', 2);
  bus.publish('a.b.x', 3);
  bus.publish('c.1', 4);
  await flush();
  // 'a.b.1' matches both includes and is not excluded; 'a.c.1' matches
  // only 'a.**' so 'all' refuses it; 'a.b.x' is excluded; 'c.1' matches
  // neither include.
  assert.deepEqual(topics(received), ['a.b.1']);
  assert.equal(bus.getStats().compositeSubscriptions, 1);
});

test('EB-510: an explicit patterns / mode / exclude each wins over the matching default field', async () => {
  // Explicit patterns win: mode 'all' and exclude still come from the
  // default, but the include list is the subscriber's own.
  {
    const bus = new EventBus();
    bus.setDefaultCompositePatterns(fleetDefault);
    const { received } = collect(bus, '**', { patterns: { patterns: ['c.*'] } });
    bus.publish('c.1', 1);
    bus.publish('a.b.1', 2);
    await flush();
    assert.deepEqual(topics(received), ['c.1']);
  }
  // Explicit mode wins: 'any' over the default 'all'.
  {
    const bus = new EventBus();
    bus.setDefaultCompositePatterns(fleetDefault);
    const { received } = collect(bus, '**', {
      patterns: { patterns: ['a.**', 'a.b.*'], mode: 'any' },
    });
    bus.publish('a.c.1', 1);
    bus.publish('a.b.1', 2);
    bus.publish('a.b.x', 3);
    await flush();
    assert.deepEqual(topics(received), ['a.c.1', 'a.b.1'], 'the default exclude still applies');
  }
  // Explicit exclude wins (replaces, does not union with, the default):
  // 'a.b.x' is deliverable again, 'a.b.1' is now the excluded leg.
  {
    const bus = new EventBus();
    bus.setDefaultCompositePatterns(fleetDefault);
    const { received } = collect(bus, '**', {
      patterns: { patterns: ['a.**', 'a.b.*'], exclude: ['a.b.1'] },
    });
    bus.publish('a.b.1', 1);
    bus.publish('a.b.2', 2);
    bus.publish('a.b.x', 3);
    await flush();
    assert.deepEqual(topics(received), ['a.b.2', 'a.b.x']);
  }
});

test('EB-510: a default of only patterns combines with explicit mode/exclude — and vice versa', async () => {
  // Default carries only patterns; the subscriber supplies mode/exclude.
  {
    const bus = new EventBus();
    bus.setDefaultCompositePatterns({ patterns: ['a.*'] });
    const { received } = collect(bus, '**', {
      patterns: { mode: 'all', exclude: ['a.x'] },
    });
    bus.publish('a.1', 1);
    bus.publish('a.x', 2);
    bus.publish('b.1', 3);
    await flush();
    assert.deepEqual(topics(received), ['a.1']);
  }
  // Vice versa: default carries only mode/exclude; the subscriber
  // supplies patterns.
  {
    const bus = new EventBus();
    bus.setDefaultCompositePatterns({ mode: 'all', exclude: ['a.b.z'] });
    const { received } = collect(bus, '**', {
      patterns: { patterns: ['a.**', 'a.b.*'] },
    });
    bus.publish('a.b.1', 1);
    bus.publish('a.1', 2);
    bus.publish('a.b.z', 3);
    await flush();
    assert.deepEqual(topics(received), ['a.b.1']);
  }
});

test('EB-510: an explicit exclude covering the merged include space throws and the live default stays put', async () => {
  const bus = new EventBus();
  bus.setDefaultCompositePatterns({ patterns: ['a.*'] });
  assert.throws(
    () => bus.subscribe('**', () => {}, { patterns: { exclude: ['a.*'] } }),
    RangeError,
    'the defaulted include is covered by the explicit exclude',
  );
  assert.throws(
    () => bus.subscribe('**', () => {}, { patterns: { patterns: ['a.*'], exclude: ['a.*'] } }),
    RangeError,
  );
  assert.equal(bus.getStats().compositeSubscriptions, 0, 'a failed subscribe registers nothing');
  assert.deepEqual(bus.getDefaultCompositePatterns(), { patterns: ['a.*'] });
  // The live default still converges for a later subscriber.
  const { received } = collect(bus, '**', { patterns: {} });
  bus.publish('a.1', 1);
  await flush();
  assert.deepEqual(topics(received), ['a.1']);
});

test('EB-510: clearing with null returns new opt-in subscribers to patterns being required', () => {
  const bus = new EventBus();
  bus.setDefaultCompositePatterns(fleetDefault);
  bus.setDefaultCompositePatterns(null);
  assert.equal(bus.getDefaultCompositePatterns(), undefined);
  assert.throws(() => bus.subscribe('**', () => {}, { patterns: {} }), RangeError);
  const audit = bus.getSubscriptionAudit({ action: 'default_composite_patterns_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, {
    previous: fleetDefault,
    patterns: null,
  });
});

test('EB-510: re-affirming the default is a no-op with no audit', () => {
  const bus = new EventBus();
  bus.setDefaultCompositePatterns({ patterns: ['a.*'] });
  // Restating the same value — with `mode: 'any'` spelled out (the
  // resolver default) and an explicit empty exclude (the same as a
  // missing one) — audits nothing.
  bus.setDefaultCompositePatterns({ patterns: ['a.*'], mode: 'any', exclude: [] });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_composite_patterns_revised' }).length,
    1,
    're-affirming the default audits nothing',
  );
  // Clearing twice: only the first clear audits.
  bus.setDefaultCompositePatterns(null);
  bus.setDefaultCompositePatterns(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_composite_patterns_revised' }).length,
    2,
  );
});

test('EB-510: illegal values throw RangeError with the live default untouched and no audit', () => {
  const bus = new EventBus();
  bus.setDefaultCompositePatterns(fleetDefault);
  const bad = [
    0,
    'x',
    [],
    true,
    undefined,
    { patterns: [] },
    { patterns: 'a.*' },
    { patterns: ['a.*', 1] },
    { patterns: [''] },
    { patterns: ['a.*'], mode: 'every' },
    { patterns: ['a.*'], mode: '' },
    { patterns: ['a.*'], exclude: 'a.x' },
    { patterns: ['a.*'], exclude: [1] },
    { patterns: ['a.*'], exclude: [''] },
    { patterns: ['a.*'], exclude: ['a.*'] },
    { mode: 'every' },
  ] as unknown[];
  for (const value of bad) {
    assert.throws(
      () => bus.setDefaultCompositePatterns(value as never),
      RangeError,
      `patterns=${String(value)}`,
    );
    assert.deepEqual(bus.getDefaultCompositePatterns(), fleetDefault);
  }
  assert.throws(() => bus.setDefaultCompositePatterns(fleetDefault, ''), RangeError);
  assert.deepEqual(bus.getDefaultCompositePatterns(), fleetDefault);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_composite_patterns_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});

test('EB-510: revising the bus default never moves an already-subscribed composite', async () => {
  const bus = new EventBus();
  bus.setDefaultCompositePatterns({ patterns: ['a.*'] });
  const first = collect(bus, '**', { patterns: {} });
  bus.publish('a.1', 1);
  bus.publish('b.1', 2);
  await flush();
  assert.deepEqual(topics(first.received), ['a.1']);
  // Revise the default: the baked subscriber keeps its 'a.*' gate.
  bus.setDefaultCompositePatterns({ patterns: ['b.*'] });
  bus.publish('a.2', 3);
  bus.publish('b.2', 4);
  await flush();
  assert.deepEqual(topics(first.received), ['a.1', 'a.2']);
  // A subscriber joining after the revision takes the new default.
  const second = collect(bus, '**', { patterns: {} });
  bus.publish('a.3', 5);
  bus.publish('b.3', 6);
  await flush();
  assert.deepEqual(topics(second.received), ['b.3']);
  assert.deepEqual(topics(first.received), ['a.1', 'a.2', 'a.3']);
});

test('EB-510: the default never enables composite matching for a subscriber that does not opt in', async () => {
  const bus = new EventBus();
  bus.setDefaultCompositePatterns(fleetDefault);
  const plain: BusMessage[] = [];
  bus.subscribe('**', (m) => plain.push(m));
  bus.publish('a.b.1', 1);
  bus.publish('zzz', 2);
  await flush();
  assert.deepEqual(topics(plain), ['a.b.1', 'zzz']);
  assert.equal(bus.getStats().compositeSubscriptions, 0);
});

test('EB-510: a default-baked subscriber still takes partial setCompositePatterns revisions, and clearing never re-applies the default', async () => {
  const bus = new EventBus();
  bus.setDefaultCompositePatterns(fleetDefault);
  const { received, sub } = collect(bus, '**', { patterns: {} });
  bus.publish('a.b.1', 1);
  bus.publish('a.c.1', 2);
  await flush();
  assert.deepEqual(topics(received), ['a.b.1'], 'baked mode is all');
  // The per-subscriber revision is a partial merge against the
  // subscriber's own live values — it never consults the bus
  // default: only the mode moves, patterns and exclude stay baked.
  bus.setCompositePatterns(sub.id, { mode: 'any' });
  bus.publish('a.c.1', 3);
  bus.publish('a.b.x', 4);
  await flush();
  assert.deepEqual(topics(received), ['a.b.1', 'a.c.1'], 'the baked exclude still drops a.b.x');
  // Clearing the subscriber's composite degrades it to a plain
  // single-pattern subscription — the bus default does not re-apply.
  bus.setCompositePatterns(sub.id, null);
  bus.publish('zzz', 5);
  await flush();
  assert.deepEqual(topics(received), ['a.b.1', 'a.c.1', 'zzz']);
  assert.throws(() => bus.setCompositePatterns(sub.id, { mode: 'all' }), RangeError);
  // Clearing the bus default does not move the subscriber either.
  bus.setDefaultCompositePatterns(null);
  bus.publish('qqq', 6);
  await flush();
  assert.deepEqual(topics(received), ['a.b.1', 'a.c.1', 'zzz', 'qqq']);
});

test('EB-510: the audit records previous/patterns exactly, the getter follows the live value, and both copies are defensive', () => {
  const bus = new EventBus();
  const opts: CompositeSubscribeOptions = {
    patterns: ['a.**', 'a.b.*'],
    mode: 'all',
    exclude: ['a.b.x'],
  };
  bus.setDefaultCompositePatterns(opts, 'ops');
  // Mutating the caller's object must not move the live default.
  opts.patterns.push('c.*');
  opts.exclude?.push('c.y');
  assert.deepEqual(bus.getDefaultCompositePatterns(), fleetDefault);
  const audit = bus.getSubscriptionAudit({ action: 'default_composite_patterns_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { previous: null, patterns: fleetDefault });
  assert.equal(audit[0].actor, 'ops');
  // Mutating the getter's arrays must not move the live default.
  const held = bus.getDefaultCompositePatterns();
  assert.ok(held !== undefined);
  held.patterns?.push('c.*');
  held.exclude?.push('c.y');
  assert.deepEqual(bus.getDefaultCompositePatterns(), fleetDefault);
  // A revision moves the live value and audits both sides.
  bus.setDefaultCompositePatterns({ patterns: ['b.*'] }, 'ops');
  assert.deepEqual(bus.getDefaultCompositePatterns(), { patterns: ['b.*'] });
  const revised = bus.getSubscriptionAudit({ action: 'default_composite_patterns_revised' });
  assert.equal(revised.length, 2);
  assert.deepEqual(revised[1].detail, {
    previous: fleetDefault,
    patterns: { patterns: ['b.*'] },
  });
  assert.equal(revised[1].actor, 'ops');
  bus.setDefaultCompositePatterns(null, 'ops');
  const cleared = bus.getSubscriptionAudit({ action: 'default_composite_patterns_revised' });
  assert.equal(cleared.length, 3);
  assert.deepEqual(cleared[2].detail, {
    previous: { patterns: ['b.*'] },
    patterns: null,
  });
});

test('EB-510: the default never enters a config snapshot', () => {
  const bus = new EventBus();
  bus.setDefaultCompositePatterns(fleetDefault);
  const snapshot = bus.exportBusConfig();
  assert.ok(!('defaultCompositePatterns' in snapshot), 'no defaultCompositePatterns key');
  assert.ok(
    !JSON.stringify(snapshot).includes('defaultCompositePatterns'),
    'the field name appears nowhere in the exported snapshot',
  );
  assert.deepEqual(bus.getDefaultCompositePatterns(), fleetDefault);
});
