import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type BusMessage, type CompositeSubscribeOptions } from '../src/bus.ts';

/**
 * Live revision of composite include/exclude patterns (EB-387):
 * `setCompositePatterns` retunes a running composite subscription's
 * filter rules without unsubscribing and re-subscribing.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-composite-revision-'));

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

const compositeAudit = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'composite_patterns_revised' });

test('adding an exclude rejects the noisy leg at once; the other leg keeps flowing', async () => {
  const bus = new EventBus();
  const { received, sub } = collect(bus, '**', {
    patterns: { patterns: ['market.**'] },
  });
  bus.publish('market.btc.trades', { n: 1 });
  bus.publish('market.eth.trades', { n: 2 });
  await flush();
  assert.deepEqual(topics(received), ['market.btc.trades', 'market.eth.trades']);

  bus.setCompositePatterns(sub.id, { exclude: ['market.btc.*'] });
  bus.publish('market.btc.trades', { n: 3 });
  bus.publish('market.eth.trades', { n: 4 });
  await flush();
  assert.deepEqual(topics(received), [
    'market.btc.trades',
    'market.eth.trades',
    'market.eth.trades',
  ]);
});

test('widening the include list delivers the new leg at once', async () => {
  const bus = new EventBus();
  const { received, sub } = collect(bus, '**', {
    patterns: { patterns: ['market.btc.*'] },
  });
  bus.publish('market.eth.trades', { n: 1 });
  await flush();
  assert.deepEqual(topics(received), []);

  bus.setCompositePatterns(sub.id, { patterns: ['market.btc.*', 'market.eth.*'] });
  bus.publish('market.eth.trades', { n: 2 });
  await flush();
  assert.deepEqual(topics(received), ['market.eth.trades']);
});

test('switching mode changes the match semantics at once', async () => {
  const bus = new EventBus();
  const { received, sub } = collect(bus, '**', {
    patterns: { patterns: ['market.*', '*.btc'], mode: 'all' },
  });
  bus.publish('market.btc', { n: 1 });
  bus.publish('market.eth', { n: 2 });
  bus.publish('fx.btc', { n: 3 });
  await flush();
  assert.deepEqual(topics(received), ['market.btc']);

  bus.setCompositePatterns(sub.id, { mode: 'any' });
  bus.publish('market.eth', { n: 4 });
  bus.publish('fx.btc', { n: 5 });
  bus.publish('orders.new', { n: 6 });
  await flush();
  assert.deepEqual(topics(received), [
    'market.btc',
    'market.eth',
    'fx.btc',
  ]);
});

test('illegal values throw RangeError and leave the live patterns untouched', async () => {
  const bus = new EventBus();
  const { received, sub } = collect(bus, '**', {
    patterns: { patterns: ['market.**'], exclude: ['market.btc.*'] },
  });
  const bad: Array<[string, unknown]> = [
    ['non-object', 'x'],
    ['array', []],
    ['undefined', undefined],
    ['empty patterns', { patterns: [] }],
    ['empty pattern string', { patterns: [''] }],
    ['bad mode', { mode: 'every' }],
    ['exclude covering include', { exclude: ['market.**'] }],
    ['exclude covering one include in any mode', { patterns: ['a.*', 'b.*'], exclude: ['a.**', 'b.**'] }],
  ];
  for (const [label, opts] of bad) {
    assert.throws(
      () => bus.setCompositePatterns(sub.id, opts as CompositeSubscribeOptions),
      RangeError,
      label,
    );
  }
  assert.throws(() => bus.setCompositePatterns('nope', { mode: 'all' }), RangeError);
  assert.equal(compositeAudit(bus).length, 0);
  // The live state is exactly the subscribe-time one.
  bus.publish('market.btc.trades', { n: 1 });
  bus.publish('market.eth.trades', { n: 2 });
  await flush();
  assert.deepEqual(topics(received), ['market.eth.trades']);
});

test('a revision identical to the live patterns is a no-op with no audit', () => {
  const bus = new EventBus();
  const { sub } = collect(bus, '**', {
    patterns: { patterns: ['market.**'], mode: 'any', exclude: ['market.btc.*'] },
  });
  // Restating every field, and restating only one field, both no-op.
  bus.setCompositePatterns(sub.id, {
    patterns: ['market.**'],
    mode: 'any',
    exclude: ['market.btc.*'],
  });
  bus.setCompositePatterns(sub.id, { mode: 'any' });
  bus.setCompositePatterns(sub.id, { exclude: ['market.btc.*'] });
  assert.equal(compositeAudit(bus).length, 0);
  bus.setCompositePatterns(sub.id, { mode: 'all' });
  assert.equal(compositeAudit(bus).length, 1);
});

test('audit pins previous and current, including the first revision previous', () => {
  const bus = new EventBus();
  const { sub } = collect(bus, '**', {
    patterns: { patterns: ['market.**'] },
  });
  bus.setCompositePatterns(sub.id, { exclude: ['market.btc.*'] }, 'ops');
  bus.setCompositePatterns(sub.id, { mode: 'all', patterns: ['market.*', '*.btc'] }, 'ops');
  const audit = compositeAudit(bus);
  assert.equal(audit.length, 2);
  assert.equal(audit[0].subId, sub.id);
  assert.equal(audit[0].pattern, '**');
  assert.deepEqual(audit[0].detail, {
    patterns: ['market.**'],
    exclude: ['market.btc.*'],
    mode: 'any',
    oldPatterns: ['market.**'],
    oldExclude: [],
    oldMode: 'any',
  });
  assert.deepEqual(audit[1].detail, {
    patterns: ['market.*', '*.btc'],
    exclude: ['market.btc.*'],
    mode: 'all',
    oldPatterns: ['market.**'],
    oldExclude: ['market.btc.*'],
    oldMode: 'any',
  });
  assert.equal(audit[0].actor, 'ops');
});

test('null clears the composite: the subscription degrades and later revisions refuse', async () => {
  const bus = new EventBus();
  const { received, sub } = collect(bus, '**', {
    patterns: { patterns: ['market.**'], exclude: ['market.btc.*'] },
  });
  assert.equal(bus.getStats().compositeSubscriptions, 1);
  bus.setCompositePatterns(sub.id, null);
  assert.equal(bus.getStats().compositeSubscriptions, 0);
  // The base pattern still matches everything — including the leg the
  // composite used to exclude.
  bus.publish('market.btc.trades', { n: 1 });
  bus.publish('orders.new', { n: 2 });
  await flush();
  assert.deepEqual(topics(received), ['market.btc.trades', 'orders.new']);
  // Cleared means gone: a later revision is refused until opt-in again.
  assert.throws(() => bus.setCompositePatterns(sub.id, { mode: 'all' }), RangeError);
  const audit = compositeAudit(bus);
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    patterns: null,
    exclude: null,
    mode: null,
    oldPatterns: ['market.**'],
    oldExclude: ['market.btc.*'],
    oldMode: 'any',
  });
});

test('a subscription that never opted into composite refuses revision', () => {
  const bus = new EventBus();
  const plain = bus.subscribe('**', () => {});
  assert.throws(() => bus.setCompositePatterns(plain.id, { mode: 'all' }), RangeError);
  assert.throws(() => bus.setCompositePatterns(plain.id, null), RangeError);
  assert.equal(compositeAudit(bus).length, 0);
});

test('a namespaced revision keeps the prefix translation', async () => {
  const bus = new EventBus();
  const t1 = bus.createNamespace('t1');
  const t2 = bus.createNamespace('t2');
  const received: BusMessage[] = [];
  const sub = t1.subscribe(
    '**',
    (msg) => received.push(msg),
    { patterns: { patterns: ['market.**'] } },
  );
  t1.publish('market.btc.trades', { n: 1 });
  t1.publish('market.eth.trades', { n: 2 });
  // Another namespace's identical topic must never leak in.
  t2.publish('market.eth.trades', { n: 3 });
  await flush();
  assert.deepEqual(topics(received), ['t1/market.btc.trades', 't1/market.eth.trades']);

  bus.setCompositePatterns(sub.id, { exclude: ['market.btc.*'] });
  t1.publish('market.btc.trades', { n: 4 });
  t1.publish('market.eth.trades', { n: 5 });
  await flush();
  assert.deepEqual(topics(received), [
    't1/market.btc.trades',
    't1/market.eth.trades',
    't1/market.eth.trades',
  ]);
});

test('revision touches neither queue, backpressure, dedup, nor lag state', async () => {
  const bus = new EventBus();
  const { received, sub } = collect(bus, '**', {
    patterns: { patterns: ['market.**'] },
    lagMonitor: { windowSize: 4, thresholdMs: 10_000 },
  });
  bus.publish('market.btc.trades', { n: 1 });
  await flush();
  assert.deepEqual(topics(received), ['market.btc.trades']);
  const filteredBefore = bus.getStats().filteredMessages;
  const lagBefore = bus.getStats().lag[0];
  assert.equal(lagBefore.samples, 1);

  bus.setCompositePatterns(sub.id, { exclude: ['market.btc.*'] });
  // Nothing delivered, nothing redelivered, lag samples intact.
  assert.deepEqual(topics(received), ['market.btc.trades']);
  assert.equal(bus.getStats().compositeSubscriptions, 1);
  assert.equal(bus.getStats().lag[0].samples, 1);
  assert.equal(bus.getStats().lag[0].subscriberId, sub.id);
  // The filtered counter keeps counting from its pre-revision value —
  // it is never reset by the revision.
  bus.publish('market.btc.trades', { n: 2 });
  await flush();
  assert.equal(bus.getStats().filteredMessages, filteredBefore + 1);
  assert.deepEqual(topics(received), ['market.btc.trades']);
});

test('revision never moves the base pattern', async () => {
  const bus = new EventBus();
  const { received, sub } = collect(bus, 'market.**', {
    patterns: { patterns: ['market.btc.*'] },
  });
  bus.setCompositePatterns(sub.id, { patterns: ['market.eth.*'] });
  bus.publish('orders.new', { n: 1 });
  bus.publish('market.btc.trades', { n: 2 });
  bus.publish('market.eth.trades', { n: 3 });
  await flush();
  // orders.new fails the base pre-filter; btc fails the new composite.
  assert.deepEqual(topics(received), ['market.eth.trades']);
});

test('shared cache references survive a revision: no leak, no early eviction', async () => {
  const bus = new EventBus();
  const a = collect(bus, '**', { patterns: { patterns: ['market.*'] } });
  const b = collect(bus, '**', { patterns: { patterns: ['market.*'] } });
  assert.equal(bus.getStats().compositeSubscriptions, 2);

  // A moves away from the shared pattern; B must keep matching it.
  bus.setCompositePatterns(a.sub.id, { patterns: ['other.*'] });
  bus.publish('market.x', { n: 1 });
  bus.publish('other.x', { n: 2 });
  await flush();
  assert.deepEqual(topics(a.received), ['other.x']);
  assert.deepEqual(topics(b.received), ['market.x']);

  // B leaves: the last reference on market.* is released. A can move
  // back onto it — the cache recompiles instead of serving a stale
  // entry — and matching works again.
  b.sub.unsubscribe();
  assert.equal(bus.getStats().compositeSubscriptions, 1);
  bus.setCompositePatterns(a.sub.id, { patterns: ['market.*'] });
  bus.publish('market.y', { n: 3 });
  await flush();
  assert.deepEqual(topics(a.received), ['other.x', 'market.y']);
});

test('replay of the remaining records filters under the new state', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  try {
    const { received, sub } = collect(bus, '**', {
      patterns: { patterns: ['market.**'] },
    });
    bus.publish('market.btc', { n: 1 });
    bus.publish('market.eth', { n: 2 });
    await flush();
    assert.deepEqual(topics(received), ['market.btc', 'market.eth']);
    received.length = 0;

    // Tighten first, then repair: the btc record must be skipped by
    // the new state while the eth record still re-enqueues.
    bus.setCompositePatterns(sub.id, { exclude: ['market.btc'] });
    const result = bus.requestMissingReplay(sub.id, 1, 2);
    await flush();
    assert.equal(result.unavailable, false);
    assert.equal(result.replayed, 1);
    assert.deepEqual(topics(received), ['market.eth']);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
