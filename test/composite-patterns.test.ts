import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  EventBus,
  patternCovers,
  type BusMessage,
  type CompositeSubscribeOptions,
} from '../src/bus.ts';

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

test('composite: mode any (default) delivers when any include pattern matches', async () => {
  const bus = new EventBus();
  const { received } = collect(bus, '**', {
    patterns: { patterns: ['market.btc.*', 'market.eth.*'] },
  });
  bus.publish('market.btc.trades', { n: 1 });
  bus.publish('market.eth.trades', { n: 2 });
  bus.publish('market.sol.trades', { n: 3 });
  bus.publish('orders.new', { n: 4 });
  await flush();
  assert.deepEqual(topics(received), ['market.btc.trades', 'market.eth.trades']);
});

test('composite: mode all requires every include pattern to match', async () => {
  const bus = new EventBus();
  const { received } = collect(bus, '**', {
    patterns: { patterns: ['market.*', '*.btc'], mode: 'all' },
  });
  bus.publish('market.btc', { n: 1 });
  bus.publish('market.eth', { n: 2 });
  bus.publish('fx.btc', { n: 3 });
  await flush();
  assert.deepEqual(topics(received), ['market.btc']);
});

test('composite: exclude drops the noisy leg (market feed minus market.btc.*)', async () => {
  const bus = new EventBus();
  const { received } = collect(bus, '**', {
    patterns: { patterns: ['market.**'], exclude: ['market.btc.*'] },
  });
  bus.publish('market.btc.trades', { n: 1 });
  bus.publish('market.eth.trades', { n: 2 });
  bus.publish('market.btc', { n: 3 });
  bus.publish('market', { n: 4 });
  bus.publish('orders.new', { n: 5 });
  await flush();
  // market.btc.trades matches the include but is excluded; market.btc
  // (two segments) is below the exclude's depth and stays; orders.new
  // fails the include.
  assert.deepEqual(topics(received), ['market.eth.trades', 'market.btc', 'market']);
  const stats = bus.getStats();
  assert.equal(stats.filteredMessages, 2);
  assert.equal(stats.sequenceGaps, 0);
});

test('composite: exclude wins over an any-mode include match', async () => {
  const bus = new EventBus();
  const { received } = collect(bus, '**', {
    patterns: { patterns: ['**'], mode: 'any', exclude: ['internal.*'] },
  });
  bus.publish('internal.debug', { n: 1 });
  bus.publish('public.news', { n: 2 });
  await flush();
  assert.deepEqual(topics(received), ['public.news']);
});

test('composite: exclude wins in all mode too', async () => {
  const bus = new EventBus();
  const { received } = collect(bus, '**', {
    patterns: { patterns: ['market.*', '*.btc'], mode: 'all', exclude: ['market.btc'] },
  });
  bus.publish('market.btc', { n: 1 });
  await flush();
  assert.deepEqual(topics(received), []);
});

test('composite: content filter is evaluated before composite matching', async () => {
  const bus = new EventBus();
  const filterCalls: string[] = [];
  const { received } = collect(bus, '**', {
    filter: (_payload, topic) => {
      filterCalls.push(topic);
      return true;
    },
    patterns: { patterns: ['**'], exclude: ['nope.*'] },
  });
  bus.publish('nope.x', { n: 1 });
  await flush();
  // The filter ran even though the composite patterns reject the topic:
  // composite matching happens after the filter, not instead of it.
  assert.deepEqual(filterCalls, ['nope.x']);
  assert.equal(received.length, 0);
});

test('composite: a throwing filter still propagates when composite would exclude', () => {
  const bus = new EventBus();
  bus.subscribe(
    '**',
    () => {},
    {
      filter: (_payload, topic) => {
        if (topic === 'nope.x') throw new Error('filter ran first');
        return true;
      },
      patterns: { patterns: ['**'], exclude: ['nope.*'] },
    },
  );
  // The filter threw before composite matching could reject the message.
  assert.throws(() => bus.publish('nope.x', {}), /filter ran first/);
});

test('composite: filter and composite compose (both must pass)', async () => {
  const bus = new EventBus();
  const { received } = collect(bus, '**', {
    filter: (payload) => (payload as { keep: boolean }).keep === true,
    patterns: { patterns: ['market.*'], exclude: ['market.btc.*'] },
  });
  bus.publish('market.eth', { keep: true });
  bus.publish('market.eth', { keep: false });
  bus.publish('market.btc.spot', { keep: true });
  await flush();
  assert.deepEqual(
    received.map((m) => m.payload),
    [{ keep: true }],
  );
  assert.equal(bus.getStats().filteredMessages, 2);
});

test('composite: empty patterns array throws RangeError', () => {
  const bus = new EventBus();
  assert.throws(
    () => bus.subscribe('**', () => {}, { patterns: { patterns: [] } }),
    RangeError,
  );
});

test('composite: invalid mode throws RangeError', () => {
  const bus = new EventBus();
  assert.throws(
    () =>
      bus.subscribe('**', () => {}, {
        patterns: { patterns: ['a.*'], mode: 'every' as 'any' },
      }),
    RangeError,
  );
});

test('composite: non-string or empty pattern throws RangeError', () => {
  const bus = new EventBus();
  assert.throws(
    () => bus.subscribe('**', () => {}, { patterns: { patterns: [''] } }),
    RangeError,
  );
  assert.throws(
    () =>
      bus.subscribe('**', () => {}, {
        patterns: { patterns: ['a.*'], exclude: [''] },
      }),
    RangeError,
  );
});

test('composite: non-object patterns option throws TypeError', () => {
  const bus = new EventBus();
  assert.throws(
    () =>
      bus.subscribe('**', () => {}, {
        patterns: ['a.*'] as unknown as CompositeSubscribeOptions,
      }),
    TypeError,
  );
});

test('composite: any-mode exclude covering every include throws RangeError', () => {
  const bus = new EventBus();
  // market.** covers market.btc.* (each include covered by some exclude).
  assert.throws(
    () =>
      bus.subscribe('**', () => {}, {
        patterns: { patterns: ['market.btc.*'], exclude: ['market.**'] },
      }),
    RangeError,
  );
  // '**' covers everything.
  assert.throws(
    () =>
      bus.subscribe('**', () => {}, {
        patterns: { patterns: ['a.*', 'b.*'], exclude: ['**'] },
      }),
    RangeError,
  );
  // Partial coverage is fine: b.* still lets messages through.
  const { sub } = collect(bus, '**', {
    patterns: { patterns: ['a.*', 'b.*'], exclude: ['a.*'] },
  });
  assert.ok(sub);
  sub.unsubscribe();
});

test('composite: all-mode exclude covering every include throws RangeError', () => {
  const bus = new EventBus();
  // One exclude covering all includes: any message matching every
  // include would match the exclude, so nothing could be delivered.
  assert.throws(
    () =>
      bus.subscribe('**', () => {}, {
        patterns: { patterns: ['market.*', 'market.btc'], mode: 'all', exclude: ['market.**'] },
      }),
    RangeError,
  );
  // An exclude covering only one of the includes is fine.
  const { sub } = collect(bus, '**', {
    patterns: { patterns: ['market.*', '*.btc'], mode: 'all', exclude: ['fx.*'] },
  });
  assert.ok(sub);
  sub.unsubscribe();
});

test('composite: failed validation registers nothing', () => {
  const bus = new EventBus();
  assert.throws(
    () => bus.subscribe('**', () => {}, { patterns: { patterns: [] } }),
    RangeError,
  );
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(bus.getStats().compositeSubscriptions, 0);
});

test('composite: getStats exposes compositeSubscriptions; patternCacheSize untouched', () => {
  const bus = new EventBus();
  // Prime the single-pattern cache with the base pattern first, so the
  // composite subscribe below must not move patternCacheSize at all.
  const plain = bus.subscribe('**', () => {});
  const before = bus.getStats();
  assert.equal(before.compositeSubscriptions, 0);
  const cacheSizeBefore = before.patternCacheSize;
  const c1 = bus.subscribe('**', () => {}, {
    patterns: { patterns: ['zz-unique-a.*', 'zz-unique-b.**'], exclude: ['zz-unique-a.nope.*'] },
  });
  const c2 = bus.subscribe('**', () => {}, {
    patterns: { patterns: ['zz-unique-a.*'], mode: 'all' },
  });
  const mid = bus.getStats();
  assert.equal(mid.compositeSubscriptions, 2);
  // The composite patterns live in the dedicated composite cache: the
  // single-pattern cache size is unchanged.
  assert.equal(mid.patternCacheSize, cacheSizeBefore);
  c1.unsubscribe();
  assert.equal(bus.getStats().compositeSubscriptions, 1);
  c2.unsubscribe();
  plain.unsubscribe();
  assert.equal(bus.getStats().compositeSubscriptions, 0);
});

test('composite: shared composite patterns reuse one compiled RegExp', async () => {
  const bus = new EventBus();
  const r1 = collect(bus, '**', { patterns: { patterns: ['shared.*'] } });
  const r2 = collect(bus, '**', { patterns: { patterns: ['shared.*'] } });
  bus.publish('shared.x', { n: 1 });
  await flush();
  assert.equal(r1.received.length, 1);
  assert.equal(r2.received.length, 1);
  r1.sub.unsubscribe();
  // The second subscriber still matches after the first leaves: the
  // shared cache entry is reference-counted, not dropped early.
  bus.publish('shared.y', { n: 2 });
  await flush();
  assert.equal(r2.received.length, 2);
  r2.sub.unsubscribe();
});

test('composite: interop with plain single-pattern subscriptions', async () => {
  const bus = new EventBus();
  const plain = collect(bus, 'market.btc.*');
  const composite = collect(bus, '**', {
    patterns: { patterns: ['market.**'], exclude: ['market.btc.*'] },
  });
  const accepted = bus.publish('market.btc.trades', { n: 1 });
  bus.publish('market.eth', { n: 2 });
  await flush();
  // The plain subscriber is unaffected by the composite subscriber's
  // exclusions, and publish() counts only the accepting subscriber.
  assert.deepEqual(topics(plain.received), ['market.btc.trades']);
  assert.deepEqual(topics(composite.received), ['market.eth']);
  assert.equal(accepted, 1);
  plain.sub.unsubscribe();
  composite.sub.unsubscribe();
});

test('composite: applies to durable-log replay', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'eb-composite-'));
  const bus = new EventBus({ durableLogDir: dir });
  bus.publish('market.btc.trades', { n: 1 });
  bus.publish('market.eth.trades', { n: 2 });
  bus.publish('market.btc', { n: 3 });
  await flush();
  const bus2 = new EventBus({ durableLogDir: dir });
  const { received } = collect(bus2, '**', {
    resumeFromSeq: 0,
    patterns: { patterns: ['market.**'], exclude: ['market.btc.*'] },
  });
  await flush();
  // Cross-topic replay order follows (at, seq, topic) and is not
  // publish-ordered here, so compare as a set.
  assert.deepEqual(
    received.map((m) => (m.payload as { n: number }).n).sort(),
    [2, 3],
  );
  assert.equal(bus2.getStats().filteredMessages, 1);
  assert.equal(bus2.getStats().sequenceGaps, 0);
});

test('patternCovers: include/exclude coverage decisions', () => {
  // cover, sub, expected
  const cases: Array<[string, string, boolean]> = [
    ['**', 'a.b.c', true],
    ['*', 'a.b.c', true],
    ['market.**', 'market.btc.*', true],
    ['market.*', 'market.btc.*', false],
    ['a.*', 'a.**', false],
    ['a.**', 'a.*', true],
    ['*.b', 'a.b', true],
    ['a.b', '*.b', false],
    ['**', '**', true],
    ['a', 'a', true],
    ['a', 'b', false],
    ['a.*.c', 'a.b.c', true],
    ['a.b.c', 'a.*.c', false],
    ['**.b', 'b', true],
    ['a.**', 'a', true],
    ['*.*', 'a.b', true],
    ['a.**.b', 'a.x.y.b', true],
    ['a.*.b', 'a.x.y.b', false],
  ];
  for (const [cover, sub, expected] of cases) {
    assert.equal(patternCovers(cover, sub), expected, `patternCovers(${cover}, ${sub})`);
  }
});
