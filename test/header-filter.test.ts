import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type BusMessage, type HeaderFilterRule } from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-header-filter-'));

const collect = (
  bus: EventBus,
  pattern: string,
  opts?: Parameters<EventBus['subscribe']>[2],
) => {
  const received: BusMessage[] = [];
  const sub = bus.subscribe(pattern, (msg) => received.push(msg), opts);
  return { received, sub };
};

test('header filter: eq op routes on header value; headers ride the envelope', async () => {
  const bus = new EventBus();
  const { received } = collect(bus, 'orders.**', {
    headerFilter: [{ key: 'tenant', op: 'eq', value: 'acme' }],
  });
  bus.publish('orders.created', { id: 1 }, { headers: { tenant: 'acme' } });
  bus.publish('orders.created', { id: 2 }, { headers: { tenant: 'other' } });
  bus.publish('orders.created', { id: 3 });
  await flush();
  // Only the tenant=acme message is delivered; the envelope carries the
  // headers so handlers (and downstream filters) can see them.
  assert.deepEqual(received.map((m) => (m.payload as { id: number }).id), [1]);
  assert.deepEqual(received[0].headers, { tenant: 'acme' });
  const stats = bus.getStats();
  assert.equal(stats.headerFilteredMessages, 2);
  assert.equal(stats.topics.find((t) => t.topic === 'orders.created')?.headerFilteredMessages, 2);
  // The EB-25 content-filter counter is untouched.
  assert.equal(stats.filteredMessages, 0);
});

test('header filter: ne / prefix / regex ops', async () => {
  const bus = new EventBus();
  const ne = collect(bus, 'a', { headerFilter: [{ key: 'env', op: 'ne', value: 'prod' }] });
  const prefix = collect(bus, 'b', { headerFilter: [{ key: 'region', op: 'prefix', value: 'us-' }] });
  const regex = collect(bus, 'c', { headerFilter: [{ key: 'version', op: 'regex', value: '^v2\\.' }] });
  bus.publish('a', 1, { headers: { env: 'staging' } });
  bus.publish('a', 2, { headers: { env: 'prod' } });
  bus.publish('b', 1, { headers: { region: 'us-west' } });
  bus.publish('b', 2, { headers: { region: 'eu-west' } });
  bus.publish('c', 1, { headers: { version: 'v2.3' } });
  bus.publish('c', 2, { headers: { version: 'v10.2' } });
  await flush();
  assert.deepEqual(ne.received.map((m) => m.payload), [1]);
  assert.deepEqual(prefix.received.map((m) => m.payload), [1]);
  assert.deepEqual(regex.received.map((m) => m.payload), [1]);
  assert.equal(bus.getStats().headerFilteredMessages, 3);
});

test('header filter: rules compose with AND; one failure skips', async () => {
  const bus = new EventBus();
  const { received } = collect(bus, 't', {
    headerFilter: [
      { key: 'tenant', op: 'eq', value: 'acme' },
      { key: 'env', op: 'ne', value: 'prod' },
    ],
  });
  bus.publish('t', 'both', { headers: { tenant: 'acme', env: 'staging' } });
  bus.publish('t', 'second-fails', { headers: { tenant: 'acme', env: 'prod' } });
  bus.publish('t', 'first-fails', { headers: { tenant: 'other', env: 'staging' } });
  await flush();
  assert.deepEqual(received.map((m) => m.payload), ['both']);
  assert.equal(bus.getStats().headerFilteredMessages, 2);
});

test('header filter: missing header never matches (fail-closed, even ne)', async () => {
  const bus = new EventBus();
  const { received } = collect(bus, 't', {
    headerFilter: [{ key: 'tenant', op: 'ne', value: 'acme' }],
  });
  // A message with no headers at all and one missing just this key are
  // both skipped: absence is not a value, so even `ne` fails closed.
  bus.publish('t', 'no-headers');
  bus.publish('t', 'other-header', { headers: { region: 'us' } });
  bus.publish('t', 'present-diff', { headers: { tenant: 'globex' } });
  await flush();
  assert.deepEqual(received.map((m) => m.payload), ['present-diff']);
  assert.equal(bus.getStats().headerFilteredMessages, 2);
});

test('header filter: skipped messages consume no backpressure budget', async () => {
  const bus = new EventBus();
  // Tiny queue with drop-oldest: if filtered messages ever entered it,
  // accepted messages would be churned out and droppedCount would move.
  const { received, sub } = collect(bus, 't', {
    queueSize: 2,
    headerFilter: [{ key: 'keep', op: 'eq', value: 'yes' }],
  });
  for (let i = 0; i < 10; i++) bus.publish('t', { keep: false, i }, { headers: { keep: 'no' } });
  bus.publish('t', { keep: true, i: 'a' }, { headers: { keep: 'yes' } });
  bus.publish('t', { keep: true, i: 'b' }, { headers: { keep: 'yes' } });
  await flush();
  assert.equal(bus.droppedCount(sub.id), 0);
  assert.deepEqual(
    received.map((m) => (m.payload as { i: unknown }).i),
    ['a', 'b'],
  );
  assert.equal(bus.getStats().headerFilteredMessages, 10);
});

test('header filter: skipped messages never count as sequence gaps', async () => {
  const bus = new EventBus();
  const { received } = collect(bus, 't', {
    headerFilter: [{ key: 'tenant', op: 'eq', value: 'acme' }],
  });
  bus.publish('t', 1, { headers: { tenant: 'acme' } });
  bus.publish('t', 2, { headers: { tenant: 'other' } }); // skipped, not a gap
  bus.publish('t', 3, { headers: { tenant: 'acme' } });
  await flush();
  assert.deepEqual(received.map((m) => m.payload), [1, 3]);
  const topic = bus.getStats().topics.find((t) => t.topic === 't');
  assert.equal(topic?.sequenceGaps, 0);
  assert.equal(topic?.headerFilteredMessages, 1);
});

test('header filter: hot update takes effect immediately and is audited', async () => {
  const bus = new EventBus();
  const { received, sub } = collect(bus, 't');
  bus.publish('t', 1, { headers: { tenant: 'acme' } });
  await flush();
  assert.deepEqual(received.map((m) => m.payload), [1]);

  assert.equal(bus.setHeaderFilter(sub.id, [{ key: 'tenant', op: 'eq', value: 'acme' }]), true);
  bus.publish('t', 2, { headers: { tenant: 'acme' } });
  bus.publish('t', 3, { headers: { tenant: 'other' } });
  await flush();
  assert.deepEqual(received.map((m) => m.payload), [1, 2]);
  assert.equal(bus.getStats().headerFilteredMessages, 1);

  const events = bus.getSubscriptionAudit({ action: 'header_filter_updated' });
  assert.equal(events.length, 1);
  assert.equal(events[0].subId, sub.id);
  assert.equal(events[0].pattern, 't');
  assert.deepEqual(events[0].detail, { rules: 1 });

  // Clearing the filter restores keep-everything.
  assert.equal(bus.setHeaderFilter(sub.id, undefined), true);
  bus.publish('t', 4, { headers: { tenant: 'other' } });
  await flush();
  assert.deepEqual(received.map((m) => m.payload), [1, 2, 4]);
  assert.deepEqual(bus.getSubscriptionAudit({ action: 'header_filter_updated' })[1].detail, {
    rules: 0,
  });
});

test('setHeaderFilter: unknown id returns false, illegal rules throw', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {});
  assert.equal(bus.setHeaderFilter('no-such-sub', [{ key: 'a', op: 'eq', value: 'b' }]), false);
  const bad: Array<{ rules: HeaderFilterRule[]; error: new (...args: never[]) => Error }> = [
    { rules: [{ key: '', op: 'eq', value: 'b' }], error: RangeError },
    { rules: [{ key: 'a', op: 'bogus' as never, value: 'b' }], error: RangeError },
    { rules: [{ key: 'a', op: 'regex', value: '([' }], error: RangeError },
  ];
  for (const { rules, error } of bad) {
    assert.throws(() => bus.setHeaderFilter(sub.id, rules), error);
    // Validation runs before the id lookup: unknown ids throw too.
    assert.throws(() => bus.setHeaderFilter('no-such-sub', rules), error);
  }
  assert.equal(bus.getSubscriptionAudit({ action: 'header_filter_updated' }).length, 0);
});

test('subscribe: invalid headerFilter rules throw before anything registers', () => {
  const bus = new EventBus();
  // Illegal regex is precompiled at subscribe time.
  assert.throws(
    () => bus.subscribe('t', () => {}, { headerFilter: [{ key: 'a', op: 'regex', value: '(unclosed' }] }),
    RangeError,
  );
  assert.throws(
    () => bus.subscribe('t', () => {}, { headerFilter: 'nope' as never }),
    TypeError,
  );
  assert.throws(
    () => bus.subscribe('t', () => {}, { headerFilter: [{ key: '', op: 'eq', value: 'x' }] }),
    RangeError,
  );
  assert.throws(
    () =>
      bus.subscribe('t', () => {}, {
        headerFilter: [{ key: 'a', op: 'eq', value: 42 as never }],
      }),
    TypeError,
  );
  // Nothing half-registered: no subscribers survived the throws.
  assert.equal(bus.getStats().totalSubscribers, 0);
});

test('publish: non-string header values throw RangeError with zero side effects', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  assert.throws(
    () => bus.publish('t', {}, { headers: { tenant: 42 as never } }),
    RangeError,
  );
  assert.throws(() => bus.publish('t', {}, { headers: null as never }), RangeError);
  assert.throws(() => bus.publish('t', {}, { headers: ['x'] as never }), RangeError);
  const stats = bus.getStats();
  assert.equal(stats.topics.find((t) => t.topic === 't')?.publishedMessages ?? 0, 0);
});

test('header filter: applies to durable-log replay', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  bus.subscribe('t', () => {});
  bus.publish('t', 'acme-1', { headers: { tenant: 'acme' } });
  bus.publish('t', 'other-1', { headers: { tenant: 'other' } });
  bus.publish('t', 'acme-2', { headers: { tenant: 'acme' } });
  await flush();
  // A reconnecting subscriber replays from the log: the header filter
  // judges each record on its persisted headers, so the replayed stream
  // matches the live filtered stream.
  const replayed: unknown[] = [];
  bus.subscribe('t', (m) => replayed.push(m.payload), {
    resumeFromSeq: 0,
    headerFilter: [{ key: 'tenant', op: 'eq', value: 'acme' }],
  });
  await flush();
  await flush();
  assert.deepEqual(replayed, ['acme-1', 'acme-2']);
  // The replay skip is counted too.
  assert.equal(bus.getStats().headerFilteredMessages, 1);
});

test('header filter: composes with the EB-25 content filter (both must accept)', async () => {
  const bus = new EventBus();
  const { received } = collect(bus, 't', {
    filter: (payload) => (payload as { keep: boolean }).keep,
    headerFilter: [{ key: 'tenant', op: 'eq', value: 'acme' }],
  });
  bus.publish('t', { keep: true, id: 'both' }, { headers: { tenant: 'acme' } });
  bus.publish('t', { keep: true, id: 'header-fails' }, { headers: { tenant: 'other' } });
  bus.publish('t', { keep: false, id: 'content-fails' }, { headers: { tenant: 'acme' } });
  await flush();
  assert.deepEqual(
    received.map((m) => (m.payload as { id: string }).id),
    ['both'],
  );
  const stats = bus.getStats();
  assert.equal(stats.headerFilteredMessages, 1);
  assert.equal(stats.filteredMessages, 1);
});

test('header filter: orthogonal to EB-144 projection (routing vs payload shape)', async () => {
  const bus = new EventBus();
  const { received } = collect(bus, 't', {
    headerFilter: [{ key: 'tenant', op: 'eq', value: 'acme' }],
    project: (payload) => ({ ...(payload as object), masked: true }),
  });
  bus.publish('t', { secret: 's3', id: 1 }, { headers: { tenant: 'acme' } });
  bus.publish('t', { secret: 's4', id: 2 }, { headers: { tenant: 'other' } });
  await flush();
  // The header filter admits the message (routing); the projection then
  // reshapes its payload — the two compose without coupling.
  assert.deepEqual(received.map((m) => m.payload), [{ secret: 's3', id: 1, masked: true }]);
  assert.equal(bus.getStats().headerFilteredMessages, 1);
  assert.equal(bus.getStats().projectedMessages, 1);
});

test('header filter: group members evaluate their own filter on the assigned copy', async () => {
  const bus = new EventBus();
  const seenA: number[] = [];
  const seenB: number[] = [];
  // Join order is A, B: round-robin assigns seq 1,3,5 → A and 2,4,6 → B.
  bus.subscribeToGroup('workers', 't', (m) => seenA.push(m.seq), {
    headerFilter: [{ key: 'tenant', op: 'eq', value: 'acme' }],
  });
  bus.subscribeToGroup('workers', 't', (m) => seenB.push(m.seq), {
    headerFilter: [{ key: 'tenant', op: 'eq', value: 'other' }],
  });
  bus.publish('t', 1, { headers: { tenant: 'acme' } }); // → A, matches
  bus.publish('t', 2, { headers: { tenant: 'other' } }); // → B, matches
  bus.publish('t', 3, { headers: { tenant: 'acme' } }); // → A, matches
  bus.publish('t', 4, { headers: { tenant: 'xyz' } }); // → B, rejected by B's filter
  await flush();
  assert.deepEqual(seenA, [1, 3]);
  assert.deepEqual(seenB, [2]);
  // The assigned member's skip is counted; the group's one copy is dropped.
  assert.equal(bus.getStats().headerFilteredMessages, 1);
});

test('metrics: header-filtered counters render in Prometheus exposition', async () => {
  const quiet = new EventBus();
  quiet.subscribe('t', () => {});
  quiet.publish('t', 1);
  const quietOut = renderPrometheus(quiet.getStats());
  // A bus without header filters renders no per-topic header-filter
  // series (the global counter always renders, like every other
  // global counter).
  assert.ok(!quietOut.includes('eventbus_header_filtered_messages_total{topic='));

  const bus = new EventBus();
  bus.subscribe('t', () => {}, { headerFilter: [{ key: 'k', op: 'eq', value: 'v' }] });
  bus.publish('t', 1, { headers: { k: 'v' } });
  bus.publish('t', 2, { headers: { k: 'other' } });
  await flush();
  const out = renderPrometheus(bus.getStats());
  assert.ok(out.includes('# HELP eventbus_header_filtered_messages_total'));
  assert.ok(out.includes('# TYPE eventbus_header_filtered_messages_total counter'));
  assert.ok(out.includes('eventbus_header_filtered_messages_total 1'));
  assert.ok(out.includes('eventbus_header_filtered_messages_total{topic="t"} 1'));
  // No confusion with the EB-25 content-filter series.
  assert.ok(!out.includes('eventbus_filtered_messages_total{'));
});

test('header filter: retained snapshot is filtered on the entry headers', async () => {
  const bus = new EventBus();
  bus.setTopicRetained('t', true);
  // Only the latest retained value per topic is snapshotted: the last
  // publish on 't' wins the table.
  bus.publish('t', 'other', { headers: { tenant: 'other' }, retain: true });
  bus.publish('t', 'acme', { headers: { tenant: 'acme' }, retain: true });
  await flush();
  const { received } = collect(bus, 't', {
    headerFilter: [{ key: 'tenant', op: 'eq', value: 'acme' }],
  });
  await flush();
  // Only the matching retained message is snapshotted to the new
  // subscriber.
  assert.deepEqual(received.map((m) => m.payload), ['acme']);
  assert.equal(bus.getStats().headerFilteredMessages, 0);

  // And a subscriber whose filter rejects the retained value gets
  // nothing (the snapshot copy is header-filtered too).
  const rejected = collect(bus, 't', {
    headerFilter: [{ key: 'tenant', op: 'eq', value: 'other' }],
  });
  await flush();
  assert.deepEqual(rejected.received, []);
  assert.equal(bus.getStats().headerFilteredMessages, 1);
});
