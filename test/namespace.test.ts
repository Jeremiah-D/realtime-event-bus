import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  EventBus,
  compilePattern,
  AclDeniedError,
  type BusMessage,
} from '../src/bus.ts';
import {
  NamespaceDeniedError,
  NamespaceNotEmptyError,
} from '../src/namespace.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-ns-'));

test('compilePattern: "/" is a literal segment character', () => {
  // Segments split on `.` only, so `t1/**` is ONE literal segment: it
  // matches nothing but the exact topic `t1/**` — in particular it does
  // not match `t10/x`, and it cannot reach `t1/orders`.
  assert.equal(compilePattern('t1/**').test('t10/x'), false);
  assert.equal(compilePattern('t1/**').test('t1/orders'), false);
  assert.equal(compilePattern('t1/**').test('t1/**'), true);
  // The dot form `t1.**` is the hierarchical wildcard: it matches `t1`
  // and `t1.a`, still not `t10/x` (anchored), and not `t1/orders` (`/`
  // is literal inside the segment).
  assert.equal(compilePattern('t1.**').test('t1'), true);
  assert.equal(compilePattern('t1.**').test('t1.a'), true);
  assert.equal(compilePattern('t1.**').test('t10/x'), false);
  assert.equal(compilePattern('t1.**').test('t1/orders'), false);
  assert.equal(compilePattern('t1/orders').test('t1/orders'), true);
});

test('createNamespace: prefix validation', () => {
  const bus = new EventBus();
  assert.throws(() => bus.createNamespace(''), RangeError);
  assert.throws(() => bus.createNamespace('   '), RangeError);
  assert.throws(() => bus.createNamespace('t1/x'), RangeError);
  assert.throws(() => bus.createNamespace('t*'), RangeError);
  assert.throws(() => bus.createNamespace('**'), RangeError);
  bus.createNamespace('t1');
  assert.throws(() => bus.createNamespace('t1'), RangeError);
  assert.throws(
    () => bus.createNamespace('t2', { allowPublish: 'yes' as unknown as boolean }),
    RangeError,
  );
});

test('namespace isolates tenants: auto-prefix publish/subscribe', async () => {
  const bus = new EventBus();
  const t1 = bus.createNamespace('t1');
  const t2 = bus.createNamespace('t2');
  const got1: BusMessage[] = [];
  const got2: BusMessage[] = [];
  t1.subscribe('orders', (msg) => got1.push(msg));
  t2.subscribe('orders', (msg) => got2.push(msg));

  t1.publish('orders', { id: 1 });
  await flush();
  assert.equal(got1.length, 1);
  assert.equal(got2.length, 0);
  // Deliveries name the concrete topic.
  assert.equal(got1[0].topic, 't1/orders');
  assert.deepEqual(got1[0].payload, { id: 1 });

  t2.publish('orders', { id: 2 });
  await flush();
  assert.equal(got1.length, 1);
  assert.equal(got2.length, 1);
  assert.equal(got2[0].topic, 't2/orders');
});

test('namespace wildcard "**" cannot cross into another namespace', async () => {
  const bus = new EventBus();
  const t1 = bus.createNamespace('t1');
  const t2 = bus.createNamespace('t2');
  const got1: string[] = [];
  const got2: string[] = [];
  t1.subscribe('**', (msg) => got1.push(msg.topic));
  t2.subscribe('**', (msg) => got2.push(msg.topic));

  t1.publish('orders.created', { id: 1 });
  t2.publish('payments.settled', { id: 2 });
  await flush();
  assert.deepEqual(got1, ['t1/orders.created']);
  assert.deepEqual(got2, ['t2/payments.settled']);
});

test('namespaced bare "*" and "**" match everything in the namespace only', async () => {
  const bus = new EventBus();
  const t1 = bus.createNamespace('t1');
  const t2 = bus.createNamespace('t2');
  const star: string[] = [];
  const dstars: string[] = [];
  t1.subscribe('*', (msg) => star.push(msg.topic));
  t2.subscribe('**', (msg) => dstars.push(msg.topic));
  t1.publish('orders', 1);
  t1.publish('orders.created', 2);
  t2.publish('orders', 3);
  await flush();
  // A bare `*`/`**` matches every topic globally — scoped to a
  // namespace, it matches every topic in that namespace, and nothing
  // outside it.
  assert.deepEqual(star, ['t1/orders', 't1/orders.created']);
  assert.deepEqual(dstars, ['t2/orders']);
});

test('global bus is the admin view: sees namespaced topics', async () => {
  const bus = new EventBus();
  const t1 = bus.createNamespace('t1');
  const got: string[] = [];
  bus.subscribe('**', (msg) => got.push(msg.topic));
  t1.publish('orders', 1);
  await flush();
  assert.deepEqual(got, ['t1/orders']);
});

test('cross-namespace topic/pattern is denied', () => {
  const bus = new EventBus();
  bus.createNamespace('t1');
  bus.createNamespace('t2');
  assert.throws(
    () => bus.publish('t2/x', {}, { namespace: 't1' }),
    (err) => {
      assert.ok(err instanceof NamespaceDeniedError);
      assert.equal(err.action, 'publish');
      assert.equal(err.namespace, 't1');
      assert.equal(err.topic, 't2/x');
      return true;
    },
  );
  assert.throws(
    () => bus.subscribe('t2/x', () => {}, { namespace: 't1' }),
    (err) => {
      assert.ok(err instanceof NamespaceDeniedError);
      assert.equal(err.action, 'subscribe');
      assert.equal(err.pattern, 't2/x');
      return true;
    },
  );
  // Any `/` in a namespaced topic is an escape attempt, even without a
  // registered prefix behind it.
  assert.throws(() => bus.publish('a/b', {}, { namespace: 't1' }), NamespaceDeniedError);
});

test('unknown namespace throws RangeError', () => {
  const bus = new EventBus();
  assert.throws(() => bus.publish('orders', {}, { namespace: 'nope' }), RangeError);
  assert.throws(() => bus.subscribe('orders', () => {}, { namespace: 'nope' }), RangeError);
});

test('allowPublish/allowSubscribe flags are enforced', async () => {
  const bus = new EventBus();
  const ro = bus.createNamespace('ro', { allowPublish: false });
  const wo = bus.createNamespace('wo', { allowSubscribe: false });

  assert.throws(() => ro.publish('orders', {}), NamespaceDeniedError);
  const got: unknown[] = [];
  ro.subscribe('orders', (msg) => got.push(msg.payload));
  // The global (admin) publish still lands in the namespace's topic.
  bus.publish('ro/orders', { id: 9 });
  await flush();
  assert.deepEqual(got, [{ id: 9 }]);

  assert.throws(() => wo.subscribe('orders', () => {}), NamespaceDeniedError);
  const sub = bus.subscribe('wo/orders', () => {});
  wo.publish('orders', { id: 1 });
  sub.unsubscribe();
});

test('getNamespaces returns a snapshot with flags and subscriber counts', () => {
  const bus = new EventBus();
  const t1 = bus.createNamespace('t1', { allowPublish: false });
  bus.createNamespace('t2');
  assert.deepEqual(bus.getNamespaces(), [
    { prefix: 't1', allowPublish: false, allowSubscribe: true, subscribers: 0 },
    { prefix: 't2', allowPublish: true, allowSubscribe: true, subscribers: 0 },
  ]);
  const s1 = t1.subscribe('a', () => {});
  const s2 = t1.subscribe('b', () => {});
  assert.equal(bus.getNamespaces()[0].subscribers, 2);
  s1.unsubscribe();
  assert.equal(bus.getNamespaces()[0].subscribers, 1);
  s2.unsubscribe();
  assert.equal(bus.getNamespaces()[0].subscribers, 0);
});

test('deleteNamespace only when empty', () => {
  const bus = new EventBus();
  const t1 = bus.createNamespace('t1');
  assert.throws(() => bus.deleteNamespace('unknown'), RangeError);
  const sub = t1.subscribe('orders', () => {});
  assert.throws(
    () => bus.deleteNamespace('t1'),
    (err) => {
      assert.ok(err instanceof NamespaceNotEmptyError);
      assert.equal(err.namespace, 't1');
      assert.equal(err.reason, 'subscribers');
      return true;
    },
  );
  sub.unsubscribe();
  bus.deleteNamespace('t1');
  assert.deepEqual(bus.getNamespaces(), []);
  // Re-registration works after deletion.
  bus.createNamespace('t1');
  assert.equal(bus.getNamespaces().length, 1);
});

test('per-namespace stats aggregate from per-topic TopicStats', async () => {
  const bus = new EventBus();
  const t1 = bus.createNamespace('t1');
  const t2 = bus.createNamespace('t2');
  t1.subscribe('**', () => {});
  t1.subscribe('orders', () => {});
  t2.subscribe('**', () => {});
  t1.publish('orders', 1);
  t1.publish('orders', 2);
  t1.publish('refunds', 3);
  t2.publish('orders', 4);
  bus.publish('global.topic', 5);
  await flush();

  const stats = bus.getStats();
  const byNs = Object.fromEntries((stats.namespaces ?? []).map((n) => [n.namespace, n]));
  assert.equal(byNs['t1'].publishedMessages, 3);
  assert.equal(byNs['t1'].topics, 2);
  assert.equal(byNs['t1'].subscribers, 2);
  assert.equal(byNs['t2'].publishedMessages, 1);
  assert.equal(byNs['t2'].topics, 1);
  assert.equal(byNs['t2'].subscribers, 1);
  // Global topics are untouched by the aggregation.
  assert.ok(stats.topics.some((t) => t.topic === 'global.topic'));
});

test('metrics expose per-namespace series; absent without namespaces', () => {
  const plain = new EventBus();
  plain.publish('a.b', 1);
  const plainOut = renderPrometheus(plain.getStats());
  assert.ok(!plainOut.includes('eventbus_namespace_'));
  assert.equal(plain.getStats().namespaces, undefined);

  const bus = new EventBus();
  const t1 = bus.createNamespace('payments');
  t1.subscribe('**', () => {});
  t1.publish('orders', 1);
  t1.publish('orders', 2);
  const out = renderPrometheus(bus.getStats());
  assert.ok(out.includes('eventbus_namespace_published_messages_total{namespace="payments"} 2'));
  assert.ok(out.includes('eventbus_namespace_subscribers{namespace="payments"} 1'));
});

test('durable log: namespaced publishes land in the child log, replay reads it', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  const t1 = bus.createNamespace('t1');

  t1.publish('orders', { id: 1 });
  t1.publish('orders', { id: 2 });
  bus.publish('global.topic', { id: 3 });
  await flush();

  // Child log directory exists and holds the namespaced records.
  const childDir = join(dir, 'namespaces', encodeURIComponent('t1'));
  assert.ok(existsSync(childDir));
  const { DurableTopicLog } = await import('../src/durablelog.ts');
  const child = DurableTopicLog.open({ dir: childDir });
  assert.deepEqual(child.topics().sort(), ['t1/orders']);
  assert.equal(child.messageCount('t1/orders'), 2);
  // The root log never saw the namespaced topics.
  const root = DurableTopicLog.open({ dir });
  assert.deepEqual(root.topics().sort(), ['global.topic']);

  // Replay through the handle reads the child log.
  const replayed: Array<{ seq: number; payload: unknown }> = [];
  t1.subscribe('orders', (msg) => replayed.push({ seq: msg.seq, payload: msg.payload }), {
    resumeFromSeq: 0,
  });
  await flush();
  assert.deepEqual(
    replayed.map((r) => r.payload),
    [{ id: 1 }, { id: 2 }],
  );
  assert.deepEqual(
    replayed.map((r) => r.seq),
    [1, 2],
  );
});

test('deleteNamespace refuses while durable entries are retained', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  const t1 = bus.createNamespace('t1');
  t1.publish('orders', { id: 1 });
  assert.throws(
    () => bus.deleteNamespace('t1'),
    (err) => {
      assert.ok(err instanceof NamespaceNotEmptyError);
      assert.equal(err.reason, 'durable-entries');
      return true;
    },
  );
});

test('EB-15 prefix index: "/" in topics does not break candidate lookup', async () => {
  const bus = new EventBus();
  const t1 = bus.createNamespace('t1');
  // Exact-pattern subscriber files under the `t1/orders` index key and
  // must be found when publishing the concrete topic.
  const exact: unknown[] = [];
  bus.subscribe('t1/orders', (msg) => exact.push(msg.payload));
  t1.publish('orders', { id: 1 });
  await flush();
  assert.deepEqual(exact, [{ id: 1 }]);

  // A global wildcard subscriber (filed under the empty key) still sees
  // namespaced publishes — the admin view.
  const wild: string[] = [];
  bus.subscribe('*', (msg) => wild.push(msg.topic));
  t1.publish('refunds', { id: 2 });
  await flush();
  assert.deepEqual(wild, ['t1/refunds']);

  // Documented dot-segment semantics: `t1/*` does NOT match `t1/orders`
  // (segments split on `.`; `/` is literal inside a segment).
  const slashStar: unknown[] = [];
  bus.subscribe('t1/*', (msg) => slashStar.push(msg.payload));
  t1.publish('orders', { id: 3 });
  await flush();
  assert.deepEqual(slashStar, []);
  assert.deepEqual(exact, [{ id: 1 }, { id: 3 }]);
});

test('namespace publish/subscribe run the full admission pipeline on concrete topics', async () => {
  const bus = new EventBus({
    acl: {
      defaultPolicy: 'deny',
      rules: [{ pattern: 't1/orders', publish: 'allow', subscribe: 'allow' }],
    },
  });
  const t1 = bus.createNamespace('t1');
  const t2 = bus.createNamespace('t2');
  const got: unknown[] = [];
  // Subscribe ACL is evaluated on the internal `t1/orders` pattern, which
  // overlaps the rule → allowed.
  t1.subscribe('orders', (msg) => got.push(msg.payload));
  // Publish ACL is evaluated on the concrete `t1/orders` topic → allowed.
  assert.equal(t1.publish('orders', { id: 1 }), 1);
  await flush();
  assert.deepEqual(got, [{ id: 1 }]);
  // The other namespace's concrete topic matches no allow rule: the
  // publish is denied (0 accepted, no throw — the publish-path
  // convention) and the subscribe is rejected.
  assert.equal(t2.publish('orders', { id: 2 }), 0);
  assert.throws(
    () => t2.subscribe('orders', () => {}),
    (err) => {
      assert.ok(err instanceof AclDeniedError);
      return true;
    },
  );
  await flush();
  assert.deepEqual(got, [{ id: 1 }]);
});
