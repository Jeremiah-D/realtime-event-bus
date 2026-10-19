/**
 * Namespace tenant quotas (EB-162): per-(namespace, topic-pattern)
 * publish budgets over a sliding window. Hard limits shed in the
 * admission phase (`onAdmissionRejected` reason `'tenant-quota'`,
 * no sequence number, no durable-log write, no rate-limit budget);
 * soft thresholds fire one latched `tenant_quota_warning` audit event
 * per excursion.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type AdmissionRejectionEvent } from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';

const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

function busWithRejections() {
  const rejected: AdmissionRejectionEvent[] = [];
  const bus = new EventBus({ onAdmissionRejected: (e) => rejected.push(e) });
  return { bus, rejected };
}

/**
 * `publish` returns deliveries, not admission — every test asserting on
 * the return value needs a live subscriber. A namespace-local `'**'`
 * sink never interferes with quota accounting.
 */
function sink(target: { subscribe(pattern: string, handler: (msg: never) => void): unknown }): void {
  target.subscribe('**', () => {});
}

test('hard message limit sheds with reason tenant-quota and counts', async () => {
  const { bus, rejected } = busWithRejections();
  const ns = bus.createNamespace('acme');
  const seen: number[] = [];
  ns.subscribe('orders.**', (msg) => seen.push(msg.seq));
  bus.setNamespaceQuota('acme', 'acme/orders.**', { messageLimit: 2 });

  assert.ok(ns.publish('orders.new', { i: 1 }) > 0);
  assert.ok(ns.publish('orders.new', { i: 2 }) > 0);
  await flush();
  // Third publish sheds: 0 deliveries, hook fires with the new reason.
  assert.equal(ns.publish('orders.new', { i: 3 }), 0);
  await flush();
  assert.deepEqual(seen, [1, 2]);
  assert.equal(rejected.length, 1);
  assert.equal(rejected[0].topic, 'acme/orders.new');
  assert.equal(rejected[0].reason, 'tenant-quota');

  const stats = bus.getStats();
  assert.equal(stats.tenantQuotaShedMessages, 1);
  assert.equal(stats.namespaceQuotas.length, 1);
  const q = stats.namespaceQuotas[0];
  assert.equal(q.namespace, 'acme');
  assert.equal(q.pattern, 'acme/orders.**');
  assert.equal(q.usedMessages, 2);
  assert.equal(q.messageLimit, 2);
  assert.equal(q.sheds, 1);
});

test('shed consumes no sequence number', async () => {
  const { bus } = busWithRejections();
  const ns = bus.createNamespace('acme');
  const seen: number[] = [];
  ns.subscribe('orders.**', (msg) => seen.push(msg.seq));
  bus.setNamespaceQuota('acme', 'acme/orders.**', { messageLimit: 2 });

  ns.publish('orders.new', { i: 1 });
  ns.publish('orders.new', { i: 2 });
  ns.publish('orders.new', { i: 3 }); // shed
  await flush();
  assert.deepEqual(seen, [1, 2]);
  // Clearing the quota lets the next publish through with the next
  // sequence number — the shed never drew one.
  assert.ok(bus.clearNamespaceQuota('acme', 'acme/orders.**'));
  ns.publish('orders.new', { i: 4 });
  await flush();
  assert.deepEqual(seen, [1, 2, 3]);
});

test('shed burns no per-topic rate-limit budget', async () => {
  const { bus, rejected } = busWithRejections();
  const ns = bus.createNamespace('acme');
  sink(ns);
  bus.setTopicRateLimit('acme/orders.**', 1, { burst: 2 });
  bus.setNamespaceQuota('acme', 'acme/orders.**', { messageLimit: 1 });

  assert.ok(ns.publish('orders.new', { i: 1 }) > 0); // quota 1/1, rate 1/2
  assert.equal(ns.publish('orders.new', { i: 2 }), 0); // quota shed — rate untouched
  assert.equal(rejected[rejected.length - 1].reason, 'tenant-quota');
  assert.ok(bus.clearNamespaceQuota('acme', 'acme/orders.**'));
  assert.ok(ns.publish('orders.new', { i: 3 }) > 0); // rate 2/2 — proves the shed burned nothing
  assert.equal(ns.publish('orders.new', { i: 4 }), 0); // rate-limit shed now
  assert.equal(rejected[rejected.length - 1].reason, 'rate-limit');
});

test('byte limit sheds oversized publishes', async () => {
  const { bus, rejected } = busWithRejections();
  const ns = bus.createNamespace('acme');
  sink(ns);
  // '{}' is 2 bytes; '{"pad":"xxxx"}' is larger.
  bus.setNamespaceQuota('acme', 'acme/orders.**', { byteLimit: 10 });
  assert.ok(ns.publish('orders.new', {}) > 0);
  assert.equal(ns.publish('orders.new', { pad: 'x'.repeat(100) }), 0);
  assert.equal(rejected[rejected.length - 1].reason, 'tenant-quota');
  const q = bus.getStats().namespaceQuotas[0];
  assert.ok(q.usedBytes <= 10, `used ${q.usedBytes}`);
  assert.equal(q.byteLimit, 10);
});

test('soft threshold fires one latched warning per excursion and re-arms', () => {
  let nowMs = 1_000_000;
  const bus = new EventBus({ now: () => nowMs });
  const ns = bus.createNamespace('acme');
  bus.setNamespaceQuota('acme', 'acme/orders.**', {
    messageLimit: 100,
    softMessages: 2,
    windowMs: 60_000,
  });

  ns.publish('orders.new', { i: 1 });
  assert.equal(bus.getSubscriptionAudit({ action: 'tenant_quota_warning' }).length, 0);
  ns.publish('orders.new', { i: 2 });
  let warnings = bus.getSubscriptionAudit({ action: 'tenant_quota_warning' });
  assert.equal(warnings.length, 1);
  const detail = warnings[0].detail as Record<string, unknown>;
  assert.equal(detail.namespace, 'acme');
  assert.equal(detail.pattern, 'acme/orders.**');
  assert.equal(detail.usedMessages, 2);
  assert.equal(detail.softMessages, 2);
  assert.equal(warnings[0].topic, 'acme/orders.new');

  // Latched: more publishes inside the window fire nothing new.
  ns.publish('orders.new', { i: 3 });
  assert.equal(bus.getSubscriptionAudit({ action: 'tenant_quota_warning' }).length, 1);

  // The window slides: usage drops under the threshold, the latch
  // re-arms, and the next crossing warns again.
  nowMs += 61_000;
  ns.publish('orders.new', { i: 4 });
  assert.equal(bus.getSubscriptionAudit({ action: 'tenant_quota_warning' }).length, 1);
  ns.publish('orders.new', { i: 5 });
  warnings = bus.getSubscriptionAudit({ action: 'tenant_quota_warning' });
  assert.equal(warnings.length, 2);
  assert.equal(bus.getStats().namespaceQuotas[0].warnings, 2);
});

test('sliding window expires old usage', () => {
  let nowMs = 2_000_000;
  const bus = new EventBus({ now: () => nowMs });
  const ns = bus.createNamespace('acme');
  sink(ns);
  bus.setNamespaceQuota('acme', 'acme/orders.**', { messageLimit: 2, windowMs: 60_000 });

  assert.ok(ns.publish('orders.new', { i: 1 }) > 0);
  assert.ok(ns.publish('orders.new', { i: 2 }) > 0);
  assert.equal(ns.publish('orders.new', { i: 3 }), 0); // shed
  nowMs += 61_000;
  assert.ok(ns.publish('orders.new', { i: 4 }) > 0); // window slid
  assert.equal(bus.getStats().namespaceQuotas[0].usedMessages, 1);
});

test('namespace scoping: other namespaces and the global bus are untouched', async () => {
  const { bus } = busWithRejections();
  const acme = bus.createNamespace('acme');
  const other = bus.createNamespace('other');
  sink(acme);
  sink(other);
  sink(bus);
  bus.setNamespaceQuota('acme', 'acme/orders.**', { messageLimit: 1 });

  assert.ok(acme.publish('orders.new', {}) > 0);
  assert.equal(acme.publish('orders.new', {}), 0); // shed
  assert.ok(other.publish('orders.new', {}) > 0); // untouched
  assert.ok(bus.publish('unrelated.topic', {}) > 0); // global bus untouched
  await flush();
  assert.equal(bus.getStats().tenantQuotaShedMessages, 1);
});

test('star namespace matches the global bus (patterns match resolved topics)', () => {
  const { bus } = busWithRejections();
  const acme = bus.createNamespace('acme');
  sink(bus);
  sink(acme);
  bus.setNamespaceQuota('*', 'orders.**', { messageLimit: 1 });

  assert.ok(bus.publish('orders.new', {}) > 0);
  assert.equal(bus.publish('orders.new', {}), 0); // shed on the global bus
  // Patterns match the namespace-resolved topic, so 'orders.**' does
  // not match 'acme/orders.new' — a star-namespaced rule that should
  // cover a tenant names the resolved pattern explicitly.
  assert.ok(acme.publish('orders.new', {}) > 0);
  assert.equal(bus.getStats().tenantQuotaShedMessages, 1);

  bus.setNamespaceQuota('*', 'acme/orders.**', { messageLimit: 1 });
  assert.ok(acme.publish('orders.new', {}) > 0);
  assert.equal(acme.publish('orders.new', {}), 0); // shed via the star rule
  assert.equal(bus.getStats().tenantQuotaShedMessages, 2);
});

test('exact rule wins over patterns; earliest pattern wins', () => {
  const { bus } = busWithRejections();
  const ns = bus.createNamespace('acme');
  sink(ns);
  bus.setNamespaceQuota('acme', 'acme/orders.**', { messageLimit: 1 });
  bus.setNamespaceQuota('acme', 'acme/orders.new', { messageLimit: 10 });

  // The exact rule wins: 3 publishes all admitted.
  assert.ok(ns.publish('orders.new', {}) > 0);
  assert.ok(ns.publish('orders.new', {}) > 0);
  assert.ok(ns.publish('orders.new', {}) > 0);
  // The pattern rule governs other topics: second publish sheds.
  assert.ok(ns.publish('orders.other', {}) > 0);
  assert.equal(ns.publish('orders.other', {}), 0);
  assert.equal(bus.getStats().tenantQuotaShedMessages, 1);
});

test('earliest-registered matching pattern wins', () => {
  const { bus } = busWithRejections();
  const ns = bus.createNamespace('acme');
  sink(ns);
  bus.setNamespaceQuota('acme', 'acme/a.**', { messageLimit: 1 });
  bus.setNamespaceQuota('acme', 'acme/**', { messageLimit: 100 });

  assert.ok(ns.publish('a.x', {}) > 0);
  assert.equal(ns.publish('a.x', {}), 0); // first pattern's limit applies
  assert.equal(bus.getStats().tenantQuotaShedMessages, 1);
});

test('replacing a rule keeps usage; tightening bites immediately', () => {
  const { bus } = busWithRejections();
  const ns = bus.createNamespace('acme');
  sink(ns);
  bus.setNamespaceQuota('acme', 'acme/orders.**', { messageLimit: 2 });
  ns.publish('orders.new', {});
  ns.publish('orders.new', {});
  // Replace with a looser limit: usage is kept, not reset.
  bus.setNamespaceQuota('acme', 'acme/orders.**', { messageLimit: 5 });
  assert.equal(bus.getStats().namespaceQuotas[0].usedMessages, 2);
  assert.ok(ns.publish('orders.new', {}) > 0);
  // Tighten below current usage: the next publish sheds at once.
  bus.setNamespaceQuota('acme', 'acme/orders.**', { messageLimit: 1 });
  assert.equal(ns.publish('orders.new', {}), 0);
  assert.equal(bus.getStats().tenantQuotaShedMessages, 1);
});

test('clearNamespaceQuota removes the rule and its usage', () => {
  const { bus } = busWithRejections();
  const ns = bus.createNamespace('acme');
  sink(ns);
  bus.setNamespaceQuota('acme', 'acme/orders.**', { messageLimit: 1 });
  assert.ok(ns.publish('orders.new', {}) > 0);
  assert.equal(ns.publish('orders.new', {}), 0);
  assert.ok(bus.clearNamespaceQuota('acme', 'acme/orders.**'));
  assert.ok(!bus.clearNamespaceQuota('acme', 'acme/orders.**'));
  assert.equal(bus.getStats().namespaceQuotas.length, 0);
  assert.ok(ns.publish('orders.new', {}) > 0); // ungated again
});

test('previewPublish reports the verdict without consuming quota', () => {
  const { bus } = busWithRejections();
  const ns = bus.createNamespace('acme');
  bus.setNamespaceQuota('acme', 'acme/orders.**', { messageLimit: 1 });
  ns.publish('orders.new', {});
  const verdict = bus.previewPublish('acme/orders.new', {});
  assert.deepEqual(verdict, {
    admitted: false,
    reason: 'tenant-quota',
    failedGate: 'tenant-quota',
  });
  // The preview consumed nothing: the real publish still sheds (not
  // double-counted) and usage is unchanged.
  assert.equal(bus.getStats().namespaceQuotas[0].usedMessages, 1);
  assert.equal(ns.publish('orders.new', {}), 0);
  assert.equal(bus.getStats().namespaceQuotas[0].usedMessages, 1);
});

test('publishAtomic checks quota in shadow and records at commit', () => {
  const { bus } = busWithRejections();
  bus.setNamespaceQuota('acme', 'acme/orders.**', { messageLimit: 3 });

  const ok = bus.publishAtomic([
    { topic: 'acme/orders.new', payload: { i: 1 } },
    { topic: 'acme/orders.new', payload: { i: 2 } },
  ]);
  assert.deepEqual(ok, { published: 2 });
  assert.equal(bus.getStats().namespaceQuotas[0].usedMessages, 2);

  // The batch's own entries count against the shadow: 2 + 2 > 3.
  const rejected = bus.publishAtomic([
    { topic: 'acme/orders.new', payload: { i: 3 } },
    { topic: 'acme/orders.new', payload: { i: 4 } },
  ]);
  assert.equal(rejected.published, 0);
  assert.equal(rejected.rejected?.reason, 'tenant-quota');
  assert.equal(bus.getStats().namespaceQuotas[0].usedMessages, 2);
  assert.equal(bus.getStats().tenantQuotaShedMessages, 1);
});

test('prometheus exposes per-rule window bytes', () => {
  const { bus } = busWithRejections();
  const ns = bus.createNamespace('acme');
  bus.setNamespaceQuota('acme', 'acme/orders.**', { messageLimit: 100 });
  ns.publish('orders.new', { i: 1 });
  const out = renderPrometheus(bus.getStats());
  const usedBytes = bus.getStats().namespaceQuotas[0].usedBytes;
  assert.ok(
    out.includes(
      `eventbus_tenant_quota_bytes_total{namespace="acme",pattern="acme/orders.**"} ${usedBytes}`,
    ),
    'missing quota bytes series',
  );
});

test('setNamespaceQuota validation', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setNamespaceQuota('', 'acme/**', { messageLimit: 1 }), RangeError);
  assert.throws(() => bus.setNamespaceQuota('acme', '', { messageLimit: 1 }), RangeError);
  assert.throws(
    () => bus.setNamespaceQuota('acme', 'acme/**', { messageLimit: 1, windowMs: 0 }),
    RangeError,
  );
  assert.throws(
    () => bus.setNamespaceQuota('acme', 'acme/**', { messageLimit: -1 }),
    RangeError,
  );
  assert.throws(
    () => bus.setNamespaceQuota('acme', 'acme/**', { messageLimit: 1.5 }),
    RangeError,
  );
  assert.throws(
    () => bus.setNamespaceQuota('acme', 'acme/**', { byteLimit: NaN }),
    RangeError,
  );
  assert.throws(() => bus.setNamespaceQuota('acme', 'acme/**', {}), RangeError);
  assert.throws(
    () => bus.setNamespaceQuota('a/b', 'a/**', { messageLimit: 1 }),
    RangeError,
  );
  // A valid rule constructs fine.
  bus.setNamespaceQuota('acme', 'acme/**', { softMessages: 10 });
  assert.equal(bus.getStats().namespaceQuotas.length, 1);
});
