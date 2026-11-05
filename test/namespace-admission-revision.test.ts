import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, ConfigApplyError, type BusMessage } from '../src/bus.ts';
import { NamespaceDeniedError } from '../src/namespace.ts';

/**
 * Live revision of namespace admission flags (EB-386):
 * `setNamespaceOptions` retunes `allowPublish` / `allowSubscribe` for a
 * registered namespace without deleting and re-creating it.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-ns-revision-'));

const topics = (received: BusMessage[]): string[] => received.map((m) => m.topic);

test('tightening allowPublish rejects the namespace publish with NamespaceDeniedError', () => {
  const bus = new EventBus();
  const t1 = bus.createNamespace('t1');
  t1.subscribe('**', () => {});
  assert.ok(t1.publish('orders', { id: 1 }) > 0);
  bus.setNamespaceOptions('t1', { allowPublish: false });
  assert.throws(
    () => t1.publish('orders', { id: 2 }),
    (err) => {
      assert.ok(err instanceof NamespaceDeniedError);
      assert.equal(err.action, 'publish');
      assert.equal(err.namespace, 't1');
      return true;
    },
  );
  assert.throws(() => bus.publish('orders', { id: 3 }, { namespace: 't1' }), NamespaceDeniedError);
});

test('tightening one namespace leaves the other namespace untouched', async () => {
  const bus = new EventBus();
  const t1 = bus.createNamespace('t1');
  const t2 = bus.createNamespace('t2');
  const received: BusMessage[] = [];
  t2.subscribe('**', (msg) => received.push(msg));
  bus.setNamespaceOptions('t1', { allowPublish: false });
  assert.throws(() => t1.publish('orders', { id: 1 }), NamespaceDeniedError);
  assert.ok(t2.publish('orders', { id: 2 }) > 0);
  await flush();
  assert.deepEqual(topics(received), ['t2/orders']);
});

test('loosening restores admission for the very next publish', () => {
  const bus = new EventBus();
  const t1 = bus.createNamespace('t1');
  t1.subscribe('**', () => {});
  bus.setNamespaceOptions('t1', { allowPublish: false });
  assert.throws(() => t1.publish('orders', { id: 1 }), NamespaceDeniedError);
  bus.setNamespaceOptions('t1', { allowPublish: true });
  assert.ok(t1.publish('orders', { id: 2 }) > 0);
});

test('denying subscribe blocks new subscriptions but not the namespace itself', () => {
  const bus = new EventBus();
  const t1 = bus.createNamespace('t1');
  t1.subscribe('**', () => {});
  bus.setNamespaceOptions('t1', { allowSubscribe: false });
  assert.throws(
    () => t1.subscribe('**', () => {}),
    (err) => {
      assert.ok(err instanceof NamespaceDeniedError);
      assert.equal(err.action, 'subscribe');
      assert.equal(err.namespace, 't1');
      return true;
    },
  );
  assert.throws(
    () => bus.subscribe('**', () => {}, { namespace: 't1' }),
    NamespaceDeniedError,
  );
  // Publishing still works: only the subscribe flag moved.
  assert.ok(t1.publish('orders', { id: 1 }) > 0);
});

test('an existing subscriber keeps receiving after subscribe is denied', async () => {
  const bus = new EventBus();
  const t1 = bus.createNamespace('t1');
  const received: BusMessage[] = [];
  t1.subscribe('**', (msg) => received.push(msg));
  t1.publish('orders', { id: 1 });
  await flush();
  bus.setNamespaceOptions('t1', { allowSubscribe: false });
  t1.publish('orders', { id: 2 });
  await flush();
  assert.deepEqual(
    received.map((m) => (m.payload as { id: number }).id),
    [1, 2],
  );
});

test('re-affirming the live flags is a no-op with no audit', () => {
  const bus = new EventBus();
  bus.createNamespace('t1', { allowPublish: false });
  bus.setNamespaceOptions('t1', { allowPublish: false });
  bus.setNamespaceOptions('t1', {});
  assert.equal(bus.getSubscriptionAudit({ action: 'namespace_options_revised' }).length, 0);
  bus.setNamespaceOptions('t1', { allowPublish: true });
  assert.equal(bus.getSubscriptionAudit({ action: 'namespace_options_revised' }).length, 1);
});

test('illegal values and unknown prefixes throw RangeError and leave the live flags untouched', () => {
  const bus = new EventBus();
  bus.createNamespace('t1', { allowPublish: true, allowSubscribe: false });
  assert.throws(() => bus.setNamespaceOptions('t1', 'x' as unknown as object), RangeError);
  assert.throws(() => bus.setNamespaceOptions('t1', null as unknown as object), RangeError);
  assert.throws(
    () => bus.setNamespaceOptions('t1', { allowPublish: 'yes' as unknown as boolean }),
    RangeError,
  );
  assert.throws(
    () => bus.setNamespaceOptions('t1', { allowSubscribe: 0 as unknown as boolean }),
    RangeError,
  );
  assert.throws(() => bus.setNamespaceOptions('nope', { allowPublish: false }), RangeError);
  assert.deepEqual(
    bus.getNamespaces().map(({ prefix, allowPublish, allowSubscribe }) => ({
      prefix,
      allowPublish,
      allowSubscribe,
    })),
    [{ prefix: 't1', allowPublish: true, allowSubscribe: false }],
  );
  assert.equal(bus.getSubscriptionAudit({ action: 'namespace_options_revised' }).length, 0);
});

test('audit pins previous and current, including the first revision previous', () => {
  const bus = new EventBus();
  bus.createNamespace('t1', { allowPublish: true, allowSubscribe: true });
  bus.setNamespaceOptions('t1', { allowPublish: false }, 'ops');
  bus.setNamespaceOptions('t1', { allowSubscribe: false }, 'ops');
  const audit = bus.getSubscriptionAudit({ action: 'namespace_options_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[0].detail, {
    prefix: 't1',
    allowPublish: false,
    oldAllowPublish: true,
    allowSubscribe: true,
    oldAllowSubscribe: true,
  });
  assert.deepEqual(audit[1].detail, {
    prefix: 't1',
    allowPublish: false,
    oldAllowPublish: false,
    allowSubscribe: false,
    oldAllowSubscribe: true,
  });
  assert.equal(audit[0].actor, 'ops');
});

test('getNamespaces reports the live flags after revision', () => {
  const bus = new EventBus();
  bus.createNamespace('t1');
  bus.createNamespace('t2', { allowSubscribe: false });
  bus.setNamespaceOptions('t1', { allowPublish: false });
  bus.setNamespaceOptions('t2', { allowSubscribe: true, allowPublish: false });
  assert.deepEqual(
    bus.getNamespaces().map(({ prefix, allowPublish, allowSubscribe }) => ({
      prefix,
      allowPublish,
      allowSubscribe,
    })),
    [
      { prefix: 't1', allowPublish: false, allowSubscribe: true },
      { prefix: 't2', allowPublish: false, allowSubscribe: true },
    ],
  );
});

test('revision touches neither the child durable log nor the quota rules', async () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  const t1 = bus.createNamespace('t1');
  t1.subscribe('**', () => {});
  bus.setNamespaceQuota('t1', 't1/orders', { messageLimit: 1 });
  assert.ok(t1.publish('orders', { id: 1 }) > 0);
  await flush();
  // Second publish sheds on the quota (limit 1).
  assert.equal(t1.publish('orders', { id: 2 }), 0);
  assert.equal(bus.getStats().tenantQuotaShedMessages, 1);
  // A real flag change: only the subscribe flag moves.
  bus.setNamespaceOptions('t1', { allowSubscribe: false });
  // The quota still bites — publishing is still admitted to the quota
  // gate (allowPublish untouched) and still sheds past the limit.
  assert.equal(t1.publish('orders', { id: 3 }), 0);
  assert.equal(bus.getStats().tenantQuotaShedMessages, 2);
  // The child log still holds exactly the one admitted record: replay
  // through the handle reads it back once the subscribe flag is back.
  bus.setNamespaceOptions('t1', { allowSubscribe: true });
  const replayed: Array<{ seq: number; payload: unknown }> = [];
  t1.subscribe('orders', (msg) => replayed.push({ seq: msg.seq, payload: msg.payload }), {
    resumeFromSeq: 0,
  });
  await flush();
  assert.deepEqual(replayed, [{ seq: 1, payload: { id: 1 } }]);
});

test('applyConfig still cannot change a live namespace flags', () => {
  const bus = new EventBus();
  bus.createNamespace('t1', { allowPublish: true, allowSubscribe: true });
  // A live subscriber makes deleteNamespace refuse, so the
  // delete-then-create modify fails and the apply rolls back.
  bus.subscribe('**', () => {}, { namespace: 't1' });
  const snap = bus.exportBusConfig();
  snap.namespaces.t1.allowSubscribe = false;
  assert.throws(() => bus.applyConfig(snap), ConfigApplyError);
  assert.deepEqual(
    bus.getNamespaces().map(({ prefix, allowPublish, allowSubscribe }) => ({
      prefix,
      allowPublish,
      allowSubscribe,
    })),
    [{ prefix: 't1', allowPublish: true, allowSubscribe: true }],
  );
});

test('the NamespaceHandle obeys the revised flags', async () => {
  const bus = new EventBus();
  const t1 = bus.createNamespace('t1');
  const received: BusMessage[] = [];
  t1.subscribe('**', (msg) => received.push(msg));
  bus.setNamespaceOptions('t1', { allowPublish: false, allowSubscribe: false });
  assert.throws(() => t1.publish('orders', { id: 1 }), NamespaceDeniedError);
  assert.throws(() => t1.subscribe('orders', () => {}), NamespaceDeniedError);
  // The pre-revision subscription still drains nothing new (nothing
  // was published), and loosening lets the handle publish again.
  bus.setNamespaceOptions('t1', { allowPublish: true });
  assert.ok(t1.publish('orders', { id: 2 }) > 0);
  await flush();
  assert.deepEqual(
    received.map((m) => (m.payload as { id: number }).id),
    [2],
  );
});
