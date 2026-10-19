/**
 * Bus maintenance mode (EB-158).
 *
 * - `enterMaintenance(reason?)` puts the bus in maintenance mode;
 *   `isInMaintenance()` reports it; `exitMaintenance()` leaves it.
 * - While in maintenance, fresh publishes are shed with admission
 *   reason `'maintenance'` — zero side effects (no sequence number, no
 *   durable-log write, no rate-limit budget) — surfaced via
 *   `onAdmissionRejected`.
 * - In-flight deliveries and live subscriptions drain normally;
 *   subscribes and operator reads still work. Unlike `close()`
 *   (EB-154, terminal), maintenance is reversible.
 * - `getStats().maintenance` exposes `{ active, reason?, rejectedMessages }`.
 * - Transitions are audited (`maintenance_entered` / `maintenance_exited`).
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

test('enterMaintenance puts the bus in maintenance mode', (t) => {
  const bus = new EventBus();
  t.after(() => bus.close());

  assert.equal(bus.isInMaintenance(), false);
  bus.enterMaintenance('deploying v2');
  assert.equal(bus.isInMaintenance(), true);

  const stats = bus.getStats();
  assert.equal(stats.maintenance.active, true);
  assert.equal(stats.maintenance.reason, 'deploying v2');
});

test('publishes are rejected with reason maintenance while in maintenance', async (t) => {
  const rejections: { reason: string }[] = [];
  const bus = new EventBus({
    onAdmissionRejected: (e) => {
      rejections.push({ reason: e.reason });
    },
  });
  t.after(() => bus.close());

  bus.enterMaintenance();

  let received = 0;
  bus.subscribe('orders.**', () => {
    received += 1;
  });

  const accepted = bus.publish('orders.created', { id: 1 });
  assert.equal(accepted, 0);
  assert.equal(received, 0);
  assert.equal(rejections.length, 1);
  assert.equal(rejections[0].reason, 'maintenance');

  const stats = bus.getStats();
  assert.equal(stats.maintenance.active, true);
  assert.equal(stats.maintenance.rejectedMessages, 1);
  assert.equal(stats.rejectedMessages, 1);
});

test('exitMaintenance restores normal publishing', async (t) => {
  const bus = new EventBus();
  t.after(() => bus.close());

  bus.enterMaintenance('upgrade');
  assert.equal(bus.isInMaintenance(), true);
  assert.equal(bus.publish('orders.created', { id: 1 }), 0);

  bus.exitMaintenance();
  assert.equal(bus.isInMaintenance(), false);

  let received = 0;
  bus.subscribe('orders.**', () => {
    received += 1;
  });
  const accepted = bus.publish('orders.created', { id: 2 });
  assert.equal(accepted, 1);
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(received, 1);

  const stats = bus.getStats();
  assert.equal(stats.maintenance.active, false);
  assert.equal(stats.maintenance.reason, undefined);
});

test('in-flight deliveries drain normally during maintenance', async (t) => {
  const bus = new EventBus();
  t.after(() => bus.close());

  const seen: number[] = [];
  bus.subscribe('orders.**', (msg) => {
    seen.push(msg.payload as number);
  });

  // Publish before entering maintenance.
  for (let i = 0; i < 10; i++) {
    bus.publish('orders.created', i);
  }
  bus.enterMaintenance('drain test');

  // Wait for deliveries to complete.
  await new Promise<void>((resolve) => setImmediate(resolve));
  await new Promise<void>((resolve) => setImmediate(resolve));

  // All pre-maintenance publishes should have been delivered.
  assert.equal(seen.length, 10);
  assert.deepEqual(seen, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
});

test('subscribes still work during maintenance', (t) => {
  const bus = new EventBus();
  t.after(() => bus.close());

  bus.enterMaintenance();
  // Subscribing during maintenance should not throw.
  const sub = bus.subscribe('orders.**', () => {});
  assert.ok(sub);
  bus.exitMaintenance();
});

test('enterMaintenance with non-string reason throws TypeError', (t) => {
  const bus = new EventBus();
  t.after(() => bus.close());

  assert.throws(
    () => bus.enterMaintenance(123 as never),
    TypeError,
  );
});

test('entering maintenance twice updates the reason without duplicate audit', (t) => {
  const bus = new EventBus();
  t.after(() => bus.close());

  bus.enterMaintenance('first');
  assert.equal(bus.getStats().maintenance.reason, 'first');

  bus.enterMaintenance('second');
  assert.equal(bus.isInMaintenance(), true);
  assert.equal(bus.getStats().maintenance.reason, 'second');
});

test('exitMaintenance when not in maintenance is a no-op', (t) => {
  const bus = new EventBus();
  t.after(() => bus.close());

  assert.equal(bus.isInMaintenance(), false);
  bus.exitMaintenance(); // Should not throw.
  assert.equal(bus.isInMaintenance(), false);
});

test('maintenance transitions are audited', (t) => {
  const bus = new EventBus();
  t.after(() => bus.close());

  bus.enterMaintenance('planned');
  bus.exitMaintenance();

  const audit = bus.getSubscriptionAudit();
  const entered = audit.find((e) => e.action === 'maintenance_entered');
  const exited = audit.find((e) => e.action === 'maintenance_exited');
  assert.ok(entered, 'expected maintenance_entered audit entry');
  assert.ok(exited, 'expected maintenance_exited audit entry');
});

test('maintenance shed has zero side effects: no seq, no durable write', async (t) => {
  const { mkdtempSync } = await import('node:fs');
  const { tmpdir } = await import('node:os');
  const { join } = await import('node:path');
  const dir = mkdtempSync(join(tmpdir(), 'maintenance-test-'));
  const bus = new EventBus({
    durableLogDir: dir,
    publishAudit: true,
  });
  t.after(() => bus.close());

  bus.enterMaintenance();

  const before = bus.getStats();
  const accepted = bus.publish('orders.created', { id: 1 });
  assert.equal(accepted, 0);
  const after = bus.getStats();

  // Rejected count goes up, but no sequence was consumed for the shed message.
  assert.equal(after.rejectedMessages, before.rejectedMessages + 1);
  assert.equal(after.maintenance.rejectedMessages, 1);

  // The shed is journaled in the publish audit.
  const audit = bus.getPublishAudit({ topic: 'orders.created' });
  const shed = audit.find((e) => e.outcome === 'rejected' && e.reason === 'maintenance');
  assert.ok(shed, 'expected a maintenance audit entry');
});
