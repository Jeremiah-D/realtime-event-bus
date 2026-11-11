import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/**
 * A manually-advanced clock handed to the bus via `EventBusOptions.now`,
 * so expiry tests are deterministic instead of racing wall time.
 */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-default-ttl-'));

test('the factory default TTL is unset: messages never expire', () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultTtlMs(), undefined);
});

test('without a default, a publish with no ttlMs never expires', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publish('t', 'forever');
  clock.nowMs = 1_000_000; // far in the future
  await flush();
  assert.deepEqual(received, ['forever']);
  assert.equal(bus.getStats().expiredMessages, 0);
});

test('a set default expires a no-ttl publish at its deadline', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setDefaultTtlMs(60_000); // deadline t=61000
  bus.publish('t', 'short-lived');
  clock.nowMs = 60_000; // inside the deadline
  await flush();
  assert.deepEqual(received, ['short-lived']);
  // A second publish past the deadline expires instead of delivering.
  bus.publish('t', 'doomed');
  clock.nowMs = 121_000; // past the second message's deadline t=121000
  await flush();
  assert.deepEqual(received, ['short-lived']);
  assert.equal(bus.getStats().expiredMessages, 1);
});

test('a per-message TTL beats the bus default in both directions', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setDefaultTtlMs(10_000); // default deadline t=11000
  bus.publish('t', 'short', { ttlMs: 100 }); // per-message: deadline t=1100
  bus.publish('t', 'protected', { ttlMs: 60_000 }); // per-message: t=61000
  bus.publish('t', 'defaulted'); // default: t=11000
  clock.nowMs = 1_200; // past the short per-message deadline only
  await flush();
  assert.deepEqual(received, ['protected', 'defaulted']);
  assert.equal(bus.getStats().expiredMessages, 1);
  bus.publish('t', 'late-default'); // default deadline t=11200
  clock.nowMs = 12_000; // past the default deadline, inside the long one
  await flush();
  assert.deepEqual(received, ['protected', 'defaulted']);
  assert.equal(bus.getStats().expiredMessages, 2);
});

test('a topic rule beats the bus default', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setDefaultTtlMs(60_000); // default deadline t=61000
  bus.setTopicTtl('t', 100); // topic rule: deadline t=1100
  bus.publish('t', 'ruled');
  clock.nowMs = 1_200; // past the topic rule, inside the bus default
  await flush();
  assert.deepEqual(received, []);
  assert.equal(bus.getStats().expiredMessages, 1);
});

test('a delayed schedule stamps the default at schedule time; a later revision does not move it', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setDefaultTtlMs(100); // schedule-time deadline t=1100
  bus.publishDelayed('t', 'scheduled', { delayMs: 2000 }); // due t=3000
  bus.setDefaultTtlMs(60_000); // widened after scheduling: must not move the stamp
  clock.nowMs = 3_000;
  bus.publish('t', 'poke'); // sweeps the due entry
  await flush();
  // The scheduled message was due past its schedule-time deadline, so
  // it expires instead of delivering; only the poke is received.
  assert.deepEqual(received, ['poke']);
  assert.equal(bus.getStats().expiredMessages, 1);
});

test('a delayed schedule made after a revision uses the new default', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setDefaultTtlMs(100);
  bus.setDefaultTtlMs(60_000); // schedule-time deadline t=61000
  bus.publishDelayed('t', 'scheduled', { delayMs: 2000 }); // due t=3000
  clock.nowMs = 3_000;
  bus.publish('t', 'poke'); // sweeps the due entry
  await flush();
  assert.ok(received.includes('scheduled'), 'schedule under the widened default must deliver');
  assert.equal(bus.getStats().expiredMessages, 0);
});

test('clearing the default with undefined restores never-expire', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setDefaultTtlMs(100);
  bus.setDefaultTtlMs(undefined);
  assert.equal(bus.getDefaultTtlMs(), undefined);
  bus.publish('t', 'forever');
  clock.nowMs = 1_000_000;
  await flush();
  assert.deepEqual(received, ['forever']);
  assert.equal(bus.getStats().expiredMessages, 0);
});

test('illegal default values throw RangeError and the live default is untouched', () => {
  const bus = new EventBus();
  bus.setDefaultTtlMs(60_000);
  const badValues: unknown[] = [1.5, -1, NaN, Infinity, -Infinity, '100', null, {}, true];
  for (const bad of badValues) {
    assert.throws(
      () => bus.setDefaultTtlMs(bad as number),
      RangeError,
      `ttlMs=${String(bad)} should throw`,
    );
    assert.equal(bus.getDefaultTtlMs(), 60_000, 'a failed revision must not move the live default');
  }
  assert.throws(() => bus.setDefaultTtlMs(100, ''), RangeError, 'an illegal actor throws');
  assert.equal(bus.getDefaultTtlMs(), 60_000);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_ttl_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});

test('revising to the current default is a no-op with no audit, including clearing an unset default', () => {
  const bus = new EventBus();
  bus.setDefaultTtlMs(undefined);
  assert.equal(bus.getSubscriptionAudit({ action: 'default_ttl_revised' }).length, 0);
  bus.setDefaultTtlMs(30_000);
  bus.setDefaultTtlMs(30_000);
  const audit = bus.getSubscriptionAudit({ action: 'default_ttl_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { previous_ttl_ms: null, ttl_ms: 30_000 });
});

test('setDefaultTtlMs audits each revision with its old and new value and actor, null on both sides', () => {
  const bus = new EventBus();
  bus.setDefaultTtlMs(30_000, 'ops');
  bus.setDefaultTtlMs(undefined, 'ops');
  const events = bus.getSubscriptionAudit({ action: 'default_ttl_revised' });
  assert.equal(events.length, 2);
  assert.equal(events[0].actor, 'ops');
  assert.deepEqual(events[0].detail, { previous_ttl_ms: null, ttl_ms: 30_000 });
  assert.equal(events[1].actor, 'ops');
  assert.deepEqual(events[1].detail, { previous_ttl_ms: 30_000, ttl_ms: null });
});

test('the getter follows the live default', () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultTtlMs(), undefined);
  bus.setDefaultTtlMs(5_000);
  assert.equal(bus.getDefaultTtlMs(), 5_000);
  bus.setDefaultTtlMs(500);
  assert.equal(bus.getDefaultTtlMs(), 500);
  bus.setDefaultTtlMs(undefined);
  assert.equal(bus.getDefaultTtlMs(), undefined);
});

test('a routed forward keeps the source deadline verbatim, not the destination rule', async () => {
  const clock = controllableClock();
  const dir = freshDir();
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  bus.subscribe('src', () => {});
  bus.subscribe('dst', () => {});
  bus.setTopicRoute('src', 'dst');
  bus.setDefaultTtlMs(100); // source deadline t=1100
  bus.setTopicTtl('dst', 60_000); // destination rule would stamp t=61000
  bus.publish('src', 'routed');
  await flush();
  const log = DurableTopicLog.open({ dir });
  const dstRecords = log.readSince('dst', 0);
  assert.equal(dstRecords.length, 1);
  assert.equal(dstRecords[0].expiresAt, 1_100, 'the routed record must keep the source deadline');
});
