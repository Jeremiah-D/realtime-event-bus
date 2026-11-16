import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/**
 * Bus-level default reliable redelivery backoff (EB-519):
 * `setDefaultRedeliveryBackoff` installs the backoff config a new
 * reliable subscriber without an explicit `redeliveryBackoff` falls
 * back to — the precedence chain is subscriber-explicit > bus default
 * > immediate redelivery — and the default is baked into the reliable
 * subscriber record at subscribe time, so a later revision never
 * moves an already-subscribed subscriber or an armed timer.
 *
 * Timing assertions follow `redelivery-backoff.test.ts`: loose lower
 * bounds only, so the suite stays CI-safe.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** The `default_redelivery_backoff_revised` audit events on the bus. */
function defaultBackoffAudits(bus: EventBus) {
  return bus.getSubscriptionAudit({ action: 'default_redelivery_backoff_revised' });
}

test('EB-519: set a default then a new reliable subscriber omitting the config waits out the backoff', async () => {
  const bus = new EventBus();
  bus.setDefaultRedeliveryBackoff({ baseMs: 40, capMs: 5000 });
  const seen: number[] = [];
  const at: number[] = [];
  bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      at.push(Date.now());
      if (d.redeliveries < 2) d.nack();
      else d.ack();
    },
    { ackTimeoutMs: 5000 },
  );
  bus.publish('t', 'flaky');
  await sleep(25);
  // The first redelivery is delayed, not immediate.
  assert.deepEqual(seen, [0]);
  await sleep(150);
  assert.deepEqual(seen, [0, 1, 2]);
  // Delays are honored (loose lower bounds — CI-safe): attempt 1 waits
  // baseMs, attempt 2 waits baseMs * 2.
  assert.ok(at[1] - at[0] >= 25, `attempt 1 fired too early: ${at[1] - at[0]}ms`);
  assert.ok(at[2] - at[1] >= 55, `attempt 2 fired too early: ${at[2] - at[1]}ms`);
});

test('EB-519: an explicit config beats the default', async () => {
  const bus = new EventBus();
  bus.setDefaultRedeliveryBackoff({ baseMs: 40 });
  const seen: number[] = [];
  bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      d.nack();
    },
    { ackTimeoutMs: 5000, redeliveryBackoff: { baseMs: 5000 } },
  );
  bus.publish('t', 'x');
  await sleep(150);
  // Still on the explicit 5000ms scheduler, not the 40ms default.
  assert.deepEqual(seen, [0]);
});

test('EB-519: subscribing with null still throws RangeError and the live default is untouched', () => {
  const bus = new EventBus();
  bus.setDefaultRedeliveryBackoff({ baseMs: 40 });
  assert.throws(
    () =>
      bus.subscribeReliable('t', () => {}, {
        redeliveryBackoff: null as never,
      }),
    RangeError,
  );
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.deepEqual(bus.getDefaultRedeliveryBackoff(), {
    baseMs: 40,
    capMs: Number.POSITIVE_INFINITY,
    jitter: false,
  });
  assert.equal(defaultBackoffAudits(bus).length, 1);
});

test('EB-519: set(null) clears the default back to immediate redelivery', async () => {
  const bus = new EventBus();
  bus.setDefaultRedeliveryBackoff({ baseMs: 5000 });
  bus.setDefaultRedeliveryBackoff(null);
  assert.equal(bus.getDefaultRedeliveryBackoff(), undefined);
  const seen: number[] = [];
  bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      if (d.redeliveries < 3) d.nack();
      else d.ack();
    },
    { ackTimeoutMs: 5000 },
  );
  bus.publish('t', 'x');
  await sleep(150);
  assert.deepEqual(seen, [0, 1, 2, 3]);
});

test('EB-519: illegal values and illegal actors throw RangeError and leave the live default untouched', () => {
  const bus = new EventBus();
  bus.setDefaultRedeliveryBackoff({ baseMs: 100, capMs: 500 });
  for (const bad of [
    { baseMs: 0 },
    { baseMs: -5 },
    { baseMs: Number.NaN },
    { baseMs: '100' },
    {},
    { baseMs: 100, capMs: 50 },
    { baseMs: 100, capMs: Number.POSITIVE_INFINITY },
    { baseMs: 100, jitter: 1 },
    { baseMs: 100, jitter: 'yes' },
    100,
    '100',
    true,
    [100],
    undefined,
  ]) {
    assert.throws(
      () => bus.setDefaultRedeliveryBackoff(bad as never),
      RangeError,
      `config=${JSON.stringify(bad)}`,
    );
  }
  assert.throws(() => bus.setDefaultRedeliveryBackoff({ baseMs: 200 }, ''), RangeError);
  assert.throws(() => bus.setDefaultRedeliveryBackoff({ baseMs: 200 }, 42 as never), RangeError);
  assert.deepEqual(bus.getDefaultRedeliveryBackoff(), {
    baseMs: 100,
    capMs: 500,
    jitter: false,
  });
  assert.equal(
    defaultBackoffAudits(bus).length,
    1,
    'the failed calls audited nothing',
  );
});

test('EB-519: a same-value revision is a no-op with no audit', () => {
  const bus = new EventBus();
  // Clearing a bus already at immediate redelivery audits nothing.
  bus.setDefaultRedeliveryBackoff(null);
  assert.equal(defaultBackoffAudits(bus).length, 0);
  assert.equal(bus.getDefaultRedeliveryBackoff(), undefined);
  bus.setDefaultRedeliveryBackoff({ baseMs: 100 });
  // A config that normalizes to the same values is the same revision.
  bus.setDefaultRedeliveryBackoff({ baseMs: 100, jitter: false });
  bus.setDefaultRedeliveryBackoff({ baseMs: 100 });
  const audit = defaultBackoffAudits(bus);
  assert.equal(audit.length, 1, 're-affirming the default audits nothing');
  assert.deepEqual(audit[0].detail, {
    previous: null,
    redeliveryBackoff: { baseMs: 100, capMs: Number.POSITIVE_INFINITY, jitter: false },
  });
});

test('EB-519: a revision never moves an already-subscribed reliable subscriber', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      d.nack();
    },
    { ackTimeoutMs: 5000, redeliveryBackoff: { baseMs: 5000 } },
  );
  // A later default revision must not move the scheduler the
  // subscriber was baked with.
  bus.setDefaultRedeliveryBackoff({ baseMs: 40 });
  bus.publish('t', 'x');
  await sleep(150);
  assert.deepEqual(seen, [0]);
});

test('EB-519: a revision never shortens or lengthens an armed timer', async () => {
  const bus = new EventBus();
  bus.setDefaultRedeliveryBackoff({ baseMs: 60 });
  const seen: number[] = [];
  bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      if (d.redeliveries === 0) d.nack();
      else d.ack();
    },
    { ackTimeoutMs: 5000 },
  );
  bus.publish('t', 'x');
  await flush();
  await flush();
  assert.deepEqual(seen, [0]);
  // The 60ms delay is armed; revising the default to 5000ms now must
  // not re-arm it.
  bus.setDefaultRedeliveryBackoff({ baseMs: 5000 });
  await sleep(200);
  assert.deepEqual(seen, [0, 1]);
});

test('EB-519: per-subscriber setRedeliveryBackoff still revises one subscriber and a later default revision does not move it', async () => {
  const bus = new EventBus();
  bus.setDefaultRedeliveryBackoff({ baseMs: 40 });
  const seen: number[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      d.nack();
    },
    { ackTimeoutMs: 5000 },
  );
  // Per-subscriber revision to a 5000ms policy: the next redelivery
  // waits it out.
  bus.setRedeliveryBackoff(sub.id, { baseMs: 5000 });
  // A later default revision must not move the policy
  // `setRedeliveryBackoff` installed.
  bus.setDefaultRedeliveryBackoff({ baseMs: 20 });
  bus.publish('t', 'x');
  await sleep(150);
  assert.deepEqual(seen, [0]);
});

test('EB-519: the default has no effect on plain subscriptions', async () => {
  const bus = new EventBus();
  bus.setDefaultRedeliveryBackoff({ baseMs: 5000 });
  const plainSeen: unknown[] = [];
  bus.subscribe('plain', (msg) => plainSeen.push(msg.payload));
  bus.publish('plain', 'a');
  bus.publish('plain', 'b');
  bus.publish('plain', 'c');
  await flush();
  // Plain delivery is immediate and unspaced: no backoff scheduler
  // exists for it.
  assert.deepEqual(plainSeen, ['a', 'b', 'c']);
  // The same default does govern a reliable subscriber on the bus.
  const seen: number[] = [];
  bus.subscribeReliable(
    'rel',
    (d) => {
      seen.push(d.redeliveries);
      d.nack();
    },
    { ackTimeoutMs: 5000 },
  );
  bus.publish('rel', 'x');
  await sleep(150);
  assert.deepEqual(seen, [0]);
});

test('EB-519: the config is stored normalized and the getter returns a copy', () => {
  const bus = new EventBus();
  const input = { baseMs: 100 };
  bus.setDefaultRedeliveryBackoff(input);
  assert.deepEqual(bus.getDefaultRedeliveryBackoff(), {
    baseMs: 100,
    capMs: Number.POSITIVE_INFINITY,
    jitter: false,
  });
  // Mutating the caller's object after the call never moves the live
  // default.
  (input as { baseMs: number }).baseMs = 999;
  assert.deepEqual(bus.getDefaultRedeliveryBackoff(), {
    baseMs: 100,
    capMs: Number.POSITIVE_INFINITY,
    jitter: false,
  });
  // Mutating what the getter returned never moves the live default
  // either.
  const copy = bus.getDefaultRedeliveryBackoff();
  assert.notEqual(copy, bus.getDefaultRedeliveryBackoff());
  (copy as { baseMs: number }).baseMs = 999;
  assert.deepEqual(bus.getDefaultRedeliveryBackoff(), {
    baseMs: 100,
    capMs: Number.POSITIVE_INFINITY,
    jitter: false,
  });
  const factory = new EventBus();
  assert.equal(factory.getDefaultRedeliveryBackoff(), undefined);
});

test('EB-519: audit records previous/redeliveryBackoff exactly (normalized) and the getter follows the live value', () => {
  const bus = new EventBus();
  bus.setDefaultRedeliveryBackoff({ baseMs: 100, capMs: 5000 }, 'ops');
  bus.setDefaultRedeliveryBackoff({ baseMs: 200 });
  bus.setDefaultRedeliveryBackoff(null);
  const audit = defaultBackoffAudits(bus);
  assert.equal(audit.length, 3);
  assert.equal(audit[0].actor, 'ops');
  assert.deepEqual(audit[0].detail, {
    previous: null,
    redeliveryBackoff: { baseMs: 100, capMs: 5000, jitter: false },
  });
  assert.deepEqual(audit[1].detail, {
    previous: { baseMs: 100, capMs: 5000, jitter: false },
    redeliveryBackoff: { baseMs: 200, capMs: Number.POSITIVE_INFINITY, jitter: false },
  });
  assert.deepEqual(audit[2].detail, {
    previous: { baseMs: 200, capMs: Number.POSITIVE_INFINITY, jitter: false },
    redeliveryBackoff: null,
  });
  assert.equal(bus.getDefaultRedeliveryBackoff(), undefined);
  // Clearing a bus already at immediate redelivery is a no-op: the
  // third entry above already cleared it, so a second clear audits
  // nothing.
  bus.setDefaultRedeliveryBackoff(null);
  assert.equal(defaultBackoffAudits(bus).length, 3);
});
