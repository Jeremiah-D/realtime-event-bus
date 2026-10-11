import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type IdleExpiredEvent } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const noop = () => {};

test('an idle subscription expires: event fires, counter moves, unsubscribe sticks', async () => {
  const events: IdleExpiredEvent[] = [];
  const bus = new EventBus({ onIdleExpired: (e) => events.push(e) });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (m) => received.push(m.payload), { maxIdleMs: 50 });
  bus.publish('t', 'before');
  await flush();
  assert.equal(received.length, 1);
  await sleep(150);
  // Gone, exactly like a manual unsubscribe.
  assert.equal(bus.getStats().totalSubscribers, 0);
  bus.publish('t', 'after');
  await flush();
  assert.equal(received.length, 1);
  // Expiry fired exactly once, after the unsubscribe.
  assert.equal(events.length, 1);
  assert.equal(events[0].subscriberId, sub.id);
  assert.equal(events[0].pattern, 't');
  assert.equal(events[0].maxIdleMs, 50);
  assert.equal(events[0].idleActivity, 'any');
  assert.equal(bus.getStats().idleExpired, 1);
});

test('successful deliveries re-arm the idle window: a busy subscriber never expires', async () => {
  const events: IdleExpiredEvent[] = [];
  const bus = new EventBus({ onIdleExpired: (e) => events.push(e) });
  const sub = bus.subscribe('t', noop, { maxIdleMs: 100 });
  // Keep the subscriber busy: every delivery re-arms a fresh 100ms window.
  for (let i = 0; i < 6; i += 1) {
    bus.publish('t', i);
    await flush();
    await sleep(30);
  }
  assert.equal(bus.getStats().totalSubscribers, 1);
  assert.equal(events.length, 0);
  // Go quiet: the window finally lapses after the last delivery.
  await sleep(220);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(events.length, 1);
  assert.equal(bus.getStats().idleExpired, 1);
  void sub;
});

test("idleActivity 'ack': acks re-arm the window for reliable subscribers", async () => {
  const events: IdleExpiredEvent[] = [];
  const bus = new EventBus({ onIdleExpired: (e) => events.push(e) });
  const sub = bus.subscribeReliable(
    't',
    (d) => d.ack(),
    { maxIdleMs: 100, idleActivity: 'ack', ackTimeoutMs: 5000 },
  );
  for (let i = 0; i < 4; i += 1) {
    bus.publish('t', i);
    await flush(); // the ack lands in the same round
    await sleep(40);
  }
  assert.equal(bus.getStats().totalSubscribers, 1);
  assert.equal(events.length, 0);
  await sleep(220);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(events.length, 1);
  assert.equal(events[0].idleActivity, 'ack');
  assert.equal(bus.getStats().idleExpired, 1);
  void sub;
});

test("idleActivity 'ack': deliveries without acks do not re-arm", async () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable(
    't',
    () => {
      /* never acks */
    },
    { maxIdleMs: 80, idleActivity: 'ack', ackTimeoutMs: 5000 },
  );
  bus.publish('t', 'one');
  await flush();
  bus.publish('t', 'two');
  await flush();
  // Deliveries happened, but none settled via ack() — the window armed
  // at subscribe time still lapses on schedule.
  await sleep(160);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(bus.getStats().idleExpired, 1);
  void sub;
});

test("idleActivity 'deliver' on a reliable subscriber: clean returns re-arm, no ack needed", async () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable(
    't',
    () => {
      /* returns cleanly, never acks */
    },
    { maxIdleMs: 100, idleActivity: 'deliver', ackTimeoutMs: 5000 },
  );
  for (let i = 0; i < 4; i += 1) {
    bus.publish('t', i);
    await flush();
    await sleep(40);
  }
  assert.equal(bus.getStats().totalSubscribers, 1);
  await sleep(220);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(bus.getStats().idleExpired, 1);
  void sub;
});

test("idleActivity 'deliver': a throwing handler is a failed delivery, not activity", async () => {
  const bus = new EventBus();
  // The health probe swallows the throws so the test process survives;
  // the failures still must not re-arm the idle window.
  const sub = bus.subscribe(
    't',
    () => {
      throw new Error('downstream boom');
    },
    {
      maxIdleMs: 80,
      idleActivity: 'deliver',
      healthProbe: { maxConsecutiveFailures: 1000 },
    },
  );
  for (let i = 0; i < 3; i += 1) {
    bus.publish('t', i);
    await flush();
    await sleep(20);
  }
  // Three failed deliveries, zero qualifying activity: the window armed
  // at subscribe time lapses on schedule.
  await sleep(140);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(bus.getStats().idleExpired, 1);
  void sub;
});

test('reliable + DLQ: a throwing handler nacks — the failed delivery is not activity', async () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable(
    't',
    () => {
      throw new Error('poison');
    },
    {
      maxIdleMs: 80,
      idleActivity: 'deliver',
      ackTimeoutMs: 5000,
      deadLetter: { maxRedeliveries: 2 },
    },
  );
  bus.publish('t', 'x');
  await flush();
  // The handler threw and the wrapper nacked instead of recording
  // activity: the idle window keeps running from the subscribe moment.
  await sleep(160);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(bus.getStats().idleExpired, 1);
  void sub;
});

test('batched handler invocations count as deliver activity', async () => {
  const bus = new EventBus();
  const batches: unknown[][] = [];
  const sub = bus.subscribe('t', (batch) => batches.push(batch), {
    maxIdleMs: 120,
    batch: { maxSize: 2, maxWaitMs: 20 },
  });
  for (let round = 0; round < 3; round += 1) {
    bus.publish('t', round);
    bus.publish('t', round);
    await flush(); // maxSize hit: the batch delivers immediately
    await sleep(50);
  }
  // Three batch deliveries, each re-arming the 120ms window.
  assert.equal(batches.length, 3);
  assert.equal(bus.getStats().totalSubscribers, 1);
  await sleep(250);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(bus.getStats().idleExpired, 1);
  void sub;
});

test('shadow mirror deliveries count as activity: a busy mirror never expires', async () => {
  const bus = new EventBus();
  let mirrored = 0;
  const sub = bus.subscribe('t', () => {
    mirrored += 1;
  }, { shadow: true, maxIdleMs: 100 });
  for (let i = 0; i < 4; i += 1) {
    bus.publish('t', i);
    await flush();
    await sleep(40);
  }
  assert.ok(mirrored >= 4);
  assert.equal(bus.getStats().totalSubscribers, 1);
  await sleep(220);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(bus.getStats().idleExpired, 1);
  void sub;
});

test('invalid maxIdleMs / idleActivity values throw RangeError at subscribe time', () => {
  const bus = new EventBus();
  for (const bad of [0, -1, -100, 1.5, NaN, Infinity, -Infinity]) {
    assert.throws(
      () => bus.subscribe('t', noop, { maxIdleMs: bad }),
      RangeError,
      `maxIdleMs=${bad}`,
    );
  }
  for (const bad of ['bogus', '', 42, true, null]) {
    assert.throws(
      () => bus.subscribe('t', noop, { maxIdleMs: 100, idleActivity: bad as never }),
      RangeError,
      `idleActivity=${String(bad)}`,
    );
  }
  // idleActivity without maxIdleMs is meaningless — reject it too.
  assert.throws(() => bus.subscribe('t', noop, { idleActivity: 'ack' }), RangeError);
  // Nothing half-registered by the failed subscribes.
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(bus.getStats().idleExpired, 0);
});

test('group member idle expiry leaves through the group path and triggers rebalance', async () => {
  const leaves: string[] = [];
  const bus = new EventBus();
  const idle = bus.subscribeToGroup('g', 't', noop, { maxIdleMs: 60 });
  const watcher = bus.subscribeToGroup('g', 't', noop, {
    onRebalance: (e) => {
      if (e.trigger === 'leave') leaves.push(e.memberId);
    },
  });
  assert.equal(bus.getStats().totalSubscribers, 2);
  await sleep(160);
  // The idle member left the competing set through the normal group
  // leave path; the survivor observed the rebalance.
  assert.equal(bus.getStats().totalSubscribers, 1);
  assert.deepEqual(leaves, [idle.id]);
  assert.equal(bus.getStats().idleExpired, 1);
  // The group still works for the survivor.
  const got: unknown[] = [];
  watcher.unsubscribe();
  const m3 = bus.subscribeToGroup('g', 't', (m) => got.push(m.payload));
  bus.publish('t', 'x');
  await flush();
  assert.equal(got.length, 1);
  m3.unsubscribe();
});

test('lease and idle window coexist: whichever fires first wins (idle first)', async () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', noop, { leaseMs: 5000, maxIdleMs: 60 });
  await sleep(160);
  // The idle window fired first; the lease was cleared by the shared
  // unsubscribe path, so it can never fire afterwards.
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(bus.getStats().idleExpired, 1);
  assert.equal(bus.getStats().expiredLeases, 0);
  await sleep(120);
  assert.equal(bus.getStats().idleExpired, 1);
  assert.equal(bus.getStats().expiredLeases, 0);
  void sub;
});

test('lease and idle window coexist: whichever fires first wins (lease first)', async () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', noop, { leaseMs: 60, maxIdleMs: 5000 });
  await sleep(160);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(bus.getStats().expiredLeases, 1);
  assert.equal(bus.getStats().idleExpired, 0);
  await sleep(120);
  assert.equal(bus.getStats().expiredLeases, 1);
  assert.equal(bus.getStats().idleExpired, 0);
  void sub;
});

test('unsubscribe clears the idle window: no phantom expiry afterwards', async () => {
  const events: IdleExpiredEvent[] = [];
  const bus = new EventBus({ onIdleExpired: (e) => events.push(e) });
  const sub = bus.subscribe('t', noop, { maxIdleMs: 60 });
  sub.unsubscribe();
  await sleep(160);
  assert.equal(events.length, 0);
  assert.equal(bus.getStats().idleExpired, 0);
});

test('no maxIdleMs means no reclamation: fully backward compatible', async () => {
  const events: IdleExpiredEvent[] = [];
  const bus = new EventBus({ onIdleExpired: (e) => events.push(e) });
  const sub = bus.subscribe('t', noop);
  bus.publish('t', 'x');
  await flush();
  await sleep(120);
  assert.equal(bus.getStats().totalSubscribers, 1);
  assert.equal(events.length, 0);
  assert.equal(bus.getStats().idleExpired, 0);
  sub.unsubscribe();
});

test('a throwing onIdleExpired is isolated: unsubscribe still completes', async () => {
  const bus = new EventBus({
    onIdleExpired: () => {
      throw new Error('observer boom');
    },
  });
  const sub = bus.subscribe('t', noop, { maxIdleMs: 60 });
  await sleep(160);
  // The broken observer did not disturb the unsubscribe path or the count.
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(bus.getStats().idleExpired, 1);
  void sub;
});

test('idleExpired counts every expiry across subscriptions', async () => {
  const bus = new EventBus();
  const a = bus.subscribe('t', noop, { maxIdleMs: 40 });
  const b = bus.subscribe('t', noop, { maxIdleMs: 40 });
  const c = bus.subscribe('t', noop); // no idle window: never expires
  await sleep(160);
  assert.equal(bus.getStats().idleExpired, 2);
  assert.equal(bus.getStats().totalSubscribers, 1);
  c.unsubscribe();
  void a;
  void b;
});
