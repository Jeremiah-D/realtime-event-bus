import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';
import {
  MemoryOutboxStore,
  OutboxRelay,
  type OutboxRecord,
  type OutboxStore,
} from '../src/outbox.ts';

const flush = () => new Promise((resolve) => setImmediate(resolve));
const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-outbox-'));

/** Injectable clock shared by the bus, the store and the relay. */
function makeClock(startMs = 1_000_000) {
  let nowMs = startMs;
  return {
    now: () => nowMs,
    advance: (ms: number) => {
      nowMs += ms;
    },
  };
}

// ---------------------------------------------------------------------------
// MemoryOutboxStore
// ---------------------------------------------------------------------------

test('store: append assigns increasing seqs, readSince windows, ackCursor prunes', () => {
  const clock = makeClock();
  const store = new MemoryOutboxStore(clock.now);
  const s1 = store.append({ topic: 'a', payload: { n: 1 } });
  const s2 = store.append({ topic: 'b', payload: { n: 2 } });
  const s3 = store.append({ topic: 'a', payload: { n: 3 } });
  assert.deepEqual([s1, s2, s3], [1, 2, 3]);

  const all = store.readSince(0, 10);
  assert.deepEqual(all.map((r) => r.seq), [1, 2, 3]);
  assert.deepEqual(
    store.readSince(1, 1).map((r) => r.seq),
    [2],
  );
  assert.deepEqual(store.readSince(0, 0), []);
  // `at` defaults to the store clock.
  assert.equal(all[0].at, 1_000_000);

  store.ackCursor(2);
  assert.deepEqual(store.readSince(2, 10).map((r) => r.seq), [3]);
  // Acked records are pruned: the backlog stays bounded.
  assert.deepEqual(store.readSince(0, 10).map((r) => r.seq), [3]);
  assert.deepEqual(store.recover(), { cursor: 2 });

  assert.throws(() => store.ackCursor(1), RangeError); // never backwards
  assert.throws(() => store.append({ topic: '', payload: 1 }), TypeError);
});

test('store: constructor validates relay options', () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const store = new MemoryOutboxStore(clock.now);
  assert.throws(() => new OutboxRelay(bus, store, { batchSize: 0 }), RangeError);
  assert.throws(() => new OutboxRelay(bus, store, { probeLimit: -1 }), RangeError);
  assert.throws(() => new OutboxRelay(bus, store, { pollIntervalMs: NaN }), RangeError);
  assert.throws(() => new OutboxRelay(undefined as never, store), TypeError);
  assert.throws(() => new OutboxRelay(bus, undefined as never), TypeError);
  bus.detachOutboxRelay(); // no relay attached: detach is a safe no-op
});

// ---------------------------------------------------------------------------
// Relay: basic drain
// ---------------------------------------------------------------------------

test('relay: one drain commits a multi-topic batch atomically via publishAtomic', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const seenA: unknown[] = [];
  const seenB: unknown[] = [];
  bus.subscribe('orders.created', (msg) => seenA.push(msg.payload));
  bus.subscribe('inventory.reserved', (msg) => seenB.push(msg.payload));

  const store = new MemoryOutboxStore(clock.now);
  store.append({ topic: 'orders.created', payload: { order: 'o1' } });
  store.append({ topic: 'inventory.reserved', payload: { order: 'o1', sku: 's1' } });

  const relay = bus.attachOutboxRelay(store, { pollIntervalMs: 0 });
  relay.start();
  await flush();

  assert.deepEqual(seenA, [{ order: 'o1' }]);
  assert.deepEqual(seenB, [{ order: 'o1', sku: 's1' }]);
  const stats = bus.getStats();
  assert.equal(stats.outboxPending, 0);
  assert.equal(stats.outboxLagMs, 0);
  assert.equal(stats.outboxCursor, 2);
  assert.equal(stats.outboxRelayed, 2);

  // Idempotent drain when idle.
  const idle = relay.drain();
  assert.deepEqual(idle, { relayed: 0 });
  relay.stop();
});

test('relay: attaching twice throws; detach stops the timer', () => {
  const bus = new EventBus();
  const store = new MemoryOutboxStore();
  let cleared = 0;
  const timers: Array<() => void> = [];
  const relay = bus.attachOutboxRelay(store, {
    pollIntervalMs: 50,
    setTimer: (fn) => {
      timers.push(fn);
      return timers.length;
    },
    clearTimer: () => {
      cleared += 1;
    },
  });
  assert.throws(() => bus.attachOutboxRelay(new MemoryOutboxStore()), Error);
  relay.start();
  assert.equal(timers.length, 1);
  bus.detachOutboxRelay();
  assert.equal(cleared, 1);
  // Re-attaching after detach works.
  bus.attachOutboxRelay(new MemoryOutboxStore(), { pollIntervalMs: 0 });
});

test('relay: timer fires drains on schedule with injected timers', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const seen: unknown[] = [];
  bus.subscribe('t', (msg) => seen.push(msg.payload));
  const store = new MemoryOutboxStore(clock.now);

  const callbacks: Array<() => void> = [];
  let cleared = 0;
  const relay = new OutboxRelay(bus, store, {
    pollIntervalMs: 25,
    now: clock.now,
    setTimer: (fn) => {
      callbacks.push(fn);
      return { unref() {} };
    },
    clearTimer: () => {
      cleared += 1;
    },
  });
  relay.start(); // one immediate drain (idle)
  store.append({ topic: 't', payload: 1 });
  assert.equal(callbacks.length, 1);
  callbacks[0](); // the poll tick
  await flush();
  assert.deepEqual(seen, [1]);
  relay.stop();
  assert.equal(cleared, 1);
});

// ---------------------------------------------------------------------------
// Relay: poison records block, cursor never moves past them
// ---------------------------------------------------------------------------

test('relay: a rejected batch advances nothing and reports onRelayError', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicSchema('orders.created', (payload) => (payload as { ok?: boolean }).ok === true);
  const seen: unknown[] = [];
  bus.subscribe('orders.created', (msg) => seen.push(msg.payload));

  const store = new MemoryOutboxStore(clock.now);
  store.append({ topic: 'orders.created', payload: { ok: true } });
  store.append({ topic: 'orders.created', payload: { ok: false } }); // poison

  const errors: Array<{ err: unknown; records: OutboxRecord[] }> = [];
  const relay = bus.attachOutboxRelay(store, {
    pollIntervalMs: 0,
    onRelayError: (err, records) => errors.push({ err, records }),
  });
  const result = relay.drain();
  await flush();

  // Atomicity: the good entry was NOT committed — all or nothing.
  assert.equal(result.relayed, 0);
  assert.ok(result.rejected !== undefined);
  assert.equal(result.rejected.reason, 'schema');
  assert.deepEqual(seen, []);
  assert.equal(errors.length, 1);
  assert.equal(errors[0].records.length, 2);
  // The cursor stays on the poison record; lag is visible.
  clock.advance(5000);
  const lag = relay.getLag();
  assert.equal(lag.pending, 2);
  assert.equal(lag.oldestMs, 5000);
  assert.equal(lag.cursor, 0);
  const stats = bus.getStats();
  assert.equal(stats.outboxPending, 2);
  assert.equal(stats.outboxLagMs, 5000);

  // The operator clears the poison record by advancing the cursor, then
  // the relay resumes on the next drain.
  store.ackCursor(2);
  const after = relay.drain();
  assert.equal(after.relayed, 0); // nothing left past the cursor
  assert.equal(relay.getLag().pending, 0);
  relay.stop();
});

// ---------------------------------------------------------------------------
// Relay: crash between commit and ack replays, EB-27 dedup suppresses
// ---------------------------------------------------------------------------

test('relay: replay after a lost ack is suppressed by the outbox:<seq> dedup key', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const seen: unknown[] = [];
  // The EB-27 backstop is subscriber opt-in: the relay mints the stable
  // identity, the subscriber's dedup window does the suppressing.
  bus.subscribe('pay', (msg) => seen.push(msg.payload), { deduplicateMessages: true });

  // A store whose ackCursor throws once: the crash between a committed
  // publishAtomic and the durable cursor advance.
  const store = new MemoryOutboxStore(clock.now);
  let ackThrows = 1;
  const flaky: OutboxStore = {
    append: (r) => store.append(r),
    readSince: (c, l) => store.readSince(c, l),
    ackCursor: (c) => {
      if (ackThrows > 0) {
        ackThrows -= 1;
        throw new Error('crash: ack never landed');
      }
      store.ackCursor(c);
    },
    recover: () => store.recover(),
  };

  store.append({ topic: 'pay', payload: { id: 1 } });
  store.append({ topic: 'pay', payload: { id: 2 } });

  const errors: unknown[] = [];
  // NOTE: no start() here — start() performs an immediate drain, and this
  // test needs exactly one drain with the ack failing.
  const relay = bus.attachOutboxRelay(flaky, {
    pollIntervalMs: 0,
    onRelayError: (err) => errors.push(err),
  });
  const first = relay.drain();
  await flush();
  // The batch committed (subscribers saw it) but the ack was lost.
  assert.deepEqual(seen, [{ id: 1 }, { id: 2 }]);
  assert.equal(first.relayed, 0);
  assert.equal(errors.length, 1);
  assert.equal(relay.getLag().cursor, 0);

  // The ack path is healthy again: the next drain replays the same two
  // records with the same `outbox:<seq>` messageIds, and the bus's
  // publish-side idempotency (EB-27) suppresses the duplicates.
  const second = relay.drain();
  await flush();
  assert.equal(second.relayed, 2);
  assert.deepEqual(seen, [{ id: 1 }, { id: 2 }]); // no double delivery
  assert.equal(relay.getLag().cursor, 2);
  relay.stop();
});

test('relay: full crash recovery — a new relay resumes from the durable cursor', async () => {
  const dir = freshDir();
  const clock = makeClock();
  const store = new MemoryOutboxStore(clock.now);

  // Bus 1 with a durable log and a durable subscriber dedup window.
  const bus1 = new EventBus({ now: clock.now, durableLogDir: dir });
  const seen1: unknown[] = [];
  bus1.subscribe('pay', (msg) => seen1.push(msg.payload), {
    deduplicateMessages: { consumerId: 'relay-e2e' },
  });
  store.append({ topic: 'pay', payload: { id: 1 } });
  const relay1 = bus1.attachOutboxRelay(store, { pollIntervalMs: 0 });
  relay1.start();
  await flush();
  assert.deepEqual(seen1, [{ id: 1 }]);
  assert.equal(store.recover().cursor, 1);

  // More business writes land while the process is "down": one commits
  // and acks, then the process dies mid-ack on the next batch.
  store.append({ topic: 'pay', payload: { id: 2 } });
  let ackThrows = 1;
  const flaky: OutboxStore = {
    append: (r) => store.append(r),
    readSince: (c, l) => store.readSince(c, l),
    ackCursor: (c) => {
      if (ackThrows > 0) {
        ackThrows -= 1;
        throw new Error('crash before ack');
      }
      store.ackCursor(c);
    },
    recover: () => store.recover(),
  };
  bus1.detachOutboxRelay();
  const relayCrash = new OutboxRelay(bus1, flaky, { pollIntervalMs: 0, now: clock.now });
  relayCrash.drain(); // commits id:2, ack lost
  await flush();
  assert.deepEqual(seen1, [{ id: 1 }, { id: 2 }]);

  // Restart: fresh bus over the same durable-log dir, same consumer
  // identity, fresh relay over the same store. recover() resumes at the
  // durable cursor (1); the replayed id:2 is suppressed by the
  // rehydrated dedup window.
  const bus2 = new EventBus({ now: clock.now, durableLogDir: dir });
  const seen2: unknown[] = [];
  bus2.subscribe('pay', (msg) => seen2.push(msg.payload), {
    deduplicateMessages: { consumerId: 'relay-e2e' },
  });
  const relay2 = bus2.attachOutboxRelay(store, { pollIntervalMs: 0 });
  relay2.start();
  await flush();
  assert.deepEqual(seen2, []); // replayed id:2 suppressed
  assert.equal(store.recover().cursor, 2);

  // New business after the restart flows normally.
  store.append({ topic: 'pay', payload: { id: 3 } });
  relay2.drain();
  await flush();
  assert.deepEqual(seen2, [{ id: 3 }]);
  bus2.detachOutboxRelay();
});

// ---------------------------------------------------------------------------
// Relay: stats + Prometheus
// ---------------------------------------------------------------------------

test('relay: lag flows into getStats and the Prometheus exposition', () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const before = bus.getStats();
  assert.equal(before.outboxPending, 0);
  assert.equal(before.outboxLagMs, 0);
  assert.equal(before.outboxCursor, 0);
  assert.equal(before.outboxRelayed, 0);

  const store = new MemoryOutboxStore(clock.now);
  store.append({ topic: 'a', payload: 1 });
  clock.advance(2500);
  store.append({ topic: 'a', payload: 2 });
  bus.attachOutboxRelay(store, { pollIntervalMs: 0 });

  const stats = bus.getStats();
  assert.equal(stats.outboxPending, 2);
  assert.equal(stats.outboxLagMs, 2500);
  assert.equal(stats.outboxCursor, 0);
  assert.equal(stats.outboxRelayed, 0);

  const text = renderPrometheus(stats);
  assert.ok(text.includes('eventbus_outbox_pending 2'), text);
  assert.ok(text.includes('eventbus_outbox_lag_ms 2500'), text);
  assert.ok(text.includes('eventbus_outbox_relayed_total 0'), text);

  // renderPrometheus stays total on a stats object that predates the
  // fields (hand-built fixtures, older snapshots).
  const legacy = renderPrometheus({ ...stats, outboxPending: undefined } as never);
  assert.ok(legacy.includes('eventbus_outbox_pending 0'), legacy);
});

test('relay: key, traceparent and the dedup identity are forwarded to publishAtomic', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const seen: unknown[] = [];
  bus.subscribe('k', (msg) => seen.push(msg.payload));

  // Spy on the atomic entry point: the relay's mapping (key, traceparent,
  // messageId) is an admission-level contract, not envelope-visible —
  // BusMessage carries no key/traceparent fields.
  const captured: Array<Parameters<EventBus['publishAtomic']>[0]> = [];
  const original = bus.publishAtomic.bind(bus);
  bus.publishAtomic = ((entries: Parameters<EventBus['publishAtomic']>[0]) => {
    captured.push(entries);
    return original(entries);
  }) as EventBus['publishAtomic'];

  const store = new MemoryOutboxStore(clock.now);
  store.append({
    topic: 'k',
    payload: { v: 1 },
    key: 'order-7',
    headers: { traceparent: '00-abc-def-01', 'x-other': 'kept-in-store' },
  });
  const relay = bus.attachOutboxRelay(store, { pollIntervalMs: 0 });
  relay.drain();
  await flush();

  assert.equal(captured.length, 1);
  assert.equal(captured[0].length, 1);
  assert.equal(captured[0][0].key, 'order-7');
  assert.equal(captured[0][0].traceparent, '00-abc-def-01');
  // The stable replay guard: the outbox seq as the EB-27 dedup identity.
  assert.equal(captured[0][0].messageId, 'outbox:1');
  assert.deepEqual(seen, [{ v: 1 }]);
  relay.stop();
});

test('relay: batchSize caps one drain cycle; the next drain continues', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const seen: unknown[] = [];
  bus.subscribe('t', (msg) => seen.push(msg.payload));
  const store = new MemoryOutboxStore(clock.now);
  for (let i = 1; i <= 5; i++) store.append({ topic: 't', payload: i });

  const relay = bus.attachOutboxRelay(store, { pollIntervalMs: 0, batchSize: 2 });
  assert.deepEqual(relay.drain(), { relayed: 2 });
  assert.deepEqual(relay.drain(), { relayed: 2 });
  assert.deepEqual(relay.drain(), { relayed: 1 });
  await flush();
  assert.deepEqual(seen, [1, 2, 3, 4, 5]);
  assert.equal(relay.getLag().cursor, 5);
  relay.stop();
});
