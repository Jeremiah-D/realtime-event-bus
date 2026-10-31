/**
 * Attached outbox relay revision (EB-302).
 *
 * `setOutboxRelay` retunes the attached relay live — the poll
 * interval, the per-cycle batch size, and the lag-probe cap — without
 * restarting the bus or re-attaching the relay. Only a bus with an
 * attached relay can be revised; illegal values fail closed with the
 * live tuning and the audit log untouched; an identical (or empty)
 * revision is a no-op with no audit. A new `batchSize` governs from
 * the next drain cycle and a new `probeLimit` from the next `getLag()`
 * probe; a revised `pollIntervalMs` re-arms the poll timer when the
 * relay is running (no extra drain) and takes effect at the next
 * `start()` when it is stopped. The cursor and the relayed count are
 * untouched by a revision.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { MemoryOutboxStore } from '../src/outbox.ts';

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

const flush = () => new Promise((resolve) => setImmediate(resolve));

/** Injected timer harness recording intervals and cleared handles. */
function makeTimers() {
  const intervals: number[] = [];
  const callbacks: Array<() => void> = [];
  const cleared: unknown[] = [];
  return {
    intervals,
    callbacks,
    cleared,
    setTimer: (fn: () => void, ms: number) => {
      intervals.push(ms);
      callbacks.push(fn);
      return callbacks.length;
    },
    clearTimer: (handle: unknown) => {
      cleared.push(handle);
    },
  };
}

describe('outbox relay revision (EB-302)', () => {
  it('setOutboxRelay fails closed when no relay is attached', () => {
    const bus = new EventBus();
    assert.throws(() => bus.setOutboxRelay({ batchSize: 5 }), RangeError);
    assert.throws(() => bus.setOutboxRelay({ pollIntervalMs: 25 }), RangeError);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'outbox_relay_revised' }).length,
      0,
    );
  });

  it('setOutboxRelay rejects illegal values and leaves the live tuning untouched', () => {
    const clock = makeClock();
    const bus = new EventBus({ now: clock.now });
    bus.subscribe('t', () => {});
    const store = new MemoryOutboxStore(clock.now);
    for (let i = 0; i < 5; i++) store.append({ topic: 't', payload: i });
    const relay = bus.attachOutboxRelay(store, {
      batchSize: 2,
      probeLimit: 2,
      pollIntervalMs: 0,
    });
    assert.deepEqual(relay.drain(), { relayed: 2 }); // cursor 2
    const bad: Array<unknown> = [
      { batchSize: 0 },
      { batchSize: -1 },
      { batchSize: 1.5 },
      { probeLimit: 0 },
      { probeLimit: 2.5 },
      { pollIntervalMs: Number.NaN },
      { pollIntervalMs: 'fast' },
      'nope',
      42,
      ['x'],
    ];
    for (const revision of bad) {
      assert.throws(
        () => bus.setOutboxRelay(revision as never),
        RangeError,
        `expected RangeError for ${JSON.stringify(revision)}`,
      );
    }
    // The failed calls changed nothing: the probe still caps at 2 and
    // the next drain still commits exactly 2.
    assert.equal(relay.getLag().pending, 2);
    assert.deepEqual(relay.drain(), { relayed: 2 }); // cursor 4
    assert.equal(relay.getLag().pending, 1);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'outbox_relay_revised' }).length,
      0,
    );
  });

  it('setOutboxRelay with identical values or an empty revision is a no-op with no audit', () => {
    const clock = makeClock();
    const bus = new EventBus({ now: clock.now });
    bus.subscribe('t', () => {});
    const store = new MemoryOutboxStore(clock.now);
    const relay = bus.attachOutboxRelay(store, {
      batchSize: 2,
      probeLimit: 10,
      pollIntervalMs: 0,
    });
    bus.setOutboxRelay({ batchSize: 2 });
    bus.setOutboxRelay({ probeLimit: 10 });
    bus.setOutboxRelay({ pollIntervalMs: 0 });
    bus.setOutboxRelay({ batchSize: 2, probeLimit: 10, pollIntervalMs: 0 });
    bus.setOutboxRelay({});
    bus.setOutboxRelay(null as never);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'outbox_relay_revised' }).length,
      0,
    );
    // The untouched tuning still governs: 2 per drain.
    for (let i = 0; i < 5; i++) store.append({ topic: 't', payload: i });
    assert.deepEqual(relay.drain(), { relayed: 2 });
  });

  it('batchSize revision governs from the next drain; cursor and relayed are untouched', async () => {
    const clock = makeClock();
    const bus = new EventBus({ now: clock.now });
    const seen: unknown[] = [];
    bus.subscribe('t', (msg) => seen.push(msg.payload));
    const store = new MemoryOutboxStore(clock.now);
    for (let i = 0; i < 5; i++) store.append({ topic: 't', payload: i });
    const relay = bus.attachOutboxRelay(store, {
      batchSize: 2,
      probeLimit: 10,
      pollIntervalMs: 0,
    });
    assert.deepEqual(relay.drain(), { relayed: 2 });
    bus.setOutboxRelay({ batchSize: 4 });
    // The revision itself moved nothing: cursor 2, relayed 2.
    assert.deepEqual(relay.getLag(), { pending: 3, oldestMs: 0, cursor: 2, relayed: 2 });
    assert.deepEqual(relay.drain(), { relayed: 3 });
    await flush();
    assert.deepEqual(seen, [0, 1, 2, 3, 4]);
    const audit = bus.getSubscriptionAudit({ action: 'outbox_relay_revised' });
    assert.equal(audit.length, 1);
    assert.deepEqual(audit[0].detail, {
      pollIntervalMs: 0,
      oldPollIntervalMs: 0,
      batchSize: 4,
      oldBatchSize: 2,
      probeLimit: 10,
      oldProbeLimit: 10,
    });
  });

  it('probeLimit revision governs from the next getLag probe', () => {
    const clock = makeClock();
    const bus = new EventBus({ now: clock.now });
    bus.subscribe('t', () => {});
    const store = new MemoryOutboxStore(clock.now);
    for (let i = 0; i < 5; i++) store.append({ topic: 't', payload: i });
    const relay = bus.attachOutboxRelay(store, {
      batchSize: 100,
      probeLimit: 2,
      pollIntervalMs: 0,
    });
    assert.equal(relay.getLag().pending, 2);
    bus.setOutboxRelay({ probeLimit: 10 });
    assert.equal(relay.getLag().pending, 5);
    // Draining itself was never capped by the probe.
    assert.deepEqual(relay.drain(), { relayed: 5 });
    assert.equal(relay.getLag().pending, 0);
  });

  it('pollIntervalMs revision re-arms the timer when running, without an extra drain', async () => {
    const clock = makeClock();
    const bus = new EventBus({ now: clock.now });
    const seen: unknown[] = [];
    bus.subscribe('t', (msg) => seen.push(msg.payload));
    const store = new MemoryOutboxStore(clock.now);
    const timers = makeTimers();
    const relay = bus.attachOutboxRelay(store, {
      pollIntervalMs: 50,
      setTimer: timers.setTimer,
      clearTimer: timers.clearTimer,
    });
    relay.start(); // one immediate (idle) drain, timer armed at 50ms
    assert.deepEqual(timers.intervals, [50]);
    // A revision that leaves the interval alone does not touch the timer.
    bus.setOutboxRelay({ batchSize: 5 });
    assert.deepEqual(timers.intervals, [50]);
    assert.deepEqual(timers.cleared, []);
    store.append({ topic: 't', payload: 1 });
    bus.setOutboxRelay({ pollIntervalMs: 25 });
    // Re-armed: the old timer cleared, a new one armed at 25ms — and
    // no drain fired: the record is still pending.
    assert.deepEqual(timers.intervals, [50, 25]);
    assert.deepEqual(timers.cleared, [1]);
    assert.equal(relay.getLag().pending, 1);
    assert.equal(relay.getLag().relayed, 0);
    timers.callbacks[1](); // the new poll tick
    await flush();
    assert.deepEqual(seen, [1]);
    assert.equal(relay.getLag().relayed, 1);
    relay.stop();
    assert.deepEqual(timers.cleared, [1, 2]);
  });

  it('pollIntervalMs 0 disables the timer while running; a later revision re-arms it', async () => {
    const clock = makeClock();
    const bus = new EventBus({ now: clock.now });
    const seen: unknown[] = [];
    bus.subscribe('t', (msg) => seen.push(msg.payload));
    const store = new MemoryOutboxStore(clock.now);
    const timers = makeTimers();
    const relay = bus.attachOutboxRelay(store, {
      pollIntervalMs: 50,
      setTimer: timers.setTimer,
      clearTimer: timers.clearTimer,
    });
    relay.start();
    assert.deepEqual(timers.intervals, [50]);
    bus.setOutboxRelay({ pollIntervalMs: 0 });
    assert.deepEqual(timers.cleared, [1]);
    assert.deepEqual(timers.intervals, [50]); // no new timer armed
    store.append({ topic: 't', payload: 1 });
    // With the timer disabled the relay only drains on explicit calls.
    assert.equal(relay.getLag().pending, 1);
    bus.setOutboxRelay({ pollIntervalMs: 25 });
    assert.deepEqual(timers.intervals, [50, 25]);
    timers.callbacks[1]();
    await flush();
    assert.deepEqual(seen, [1]);
    relay.stop();
  });

  it('pollIntervalMs revision while stopped takes effect at the next start', () => {
    const clock = makeClock();
    const bus = new EventBus({ now: clock.now });
    bus.subscribe('t', () => {});
    const store = new MemoryOutboxStore(clock.now);
    const timers = makeTimers();
    const relay = bus.attachOutboxRelay(store, {
      pollIntervalMs: 50,
      setTimer: timers.setTimer,
      clearTimer: timers.clearTimer,
    });
    bus.setOutboxRelay({ pollIntervalMs: 25 });
    // Stopped: no timer touched at all.
    assert.deepEqual(timers.intervals, []);
    assert.deepEqual(timers.cleared, []);
    relay.start();
    assert.deepEqual(timers.intervals, [25]);
    relay.stop();
    assert.deepEqual(timers.cleared, [1]);
  });
});
