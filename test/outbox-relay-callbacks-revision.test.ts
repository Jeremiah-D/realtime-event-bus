/**
 * Attached outbox relay observation-hook revision (EB-452).
 *
 * `setOutboxRelayCallbacks` swaps the attached relay's `onRelayError`
 * and/or `onRelayed` hooks live — without restarting the bus or
 * re-attaching the relay. Only a bus with an attached relay can be
 * revised; illegal values fail closed with the live hooks and the
 * audit log untouched; an identical (or empty) revision is a no-op
 * with no audit. A new hook governs from the very next drain (a
 * stopped relay picks it up at the next `start()`); the cursor, the
 * relayed count, and the poll timer are untouched by a revision.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { MemoryOutboxStore, type OutboxRecord } from '../src/outbox.ts';

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

describe('outbox relay callbacks revision (EB-452)', () => {
  it('setOutboxRelayCallbacks fails closed when no relay is attached', () => {
    const bus = new EventBus();
    const hook = () => {};
    assert.throws(() => bus.setOutboxRelayCallbacks({ onRelayed: hook }), RangeError);
    assert.throws(() => bus.setOutboxRelayCallbacks({ onRelayError: hook }), RangeError);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'outbox_relay_callbacks_revised' }).length,
      0,
    );
  });

  it('setOutboxRelayCallbacks rejects a bad actor', () => {
    const clock = makeClock();
    const bus = new EventBus({ now: clock.now });
    const store = new MemoryOutboxStore(clock.now);
    bus.attachOutboxRelay(store, { pollIntervalMs: 0 });
    assert.throws(
      () => bus.setOutboxRelayCallbacks({ onRelayed: () => {} }, ''),
      RangeError,
    );
    assert.equal(
      bus.getSubscriptionAudit({ action: 'outbox_relay_callbacks_revised' }).length,
      0,
    );
  });

  it('a replaced error hook reports the next failed drain, the old one never fires again', async () => {
    const clock = makeClock();
    const bus = new EventBus({ now: clock.now });
    bus.setTopicSchema('orders.created', (payload) => (payload as { ok?: boolean }).ok === true);
    bus.subscribe('orders.created', () => {});
    const store = new MemoryOutboxStore(clock.now);
    store.append({ topic: 'orders.created', payload: { ok: true } });
    store.append({ topic: 'orders.created', payload: { ok: false } }); // poison

    const oldErrors: unknown[] = [];
    const newErrors: Array<{ err: unknown; records: OutboxRecord[] }> = [];
    const relay = bus.attachOutboxRelay(store, {
      pollIntervalMs: 0,
      onRelayError: (err) => oldErrors.push(err),
    });
    const first = relay.drain();
    assert.equal(first.relayed, 0);
    assert.equal(oldErrors.length, 1);

    const newHook = (err: unknown, records: OutboxRecord[]) =>
      newErrors.push({ err, records });
    bus.setOutboxRelayCallbacks({ onRelayError: newHook });
    const second = relay.drain();
    assert.equal(second.relayed, 0);
    assert.ok(second.rejected !== undefined);
    assert.equal(second.rejected.reason, 'schema');
    // Only the new hook fired, with the poison batch; the cursor never moved.
    assert.equal(oldErrors.length, 1);
    assert.equal(newErrors.length, 1);
    assert.equal(newErrors[0].records.length, 2);
    assert.equal(relay.getLag().cursor, 0);
    await flush();
    relay.stop();
  });

  it('a replaced success hook reports the next committed drain, the old one never fires again', () => {
    const clock = makeClock();
    const bus = new EventBus({ now: clock.now });
    bus.subscribe('pay', () => {});
    const store = new MemoryOutboxStore(clock.now);
    store.append({ topic: 'pay', payload: { id: 1 } });

    const oldRelayed: unknown[] = [];
    const newRelayed: OutboxRecord[][] = [];
    const relay = bus.attachOutboxRelay(store, {
      pollIntervalMs: 0,
      onRelayed: (records) => oldRelayed.push(records),
    });
    bus.setOutboxRelayCallbacks({
      onRelayed: (records) => newRelayed.push(records),
    });
    const result = relay.drain();
    assert.equal(result.relayed, 1);
    assert.equal(oldRelayed.length, 0);
    assert.equal(newRelayed.length, 1);
    assert.deepEqual(
      newRelayed[0].map((r) => r.seq),
      [1],
    );
    relay.stop();
  });

  it('null clears only its own hook; the other hook keeps firing', () => {
    const clock = makeClock();
    const bus = new EventBus({ now: clock.now });
    bus.subscribe('pay', () => {});
    const store = new MemoryOutboxStore(clock.now);
    store.append({ topic: 'pay', payload: { id: 1 } });

    const errors: unknown[] = [];
    const relayed: OutboxRecord[][] = [];
    const relay = bus.attachOutboxRelay(store, {
      pollIntervalMs: 0,
      onRelayError: (err) => errors.push(err),
      onRelayed: (records) => relayed.push(records),
    });
    bus.setOutboxRelayCallbacks({ onRelayError: null });
    const result = relay.drain();
    assert.equal(result.relayed, 1);
    // The cleared error hook is gone; the untouched success hook still fires.
    assert.equal(relayed.length, 1);
    assert.deepEqual(
      bus.getSubscriptionAudit({ action: 'outbox_relay_callbacks_revised' }).map(
        (e) => e.detail,
      ),
      [{ onRelayError: 'cleared', onRelayed: 'unchanged' }],
    );
    relay.stop();
  });

  it('re-installing the live hook and an empty patch are no-ops with no audit', () => {
    const clock = makeClock();
    const bus = new EventBus({ now: clock.now });
    const store = new MemoryOutboxStore(clock.now);
    const hook = () => {};
    bus.attachOutboxRelay(store, { pollIntervalMs: 0, onRelayed: hook });
    // Same reference, clearing a hook that was never set, and `{}`.
    bus.setOutboxRelayCallbacks({ onRelayed: hook });
    bus.setOutboxRelayCallbacks({ onRelayError: null });
    bus.setOutboxRelayCallbacks({});
    assert.equal(
      bus.getSubscriptionAudit({ action: 'outbox_relay_callbacks_revised' }).length,
      0,
    );
  });

  it('an illegal hook value fails closed with the live hooks untouched', () => {
    const clock = makeClock();
    const bus = new EventBus({ now: clock.now });
    bus.subscribe('pay', () => {});
    const store = new MemoryOutboxStore(clock.now);
    store.append({ topic: 'pay', payload: { id: 1 } });

    const relayed: OutboxRecord[][] = [];
    const relay = bus.attachOutboxRelay(store, {
      pollIntervalMs: 0,
      onRelayed: (records) => relayed.push(records),
    });
    const bad: Array<unknown> = [
      { onRelayed: 'not-a-function' },
      { onRelayed: 42 },
      { onRelayError: {} },
      { onRelayError: () => {}, onRelayed: true },
    ];
    for (const patch of bad) {
      assert.throws(
        () =>
          bus.setOutboxRelayCallbacks(
            patch as { onRelayed?: never; onRelayError?: never },
          ),
        RangeError,
      );
    }
    assert.throws(() => bus.setOutboxRelayCallbacks('nope' as never), RangeError);
    assert.throws(() => bus.setOutboxRelayCallbacks([] as never), RangeError);
    // The live success hook survived every failed revision.
    const result = relay.drain();
    assert.equal(result.relayed, 1);
    assert.equal(relayed.length, 1);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'outbox_relay_callbacks_revised' }).length,
      0,
    );
    relay.stop();
  });

  it('a throwing replacement hook is swallowed and the relay keeps draining', async () => {
    const clock = makeClock();
    const bus = new EventBus({ now: clock.now });
    bus.setTopicSchema('orders.created', (payload) => (payload as { ok?: boolean }).ok === true);
    bus.subscribe('orders.created', () => {});
    bus.subscribe('pay', () => {});
    const store = new MemoryOutboxStore(clock.now);
    store.append({ topic: 'orders.created', payload: { ok: false } }); // poison
    store.append({ topic: 'pay', payload: { id: 1 } });

    const relay = bus.attachOutboxRelay(store, { pollIntervalMs: 0 });
    bus.setOutboxRelayCallbacks({
      onRelayError: () => {
        throw new Error('pager exploded');
      },
    });
    // The throwing error hook must not kill the drain: the poison batch
    // still reports (and is swallowed) and the cursor stays put.
    const failed = relay.drain();
    assert.equal(failed.relayed, 0);
    assert.equal(relay.getLag().cursor, 0);

    // The operator clears the poison record; a throwing success hook
    // must not break the commit either.
    store.ackCursor(1);
    const calls: OutboxRecord[][] = [];
    bus.setOutboxRelayCallbacks({
      onRelayed: (records) => {
        calls.push(records);
        throw new Error('metrics exploded');
      },
    });
    const ok = relay.drain();
    assert.equal(ok.relayed, 1);
    assert.equal(relay.getLag().cursor, 2);
    assert.equal(relay.getLag().relayed, 1);
    assert.equal(calls.length, 1);
    await flush();
    relay.stop();
  });

  it('the audit detail records only set/cleared/unchanged, never the hook functions', () => {
    const clock = makeClock();
    const bus = new EventBus({ now: clock.now });
    const store = new MemoryOutboxStore(clock.now);
    const errorHook = () => {};
    const relayedHook = () => {};
    bus.attachOutboxRelay(store, { pollIntervalMs: 0 });
    bus.setOutboxRelayCallbacks(
      { onRelayError: errorHook, onRelayed: relayedHook },
      'operator',
    );
    const events = bus.getSubscriptionAudit({ action: 'outbox_relay_callbacks_revised' });
    assert.equal(events.length, 1);
    assert.equal(events[0].actor, 'operator');
    assert.deepEqual(events[0].detail, { onRelayError: 'set', onRelayed: 'set' });
    // No function reference anywhere in the audit record.
    assert.equal(JSON.stringify(events[0]).includes('function'), false);
    assert.equal(JSON.stringify(events[0].detail).includes('=>'), false);
  });

  it('a revision leaves the cursor, the relayed count, and the poll timer untouched', () => {
    const clock = makeClock();
    const timers = makeTimers();
    const bus = new EventBus({ now: clock.now });
    bus.subscribe('pay', () => {});
    const store = new MemoryOutboxStore(clock.now);
    store.append({ topic: 'pay', payload: { id: 1 } });
    store.append({ topic: 'pay', payload: { id: 2 } });
    const relay = bus.attachOutboxRelay(store, {
      pollIntervalMs: 100,
      batchSize: 1,
      setTimer: timers.setTimer,
      clearTimer: timers.clearTimer,
    });
    relay.start();
    assert.deepEqual(timers.intervals, [100]);
    assert.equal(relay.getLag().cursor, 1);
    assert.equal(relay.getLag().relayed, 1);

    bus.setOutboxRelayCallbacks({
      onRelayError: () => {},
      onRelayed: () => {},
    });
    // No timer was cleared or re-armed; the cursor and count are untouched.
    assert.deepEqual(timers.intervals, [100]);
    assert.deepEqual(timers.cleared, []);
    assert.equal(relay.getLag().cursor, 1);
    assert.equal(relay.getLag().relayed, 1);
    // And the next drain still works with the new hooks in place.
    assert.equal(relay.drain().relayed, 1);
    assert.equal(relay.getLag().cursor, 2);
    relay.stop();
  });

  it('a stopped relay can be revised and the next start() takes effect', () => {
    const clock = makeClock();
    const bus = new EventBus({ now: clock.now });
    bus.subscribe('pay', () => {});
    const store = new MemoryOutboxStore(clock.now);
    store.append({ topic: 'pay', payload: { id: 1 } });

    const relayed: OutboxRecord[][] = [];
    const relay = bus.attachOutboxRelay(store, { pollIntervalMs: 0 });
    // Never started: revise while stopped, then start — the immediate
    // drain inside start() must report on the new hook.
    bus.setOutboxRelayCallbacks({
      onRelayed: (records) => relayed.push(records),
    });
    relay.start();
    assert.equal(relayed.length, 1);
    assert.deepEqual(
      relayed[0].map((r) => r.seq),
      [1],
    );
    assert.equal(relay.getLag().relayed, 1);
    relay.stop();
  });
});
