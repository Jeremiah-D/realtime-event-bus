import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';
import {
  resolveExpiringSoonOptions,
  type ExpiringSoonEvent,
} from '../src/expiring.ts';

/**
 * TTL expiring-soon warnings (EB-110): `SubscribeOptions.expiringSoon`
 * warns once when the queue head's remaining TTL enters the warning
 * window, re-arms when the queue drains, and never disturbs delivery.
 *
 * Determinism trick: `bus.publish` enqueues synchronously and only
 * schedules the drain as a microtask, so several publishes in a row build
 * a real backlog synchronously — the warning is evaluated at each
 * enqueue, exactly like the production path.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

test('no warning without a TTL on the head message', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: ExpiringSoonEvent[] = [];
  bus.subscribe('t', () => {}, {
    expiringSoon: { expiringSoonMs: 100, onExpiringSoon: (e) => events.push(e) },
  });
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  assert.equal(events.length, 0);
  await flush();
  assert.equal(events.length, 0);
  assert.equal(bus.getStats().expiringSoonAlerts, 0);
});

test('warning fires once when the head remaining TTL enters the window', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 200);
  const events: ExpiringSoonEvent[] = [];
  const received: unknown[] = [];
  const sub = bus.subscribe(
    't',
    (msg) => {
      received.push(msg.payload);
    },
    { expiringSoon: { expiringSoonMs: 100, onExpiringSoon: (e) => events.push(e) } },
  );
  bus.publish('t', 'm1'); // deadline 1200, remaining 200 > 100: no warning
  assert.equal(events.length, 0);
  clock.nowMs = 1150; // head remaining 50 <= 100
  bus.publish('t', 'm2'); // enqueue evaluation sees the head inside the window
  assert.equal(events.length, 1);
  bus.publish('t', 'm3'); // latch set: no second warning for the same backlog
  assert.equal(events.length, 1);
  const [event] = events;
  assert.equal(event.subscriberId, sub.id);
  assert.equal(event.pattern, 't');
  assert.equal(event.remainingMs, 50);
  assert.equal(event.expiringSoonMs, 100);
  assert.equal(event.queueDepth, 2);
  assert.equal(event.at, 1150);
  await flush();
  // Read-only: the warned messages still deliver normally.
  assert.deepEqual(received, ['m1', 'm2', 'm3']);
  assert.equal(bus.getStats().expiringSoonAlerts, 1);
});

test('latch re-arms when the queue drains', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 200);
  const events: ExpiringSoonEvent[] = [];
  bus.subscribe('t', () => {}, {
    expiringSoon: { expiringSoonMs: 100, onExpiringSoon: (e) => events.push(e) },
  });
  bus.publish('t', 'm1');
  clock.nowMs = 1150;
  bus.publish('t', 'm2'); // fires (head m1 remaining 50)
  assert.equal(events.length, 1);
  await flush(); // queue drains -> latch re-arms
  assert.equal(bus.getStats().expiringSoonAlerts, 1);
  // A fresh backlog warns again: the new head is already inside the window.
  bus.setTopicTtl('t', 50);
  bus.publish('t', 'm3'); // deadline 1200, remaining 50 <= 100: fires at once
  assert.equal(events.length, 2);
  assert.equal(events[1].remainingMs, 50);
  assert.equal(bus.getStats().expiringSoonAlerts, 2);
  await flush();
});

test('an already-expired head still warns (with non-positive remaining)', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 100);
  const events: ExpiringSoonEvent[] = [];
  const received: unknown[] = [];
  bus.subscribe(
    't',
    (msg) => {
      received.push(msg.payload);
    },
    { expiringSoon: { expiringSoonMs: 50, onExpiringSoon: (e) => events.push(e) } },
  );
  bus.publish('t', 'm1'); // deadline 1100
  clock.nowMs = 1200; // head already past its deadline
  bus.publish('t', 'm2');
  assert.equal(events.length, 1);
  assert.equal(events[0].remainingMs, -100);
  await flush();
  // The expired head is discarded by TTL as usual; the warning changed nothing.
  assert.deepEqual(received, ['m2']);
  assert.equal(bus.getStats().expiredMessages, 1);
});

test('a throwing onExpiringSoon is isolated: publish and delivery survive', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 200);
  const received: unknown[] = [];
  bus.subscribe(
    't',
    (msg) => {
      received.push(msg.payload);
    },
    {
      expiringSoon: {
        expiringSoonMs: 100,
        onExpiringSoon: () => {
          throw new Error('monitoring hook blew up');
        },
      },
    },
  );
  bus.publish('t', 'm1');
  clock.nowMs = 1150;
  bus.publish('t', 'm2'); // warning fires, hook throws, swallowed
  await flush();
  assert.deepEqual(received, ['m1', 'm2']);
  assert.equal(bus.getStats().expiringSoonAlerts, 1);
});

test('invalid expiringSoon options throw fail-fast from subscribe', () => {
  const bus = new EventBus();
  const cb = () => {};
  assert.throws(
    () => bus.subscribe('t', cb, { expiringSoon: { expiringSoonMs: -1, onExpiringSoon: cb } }),
    RangeError,
  );
  assert.throws(
    () =>
      bus.subscribe('t', cb, {
        expiringSoon: { expiringSoonMs: Number.NaN, onExpiringSoon: cb },
      }),
    RangeError,
  );
  assert.throws(
    () =>
      bus.subscribe(
        't',
        cb,
        // @ts-expect-error testing runtime validation
        { expiringSoon: { expiringSoonMs: 10, onExpiringSoon: 'nope' } },
      ),
    TypeError,
  );
  assert.throws(
    // @ts-expect-error testing runtime validation
    () => bus.subscribe('t', cb, { expiringSoon: true }),
    TypeError,
  );
  assert.equal(resolveExpiringSoonOptions(undefined, 'subscribe'), undefined);
});

test('warning count renders in the Prometheus exposition', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 200);
  bus.subscribe('t', () => {}, {
    expiringSoon: { expiringSoonMs: 100, onExpiringSoon: () => {} },
  });
  bus.publish('t', 'm1');
  clock.nowMs = 1150;
  bus.publish('t', 'm2');
  await flush();
  const exposition = renderPrometheus(bus.getStats());
  assert.match(exposition, /^eventbus_expiring_soon_alerts_total 1$/m);
});
