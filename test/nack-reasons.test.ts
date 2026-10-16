import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  type BusStats,
  type DegradedEvent,
  type Delivery,
  type BusMessage,
} from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/**
 * Runs enough flush rounds for a redelivery chain to play out: each
 * redelivery requeues through a scheduled flush, so one flush per
 * redelivery round plus slack.
 */
const settleRedeliveries = async (rounds = 16): Promise<void> => {
  for (let i = 0; i < rounds; i++) await flush();
};

test('handler_throw: a synchronously throwing handler classifies redeliveries (EB-129)', async () => {
  const bus = new EventBus();
  // Always throws, DLQ budget 2: three counted redeliveries, then the
  // dead-lettering — the dead-lettering is the *result* of the third
  // redelivery, not a separate count.
  const sub = bus.subscribeReliable(
    't',
    () => {
      throw new Error('boom');
    },
    { deadLetter: { maxRedeliveries: 2 }, ackTimeoutMs: 60_000 },
  );
  bus.publish('t', 'a');
  await settleRedeliveries();

  const rows = bus.getStats().nackReasons;
  assert.equal(rows.length, 1);
  assert.deepEqual(rows[0], {
    subscriberId: sub.id,
    pattern: 't',
    handlerThrow: 3,
    explicitNack: 0,
    ackTimeout: 0,
  });
  const dlq = bus.getDeadLetterMessages(sub.id);
  assert.equal(dlq.length, 1);
  assert.equal(dlq[0].lastError, 'boom');
});

test('handler_throw: a throwing batch handler counts every message in the batch', async () => {
  const bus = new EventBus();
  const bsub = bus.subscribeReliable(
    'bt',
    () => {
      throw new Error('batch-boom');
    },
    {
      batch: { maxSize: 2, maxWaitMs: 0 },
      deadLetter: { maxRedeliveries: 1 },
      ackTimeoutMs: 60_000,
    },
  );
  bus.publish('bt', 'x');
  bus.publish('bt', 'y');
  await settleRedeliveries();
  await sleep(15);
  await settleRedeliveries();

  const rows = bus.getStats().nackReasons;
  assert.equal(rows.length, 1);
  // Two messages x two redelivery rounds (the second round dead-letters
  // each message) — batch-internal per-message accounting.
  assert.deepEqual(rows[0], {
    subscriberId: bsub.id,
    pattern: 'bt',
    handlerThrow: 4,
    explicitNack: 0,
    ackTimeout: 0,
  });
  assert.equal(bus.getDeadLetterMessages(bsub.id).length, 2);
});

test('explicit_nack: delivery.nack() classifies per redelivery', async () => {
  const bus = new EventBus();
  let first = true;
  const sub = bus.subscribeReliable(
    't',
    (d: Delivery<BusMessage>) => {
      if (first) {
        first = false;
        d.nack();
      } else {
        d.ack();
      }
    },
    { ackTimeoutMs: 60_000 },
  );
  bus.publish('t', 'a');
  await settleRedeliveries();

  const rows = bus.getStats().nackReasons;
  assert.equal(rows.length, 1);
  assert.deepEqual(rows[0], {
    subscriberId: sub.id,
    pattern: 't',
    handlerThrow: 0,
    explicitNack: 1,
    ackTimeout: 0,
  });
});

test('explicit_nack: bus.nackBatch() counts each requeued message', async () => {
  const bus = new EventBus();
  const held: Array<Delivery<BusMessage>> = [];
  let collecting = true;
  const bsub = bus.subscribeReliable(
    'bt',
    (d: Delivery<BusMessage>) => {
      if (collecting) {
        held.push(d); // leave outstanding — settled via nackBatch below
      } else {
        d.ack();
      }
    },
    { ackTimeoutMs: 60_000 },
  );
  bus.publish('bt', 1);
  bus.publish('bt', 2);
  await settleRedeliveries();
  assert.equal(held.length, 2);

  collecting = false;
  assert.equal(bus.nackBatch(bsub.id, held), 2);
  await settleRedeliveries();

  const rows = bus.getStats().nackReasons;
  assert.equal(rows.length, 1);
  assert.deepEqual(rows[0], {
    subscriberId: bsub.id,
    pattern: 'bt',
    handlerThrow: 0,
    explicitNack: 2,
    ackTimeout: 0,
  });
  assert.equal(bus.getStats().unackedDeliveries, 0);
});

test('ack_timeout: an unsettled delivery requeues through the timer and counts ack_timeout', async () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable(
    't',
    (d: Delivery<BusMessage>) => {
      // Never settles until the second redelivery: each silent round is
      // one ack-timeout requeue, then ack to stop the chain deterministically.
      if (d.redeliveries >= 2) d.ack();
    },
    { ackTimeoutMs: 30 },
  );
  bus.publish('t', 'a');
  await sleep(150); // both 30ms timers fire with wide margin; the ack stops any third

  // Assert before unsubscribing: teardown drops the subscriber from
  // getStats(), so the rows must be read while it is still live.
  const rows = bus.getStats().nackReasons;
  assert.equal(rows.length, 1);
  assert.deepEqual(rows[0], {
    subscriberId: sub.id,
    pattern: 't',
    handlerThrow: 0,
    explicitNack: 0,
    ackTimeout: 2,
  });
  sub.unsubscribe();
  await flush();
});

test('the three classes are mutually exclusive: one redelivery counts exactly once', async () => {
  const bus = new EventBus();
  // Round plan for one message: throw once (handler_throw), explicit
  // nack once (explicit_nack), then stay silent through one ack timeout
  // (ack_timeout), then ack to stop the chain.
  let phase = 0;
  const sub = bus.subscribeReliable(
    't',
    (d: Delivery<BusMessage>) => {
      phase += 1;
      if (phase === 1) throw new Error('first-boom');
      if (phase === 2) d.nack();
      // phase 3: never settles — the ack timeout requeues it.
      // phase 4: ack to end the chain.
      if (phase >= 4) d.ack();
    },
    { deadLetter: { maxRedeliveries: 10 }, ackTimeoutMs: 30 },
  );
  bus.publish('t', 'a');
  await settleRedeliveries(); // throw -> redelivery (handler_throw); nack -> redelivery (explicit_nack)
  // Phase 3: the ack-timeout timer requeues once; the phase-4 redelivery
  // acks synchronously, so exactly one timer round can ever fire.
  const deadline = Date.now() + 3000;
  while ((bus.getStats().nackReasons[0]?.ackTimeout ?? 0) < 1 && Date.now() < deadline) {
    await sleep(10);
  }
  await settleRedeliveries(); // phase 4: redelivery acked, chain ends

  // Assert before unsubscribing: teardown drops the subscriber from
  // getStats(), so the rows must be read while it is still live.
  const rows = bus.getStats().nackReasons;
  assert.equal(rows.length, 1);
  // Exact cumulative assertions: nothing double-counted, nothing lost.
  assert.deepEqual(rows[0], {
    subscriberId: sub.id,
    pattern: 't',
    handlerThrow: 1,
    explicitNack: 1,
    ackTimeout: 1,
  });
  sub.unsubscribe();
  await flush();
});

test('getStats().nackReasons: zero-count reliable subscribers and plain subscribers emit no rows', async () => {
  const bus = new EventBus();
  const quiet = bus.subscribeReliable('quiet', (d: Delivery<BusMessage>) => d.ack(), {
    ackTimeoutMs: 60_000,
  });
  const plain = bus.subscribe('plain', () => {});
  const noisy = bus.subscribeReliable('noisy', (d: Delivery<BusMessage>) => d.nack(), {
    deadLetter: { maxRedeliveries: 0 }, // the first redelivery dead-letters: exactly one count
    ackTimeoutMs: 60_000,
  });
  bus.publish('quiet', 1);
  bus.publish('plain', 1);
  bus.publish('noisy', 1);
  await settleRedeliveries();

  const rows = bus.getStats().nackReasons;
  // Only the subscriber with a non-zero count appears.
  assert.equal(rows.length, 1);
  assert.equal(rows[0].subscriberId, noisy.id);
  assert.equal(rows[0].pattern, 'noisy');
  assert.deepEqual(rows[0], {
    subscriberId: noisy.id,
    pattern: 'noisy',
    handlerThrow: 0,
    explicitNack: 1,
    ackTimeout: 0,
  });
  assert.ok(!rows.some((r) => r.subscriberId === quiet.id), 'zero-count reliable subscriber must not appear');
  assert.ok(!rows.some((r) => r.subscriberId === plain.id), 'plain subscriber must not appear');
});

test('metrics: eventbus_nack_reasons_total renders one series per non-zero class', async () => {
  const bus = new EventBus();
  let phase = 0;
  const sub = bus.subscribeReliable(
    't',
    (d: Delivery<BusMessage>) => {
      phase += 1;
      if (phase === 1) throw new Error('boom');
      d.ack();
    },
    { deadLetter: { maxRedeliveries: 10 }, ackTimeoutMs: 60_000 },
  );
  bus.publish('t', 'a');
  await settleRedeliveries();

  const text = renderPrometheus(bus.getStats());
  assert.ok(text.includes('# TYPE eventbus_nack_reasons_total counter'), 'missing TYPE line');
  assert.ok(
    text.includes(`eventbus_nack_reasons_total{subscriber="${sub.id}",reason="handler_throw"} 1`),
    `missing handler_throw series in:\n${text}`,
  );
  // Zero-count classes emit no series.
  assert.ok(!text.includes('reason="explicit_nack"'), 'zero explicit_nack must emit no series');
  assert.ok(!text.includes('reason="ack_timeout"'), 'zero ack_timeout must emit no series');
});

test('metrics: subscriber label values are escaped per the exposition format', () => {
  // The renderer is a pure function of BusStats: fabricate a snapshot to
  // cover label characters a subscriber id can never carry through the
  // bus (a quote/newline never appears in `sub-N` ids — but the escaper
  // must still handle them).
  const stats = {
    // `deliveryLatency` / `topics` have no `?? []` tolerance in the
    // renderer, so the partial fixture must carry them explicitly.
    deliveryLatency: [],
    topics: [],
    nackReasons: [
      {
        subscriberId: 'we"ird\nsub\\id',
        pattern: 't',
        handlerThrow: 1,
        explicitNack: 2,
        ackTimeout: 0,
      },
    ],
  } as unknown as BusStats;
  const text = renderPrometheus(stats);
  assert.ok(
    text.includes('eventbus_nack_reasons_total{subscriber="we\\"ird\\nsub\\\\id",reason="handler_throw"} 1'),
    `unexpected label escaping in:\n${text}`,
  );
  assert.ok(
    text.includes('eventbus_nack_reasons_total{subscriber="we\\"ird\\nsub\\\\id",reason="explicit_nack"} 2'),
    `unexpected label escaping in:\n${text}`,
  );
});

test('orthogonal to EB-18: repeated handler throws count nack reasons without degrading', async () => {
  const bus = new EventBus();
  const events: DegradedEvent[] = [];
  const sub = bus.subscribeReliable(
    't',
    () => {
      throw new Error('always-boom');
    },
    {
      deadLetter: { maxRedeliveries: 2 },
      ackTimeoutMs: 60_000,
      // EB-18: with a plain subscription this threshold would degrade
      // the subscriber after 2 consecutive failures.
      healthProbe: { maxConsecutiveFailures: 2 },
      onDegraded: (e) => events.push(e),
    },
  );
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await settleRedeliveries();

  // Every redelivery is classified as handler_throw ...
  const rows = bus.getStats().nackReasons;
  assert.equal(rows.length, 1);
  assert.equal(rows[0].handlerThrow, 6); // 2 messages x 3 redeliveries (DLQ budget 2)
  // ... while the health probe never sees a failure: with a DLQ the
  // throw is caught inside the at-least-once wrapper, so the probe's
  // handler invocation returns cleanly. Counting is diagnostic only.
  assert.equal(bus.getStats().degradedSubscribers, 0);
  assert.deepEqual(bus.subscriberHealth(sub.id), {
    subscriberId: sub.id,
    enabled: true,
    degraded: false,
    consecutiveFailures: 0,
  });
  assert.equal(events.length, 0);
});

test('stale handles: a nack on an already-settled delivery counts nothing', async () => {
  const bus = new EventBus();
  const held: Array<Delivery<BusMessage>> = [];
  const sub = bus.subscribeReliable('t', (d: Delivery<BusMessage>) => held.push(d), {
    ackTimeoutMs: 60_000,
  });
  bus.publish('t', 'a');
  await settleRedeliveries();
  assert.equal(held.length, 1);

  held[0].nack(); // live: counts explicit_nack once
  await settleRedeliveries();
  held[0].nack(); // stale: the tracker's settled guard no-ops it
  await settleRedeliveries();
  held[1].ack(); // settle the redelivery cleanly
  await flush();

  const rows = bus.getStats().nackReasons;
  assert.equal(rows.length, 1);
  assert.equal(rows[0].explicitNack, 1);
  assert.equal(rows[0].handlerThrow, 0);
  assert.equal(rows[0].ackTimeout, 0);
});
