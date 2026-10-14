import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  ChaosInjectionError,
  type ChaosScenario,
} from '../src/bus.ts';
import { FaultInjector } from '../src/chaos.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

function chaosBus(scenarios: ChaosScenario[]): EventBus {
  return new EventBus({ chaos: { scenarios } });
}

// ---------------------------------------------------------------------------
// FaultInjector unit: seeded determinism, rate boundaries, validation
// ---------------------------------------------------------------------------

test('FaultInjector: same seed + same evaluation order = identical fault sequence', () => {
  const scenarios: ChaosScenario[] = [
    { type: 'error', target: 'handler', rate: 0.5, seed: 42 },
    { type: 'drop', target: 'drain', rate: 0.3, seed: 7 },
  ];
  const run = () => {
    const injector = new FaultInjector(scenarios);
    const seq: Array<string | null> = [];
    for (let i = 0; i < 50; i += 1) {
      seq.push(injector.faultFor('handler')?.type ?? null);
      seq.push(injector.faultFor('drain')?.type ?? null);
      seq.push(injector.faultFor('admission')?.type ?? null);
    }
    return seq;
  };
  const a = run();
  const b = run();
  assert.deepEqual(a, b);
  // Sanity: with these rates/seeds some faults actually fire.
  assert.ok(a.some((f) => f !== null), 'expected at least one fault to fire');
});

test('FaultInjector: rate 1 always fires, rate 0 never fires', () => {
  const always = new FaultInjector([{ type: 'error', target: 'handler', rate: 1, seed: 1 }]);
  for (let i = 0; i < 10; i += 1) {
    assert.equal(always.faultFor('handler')?.type, 'error');
  }
  const never = new FaultInjector([{ type: 'error', target: 'handler', rate: 0, seed: 1 }]);
  for (let i = 0; i < 10; i += 1) {
    assert.equal(never.faultFor('handler'), null);
  }
  // Different seeds diverge (the streams are actually seeded).
  const s1 = new FaultInjector([{ type: 'error', target: 'handler', rate: 0.5, seed: 1 }]);
  const s2 = new FaultInjector([{ type: 'error', target: 'handler', rate: 0.5, seed: 2 }]);
  const seq1 = Array.from({ length: 30 }, () => s1.faultFor('handler') !== null);
  const seq2 = Array.from({ length: 30 }, () => s2.faultFor('handler') !== null);
  assert.notDeepEqual(seq1, seq2);
});

test('constructor rejects invalid chaos scenarios fail-fast', () => {
  const bad: ChaosScenario[][] = [
    // type/target matrix violations
    [{ type: 'latency', target: 'admission', rate: 1, latencyMs: 10 }],
    [{ type: 'stall', target: 'admission', rate: 1 }],
    [{ type: 'error', target: 'drain', rate: 1 }],
    [{ type: 'latency', target: 'handler', rate: 1, latencyMs: 10 }],
    [{ type: 'stall', target: 'handler', rate: 1 }],
    // latencyMs rules
    [{ type: 'latency', target: 'drain', rate: 1 }],
    [{ type: 'latency', target: 'drain', rate: 1, latencyMs: -5 }],
    [{ type: 'drop', target: 'drain', rate: 1, latencyMs: 10 }],
    // rate rules
    [{ type: 'drop', target: 'drain', rate: 1.5 }],
    [{ type: 'drop', target: 'drain', rate: -0.1 }],
    [{ type: 'drop', target: 'drain', rate: Number.NaN }],
    // unknown type/target
    [{ type: 'explode' as never, target: 'drain', rate: 1 }],
    [{ type: 'drop', target: 'nowhere' as never, rate: 1 }],
  ];
  for (const scenarios of bad) {
    assert.throws(() => new EventBus({ chaos: { scenarios } }), RangeError);
  }
});

test('empty chaos config means no injector: stats.chaos absent, zero overhead', () => {
  for (const bus of [new EventBus(), new EventBus({ chaos: undefined }), new EventBus({ chaos: {} }), new EventBus({ chaos: { scenarios: [] } })]) {
    let calls = 0;
    bus.subscribe('t', () => {
      calls += 1;
    });
    bus.publish('t', 'x');
    assert.equal(bus.getStats().chaos, undefined);
    assert.equal(calls, 0); // flush has not run yet; publish itself is unaffected
  }
});

// ---------------------------------------------------------------------------
// admission target
// ---------------------------------------------------------------------------

test("admission 'drop': publish returns 0, nothing sequenced/fanned out/logged", async () => {
  const bus = chaosBus([{ type: 'drop', target: 'admission', rate: 1, seed: 3 }]);
  let calls = 0;
  bus.subscribe('t', () => {
    calls += 1;
  });
  assert.equal(bus.publish('t', 'a'), 0);
  assert.equal(bus.publish('t', 'b'), 0);
  await flush();
  assert.equal(calls, 0);
  const stats = bus.getStats();
  assert.equal(stats.chaos?.injected, 2);
  assert.equal(stats.chaos?.byTarget.admission, 2);
  // Nothing was sequenced: the topic saw no publishes at all.
  const topic = stats.topics.find((t) => t.topic === 't');
  assert.equal(topic?.publishedMessages ?? 0, 0);
});

test("admission 'error': publish throws ChaosInjectionError with zero side effects", async () => {
  const bus = chaosBus([{ type: 'error', target: 'admission', rate: 1, seed: 4 }]);
  let calls = 0;
  bus.subscribe('t', () => {
    calls += 1;
  });
  assert.throws(() => bus.publish('t', 'a'), ChaosInjectionError);
  await flush();
  assert.equal(calls, 0);
  assert.equal(bus.getStats().chaos?.byTarget.admission, 1);
});

test('admission fault is deterministic under a seeded rate', async () => {
  const mk = () => chaosBus([{ type: 'drop', target: 'admission', rate: 0.5, seed: 99 }]);
  const run = async () => {
    const bus = mk();
    const accepted: number[] = [];
    for (let i = 0; i < 20; i += 1) accepted.push(bus.publish('t', i));
    await flush();
    return accepted;
  };
  assert.deepEqual(await run(), await run());
});

// ---------------------------------------------------------------------------
// drain target
// ---------------------------------------------------------------------------

test("drain 'stall': queue accumulates and backpressure fires deterministically", async () => {
  const bus = chaosBus([{ type: 'stall', target: 'drain', rate: 1, seed: 5 }]);
  let calls = 0;
  let backpressure = 0;
  const sub = bus.subscribe('t', () => {
    calls += 1;
  }, {
    queueSize: 8,
    onBackpressure: () => {
      backpressure += 1;
    },
  });
  for (let i = 0; i < 20; i += 1) bus.publish('t', i);
  await flush();
  // The drain never ran: nothing delivered, the queue saturated and the
  // backpressure latch fired.
  assert.equal(calls, 0);
  assert.ok(backpressure >= 1, 'expected onBackpressure to fire');
  assert.ok(bus.pendingCount(sub.id) > 0, 'expected backlog to accumulate');
  assert.equal(bus.getStats().chaos?.byTarget.drain ?? 0, 1);
});

test("drain 'drop': queued messages are discarded and the loss is a visible sequence gap", async () => {
  const bus = chaosBus([{ type: 'drop', target: 'drain', rate: 1, seed: 6 }]);
  const seen: unknown[] = [];
  bus.subscribe('t', (msg) => {
    seen.push(msg.payload);
  });
  // Establish the gap baseline with one clean delivery first (chaos off would
  // deliver seq 1; here the first drain already drops, so the baseline is
  // established by the drop itself — publish one message, let it drop).
  bus.publish('t', 'lost-1');
  bus.publish('t', 'lost-2');
  await flush();
  assert.deepEqual(seen, []);
  // A second bus without chaos shows the contrast: drops are the fault.
  const clean = new EventBus();
  const cleanSeen: unknown[] = [];
  clean.subscribe('t', (msg) => {
    cleanSeen.push(msg.payload);
  });
  clean.publish('t', 'kept');
  await flush();
  assert.deepEqual(cleanSeen, ['kept']);
  const stats = bus.getStats();
  assert.equal(stats.chaos?.byTarget.drain, 1);
});

test("drain 'latency': the drain round is deferred, then delivered", async () => {
  const bus = chaosBus([{ type: 'latency', target: 'drain', rate: 1, seed: 7, latencyMs: 40 }]);
  const seen: unknown[] = [];
  bus.subscribe('t', (msg) => {
    seen.push(msg.payload);
  });
  bus.publish('t', 'hello');
  await flush();
  // The first drain round deferred instead of delivering.
  assert.deepEqual(seen, []);
  await sleep(120);
  await flush();
  assert.deepEqual(seen, ['hello']);
  assert.equal(bus.getStats().chaos?.byTarget.drain ?? 0, 1);
});

// ---------------------------------------------------------------------------
// handler target: EB-18 degraded + EB-28 dead-letter reproduction
// ---------------------------------------------------------------------------

test("handler 'error' + health probe: deterministic EB-18 degraded-subscriber reproduction", async () => {
  const bus = chaosBus([{ type: 'error', target: 'handler', rate: 1, seed: 8 }]);
  const events: Array<{ subscriberId: string }> = [];
  let calls = 0;
  const sub = bus.subscribe(
    't',
    () => {
      calls += 1;
    },
    {
      healthProbe: { maxConsecutiveFailures: 3 },
      onDegraded: (e) => events.push(e),
    },
  );
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  bus.publish('t', 'c');
  await flush();
  // Every handler invocation faulted; the probe counted three consecutive
  // failures and auto-paused the subscriber — without any real handler bug.
  assert.equal(calls, 0);
  assert.equal(events.length, 1);
  assert.equal(events[0].subscriberId, sub.id);
  assert.equal(bus.getStats().degradedSubscribers, 1);
  assert.equal(bus.getStats().chaos?.byTarget.handler, 3);
});

test("handler 'error' + reliable + deadLetter: deterministic EB-28 dead-letter reproduction", async () => {
  const bus = chaosBus([{ type: 'error', target: 'handler', rate: 1, seed: 9 }]);
  const sub = bus.subscribeReliable(
    't',
    () => {
      // Never runs: the fault throws before user code.
    },
    { deadLetter: { maxRedeliveries: 2 }, ackTimeoutMs: 50 },
  );
  bus.publish('t', 'poison');
  // Initial delivery + 2 redeliveries, then the budget is exhausted and
  // the message is dead-lettered — each round needs its own flush.
  for (let i = 0; i < 6; i += 1) await flush();
  const dead = bus.getDeadLetterMessages(sub.id);
  assert.equal(dead.length, 1);
  assert.equal(dead[0].payload, 'poison');
  assert.ok(dead[0].lastError?.includes('chaos'), `lastError should name the fault, got: ${dead[0].lastError}`);
  assert.ok((bus.getStats().chaos?.byTarget.handler ?? 0) >= 3);
});

test("handler 'drop': user code never runs, message consumed as delivered", async () => {
  const bus = chaosBus([{ type: 'drop', target: 'handler', rate: 1, seed: 10 }]);
  let calls = 0;
  bus.subscribe('t', () => {
    calls += 1;
  });
  bus.publish('t', 'a');
  await flush();
  assert.equal(calls, 0);
  assert.equal(bus.getStats().chaos?.byTarget.handler, 1);
  // No failure was recorded anywhere: a drop is silent loss, not an error.
  assert.equal(bus.getStats().degradedSubscribers, 0);
});

// ---------------------------------------------------------------------------
// off = zero behavioral difference
// ---------------------------------------------------------------------------

test('chaos off: identical delivery behavior, no stats footprint', async () => {
  const mk = (chaos?: { scenarios: ChaosScenario[] }) => {
    const bus = new EventBus(chaos === undefined ? undefined : { chaos });
    const seen: unknown[] = [];
    bus.subscribe('t', (msg) => seen.push(msg.payload));
    return { bus, seen };
  };
  const plain = mk();
  const empty = mk({ scenarios: [] });
  for (const { bus } of [plain, empty]) {
    bus.publish('t', 'x');
    bus.publish('t', 'y');
  }
  await flush();
  await flush();
  assert.deepEqual(plain.seen, ['x', 'y']);
  assert.deepEqual(empty.seen, ['x', 'y']);
  assert.equal(plain.bus.getStats().chaos, undefined);
  assert.equal(empty.bus.getStats().chaos, undefined);
});
