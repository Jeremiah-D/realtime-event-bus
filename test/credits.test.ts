import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type CreditExhaustedEvent } from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-credits-'));

test('grant/consume/exhaust: delivery pauses at zero credit, nothing is dropped', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const events: CreditExhaustedEvent[] = [];
  const sub = bus.subscribe('t', (m) => received.push(m.payload), {
    onCreditExhausted: (e) => events.push(e),
  });
  assert.equal(bus.grantCredits(sub.id, 2), 2);
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  // Only the granted 2 are delivered; the rest stay queued.
  assert.deepEqual(received, [0, 1]);
  assert.equal(bus.getStats().creditExhaustedSubscribers, 1);
  assert.equal(events.length, 1);
  assert.equal(events[0].subscriberId, sub.id);
  assert.equal(events[0].pattern, 't');
  assert.equal(events[0].pendingMessages, 3);
});

test('refill auto-resumes in order; the latch is edge-triggered per episode', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const events: CreditExhaustedEvent[] = [];
  const sub = bus.subscribe('t', (m) => received.push(m.payload), {
    onCreditExhausted: (e) => events.push(e),
  });
  bus.grantCredits(sub.id, 2);
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0, 1]);
  assert.equal(events.length, 1);
  // A refill re-arms the latch and resumes automatically — no manual
  // resume call needed.
  bus.grantCredits(sub.id, 2);
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3]);
  assert.equal(events.length, 2);
  bus.grantCredits(sub.id, 1);
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3, 4]);
  assert.equal(events.length, 3);
  assert.equal(bus.getStats().creditExhaustedSubscribers, 1);
});

test('grantCredits accumulates and returns the new balance', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {});
  assert.equal(bus.grantCredits(sub.id, 2), 2);
  assert.equal(bus.grantCredits(sub.id, 3), 5);
  assert.equal(bus.getStats().creditExhaustedSubscribers, 0);
});

test('grantCredits validates its arguments', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {});
  for (const bad of [0, -1, 1.5, NaN, Infinity, -Infinity]) {
    assert.throws(() => bus.grantCredits(sub.id, bad), RangeError, `n=${String(bad)}`);
  }
  assert.throws(() => bus.grantCredits('nope', 1), /unknown subscriber/);
});

test('opt-in: subscribers without a grant have unlimited credit', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (m) => received.push(m.payload));
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3, 4]);
  assert.equal(bus.getStats().creditExhaustedSubscribers, 0);
  const text = renderPrometheus(bus.getStats());
  assert.ok(text.includes('eventbus_credit_exhausted_subscribers 0'));
});

test('durable-log replay does not consume credit (independent budget)', async () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  // Publish history with no subscribers; it lands in the durable log.
  bus.publish('t', 'r1');
  bus.publish('t', 'r2');
  bus.publish('t', 'r3');
  await flush();
  const received: unknown[] = [];
  const events: CreditExhaustedEvent[] = [];
  const sub = bus.subscribe('t', (m) => received.push(m.payload), {
    resumeFromSeq: 0,
    onCreditExhausted: (e) => events.push(e),
  });
  // Grant BEFORE the replay flush runs: the replayed backlog is already
  // queued (replay is synchronous in subscribe), the live messages below
  // queue behind it.
  bus.grantCredits(sub.id, 1);
  bus.publish('t', 'l1');
  bus.publish('t', 'l2');
  await flush();
  // The 3 replayed messages flow on the independent replay budget — the
  // single credit is consumed by exactly one live message.
  assert.deepEqual(received, ['r1', 'r2', 'r3', 'l1']);
  assert.equal(events.length, 1);
  assert.equal(events[0].pendingMessages, 1);
  assert.equal(bus.getStats().creditExhaustedSubscribers, 1);
  // The remaining live message resumes on refill.
  bus.grantCredits(sub.id, 1);
  await flush();
  assert.deepEqual(received, ['r1', 'r2', 'r3', 'l1', 'l2']);
  assert.equal(events.length, 2);
});

test('batch delivery consumes one credit per message', async () => {
  const bus = new EventBus();
  const batches: unknown[][] = [];
  const events: CreditExhaustedEvent[] = [];
  const sub = bus.subscribe(
    't',
    (msgs) => batches.push((msgs as { payload: unknown }[]).map((m) => m.payload)),
    { batch: { maxSize: 3, maxWaitMs: 5 }, onCreditExhausted: (e) => events.push(e) },
  );
  bus.grantCredits(sub.id, 2);
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  await sleep(30);
  await flush();
  // Collection is bounded by the credit: the first batch carries only
  // the 2 granted messages and exhausts the balance.
  assert.deepEqual(batches, [[0, 1]]);
  assert.equal(events.length, 1);
  assert.equal(bus.getStats().creditExhaustedSubscribers, 1);
  bus.grantCredits(sub.id, 3);
  await flush();
  await sleep(30);
  await flush();
  assert.deepEqual(batches, [
    [0, 1],
    [2, 3, 4],
  ]);
  assert.equal(events.length, 2);
});

test('exhausted subscriber pauses while others keep receiving', async () => {
  const bus = new EventBus();
  const a: unknown[] = [];
  const b: unknown[] = [];
  const subA = bus.subscribe('t', (m) => a.push(m.payload));
  bus.subscribe('t', (m) => b.push(m.payload));
  bus.grantCredits(subA.id, 1);
  bus.publish('t', 'x');
  bus.publish('t', 'y');
  await flush();
  assert.deepEqual(a, ['x']);
  assert.deepEqual(b, ['x', 'y']);
  assert.equal(bus.getStats().creditExhaustedSubscribers, 1);
  bus.grantCredits(subA.id, 2);
  await flush();
  assert.deepEqual(a, ['x', 'y']);
  assert.equal(bus.getStats().creditExhaustedSubscribers, 0);
});

test('Prometheus gauge tracks exhaustion episodes', async () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {});
  const gauge = () =>
    renderPrometheus(bus.getStats())
      .split('\n')
      .find((l) => l.startsWith('eventbus_credit_exhausted_subscribers '));
  assert.equal(gauge(), 'eventbus_credit_exhausted_subscribers 0');
  bus.grantCredits(sub.id, 1);
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  assert.equal(gauge(), 'eventbus_credit_exhausted_subscribers 1');
  bus.grantCredits(sub.id, 2);
  await flush();
  assert.equal(gauge(), 'eventbus_credit_exhausted_subscribers 0');
});
