import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, AclDeniedError } from '../src/bus.ts';

/**
 * Synchronous cursor-based poll (EB-148): `bus.poll(topic,
 * {maxMessages, cursor?, timeoutMs?})` — the batch/ETL counterpart to the
 * async long-poll `pull` (EB-111). Returns `{ messages, nextCursor }`
 * synchronously; the caller walks the topic's history by feeding
 * `nextCursor` back as `cursor`.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-poll-'));

test('poll: returns messages in ascending seq order with nextCursor', () => {
  const bus = new EventBus();
  bus.publish('orders', { id: 1 });
  bus.publish('orders', { id: 2 });
  bus.publish('orders', { id: 3 });
  const page = bus.poll('orders', { maxMessages: 10 });
  assert.deepEqual(
    page.messages.map((m) => m.seq),
    [1, 2, 3],
  );
  assert.deepEqual(
    page.messages.map((m) => m.payload),
    [{ id: 1 }, { id: 2 }, { id: 3 }],
  );
  assert.equal(page.nextCursor, 3);
  assert.ok(page.messages.every((m) => m.topic === 'orders'));
  assert.ok(page.messages.every((m) => typeof m.at === 'number'));
});

test('poll: maxMessages caps the page; cursor walks the stream', () => {
  const bus = new EventBus();
  for (let i = 1; i <= 5; i++) bus.publish('t', i);
  let cursor = 0;
  const seen: number[] = [];
  for (;;) {
    const page = bus.poll('t', { maxMessages: 2, cursor });
    seen.push(...page.messages.map((m) => m.payload as number));
    if (page.messages.length === 0) {
      // An empty page returns the input cursor unchanged.
      assert.equal(page.nextCursor, cursor);
      break;
    }
    cursor = page.nextCursor;
  }
  assert.deepEqual(seen, [1, 2, 3, 4, 5]);
  assert.equal(cursor, 5);
});

test('poll: empty topic returns an empty page with the input cursor', () => {
  const bus = new EventBus();
  const page = bus.poll('never-published', { maxMessages: 10, cursor: 7 });
  assert.deepEqual(page.messages, []);
  assert.equal(page.nextCursor, 7);
});

test('poll: validation throws RangeError fail-fast', () => {
  const bus = new EventBus();
  assert.throws(() => bus.poll('', { maxMessages: 1 }), RangeError);
  assert.throws(() => bus.poll('a.*', { maxMessages: 1 }), RangeError);
  assert.throws(() => bus.poll('t', { maxMessages: 0 }), RangeError);
  assert.throws(() => bus.poll('t', { maxMessages: 1.5 }), RangeError);
  assert.throws(() => bus.poll('t', { maxMessages: 1, cursor: -1 }), RangeError);
  assert.throws(() => bus.poll('t', { maxMessages: 1, cursor: 1.5 }), RangeError);
  assert.throws(() => bus.poll('t', { maxMessages: 1, timeoutMs: -1 }), RangeError);
  assert.throws(() => bus.poll('t', { maxMessages: 1, timeoutMs: Number.NaN }), RangeError);
  // A failed validation leaves no poll counted.
  assert.deepEqual(bus.getStats().syncPollStats, { polls: 0, polledMessages: 0 });
});

test('poll: TTL-expired messages are skipped', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 100);
  bus.publish('t', 'fresh');
  clock.nowMs += 50;
  bus.publish('t', 'also-fresh');
  clock.nowMs += 60; // first message's deadline (t=1100) has passed
  const page = bus.poll('t', { maxMessages: 10 });
  assert.deepEqual(
    page.messages.map((m) => m.payload),
    ['also-fresh'],
  );
  assert.equal(page.nextCursor, 2);
});

test('poll: compressed payloads are inflated to the application payload', () => {
  const bus = new EventBus();
  bus.setTopicCompression('t', { thresholdBytes: 1 });
  const payload = { big: 'x'.repeat(200) };
  bus.publish('t', payload);
  const page = bus.poll('t', { maxMessages: 5 });
  assert.equal(page.messages.length, 1);
  assert.deepEqual(page.messages[0].payload, payload);
  assert.equal(page.nextCursor, 1);
});

test('poll: durable log is the source when configured; restart keeps the cursor', () => {
  const dir = freshDir();
  const bus1 = new EventBus({ durableLogDir: dir });
  bus1.publish('t', 'a');
  bus1.publish('t', 'b');
  bus1.publish('t', 'c');
  const page1 = bus1.poll('t', { maxMessages: 2 });
  assert.deepEqual(
    page1.messages.map((m) => m.payload),
    ['a', 'b'],
  );
  assert.equal(page1.nextCursor, 2);
  // A new bus over the same directory continues from the cursor.
  const bus2 = new EventBus({ durableLogDir: dir });
  const page2 = bus2.poll('t', { maxMessages: 10, cursor: page1.nextCursor });
  assert.deepEqual(
    page2.messages.map((m) => m.payload),
    ['c'],
  );
  assert.equal(page2.nextCursor, 3);
});

test('poll: does not disturb push subscribers or committed offsets', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (m) => received.push(m.payload));
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  const page = bus.poll('t', { maxMessages: 10 });
  assert.equal(page.messages.length, 2);
  await flush();
  // Push delivery is unaffected by the poll.
  assert.deepEqual(received, ['a', 'b']);
  // Polling moves no delivery counters.
  const stats = bus.getStats();
  assert.equal(stats.deliveredMessages, 2);
  assert.equal(stats.syncPollStats.polledMessages, 2);
});

test('poll: stats count polls and polled messages independently', () => {
  const bus = new EventBus();
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  assert.deepEqual(bus.getStats().syncPollStats, { polls: 0, polledMessages: 0 });
  bus.poll('t', { maxMessages: 1 });
  bus.poll('t', { maxMessages: 10, cursor: 1 });
  bus.poll('t', { maxMessages: 10, cursor: 2 });
  assert.deepEqual(bus.getStats().syncPollStats, { polls: 3, polledMessages: 2 });
  // The async pull counters are untouched.
  assert.deepEqual(bus.getStats().pullStats, { pulls: 0, pulledMessages: 0, longPollTimeouts: 0 });
  // And the push delivery metrics are untouched.
  assert.equal(bus.getStats().deliveredMessages, 0);
});

test('poll: unauthorized topic throws AclDeniedError', () => {
  const bus = new EventBus();
  bus.setAclRules([{ pattern: 'secret.**', subscribe: 'deny' }]);
  assert.throws(() => bus.poll('secret.data', { maxMessages: 1 }), AclDeniedError);
});

test('poll: namespace option resolves like pull/subscribe', () => {
  const bus = new EventBus();
  const ns = bus.createNamespace('tenant1', { allowPublish: true, allowSubscribe: true });
  ns.publish('orders', { id: 9 });
  const page = bus.poll('orders', { maxMessages: 10, namespace: 'tenant1' });
  assert.equal(page.messages.length, 1);
  assert.deepEqual(page.messages[0].payload, { id: 9 });
  assert.equal(page.nextCursor, 1);
  // The global bus view does not see the namespaced topic.
  const global = bus.poll('orders', { maxMessages: 10 });
  assert.deepEqual(global.messages, []);
});

test('poll: timeoutMs is accepted but the call never blocks', () => {
  const bus = new EventBus();
  bus.publish('t', 'x');
  const start = Date.now();
  const page = bus.poll('t', { maxMessages: 10, timeoutMs: 5000 });
  assert.ok(Date.now() - start < 1000, 'poll must return immediately');
  assert.equal(page.messages.length, 1);
  assert.equal(page.nextCursor, 1);
});
