/**
 * Bus-level default allowed-lateness revision (EB-324).
 *
 * `setAllowedLatenessMs(allowedLatenessMs, actor?)` retunes the
 * bus-wide default grace for event-time watermark tracking — without
 * restarting the bus. Only the default moves: per-topic overrides
 * (`setTopicAllowedLateness`, EB-59) keep winning over it, the
 * per-topic peak (`max(eventTime)`) never rolls back, and messages
 * already judged late stay late — loosening never resurrects them.
 * The new default governs from the next event-time observation on
 * every topic without an override, and the next `getStats()` watermark
 * reading reflects it immediately. A bus without `durableLogDir`
 * revises fine (watermark tracking is bus-wide, always on); illegal
 * values fail closed with the live default untouched; an identical
 * revision is a no-op with no audit.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type LateMessageEvent } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const audit = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'allowed_lateness_revised' });

function topicStats(bus: EventBus, topic: string) {
  const row = bus.getStats().topics.find((t) => t.topic === topic);
  assert.ok(row !== undefined, `expected stats row for ${topic}`);
  return row;
}

test('constructor still rejects illegal allowedLatenessMs', () => {
  for (const bad of [-1, Number.NaN, Number.POSITIVE_INFINITY, '100']) {
    assert.throws(
      () => new EventBus({ allowedLatenessMs: bad as never }),
      RangeError,
    );
  }
  const bus = new EventBus({ allowedLatenessMs: 0 });
  bus.publish('t', 'a', { eventTime: 1000 });
  assert.equal(topicStats(bus, 't').watermark, 1000);
  bus.close();
});

test('revision rejects illegal values and leaves the live default and audit untouched', async () => {
  const bus = new EventBus({ allowedLatenessMs: 1000 });
  for (const bad of [-1, Number.NaN, Number.POSITIVE_INFINITY, '100', undefined]) {
    assert.throws(() => bus.setAllowedLatenessMs(bad as never), RangeError);
  }
  assert.equal(audit(bus).length, 0);
  // The failed calls changed nothing: the old default still judges.
  bus.publish('t', 'a', { eventTime: 5000 });
  bus.publish('t', 'b', { eventTime: 4500 }); // on time under 1000 (wm 4000)
  await flush();
  assert.equal(topicStats(bus, 't').lateMessages, 0);
  assert.equal(topicStats(bus, 't').watermark, 4000);
  // A valid revision afterwards still works on the untouched default.
  bus.setAllowedLatenessMs(2000, 'ops');
  assert.equal(audit(bus).length, 1);
  assert.deepEqual(audit(bus)[0].detail, { from: 1000, to: 2000 });
  assert.equal(audit(bus)[0].actor, 'ops');
  assert.equal(topicStats(bus, 't').watermark, 3000);
  bus.close();
});

test('revision requires a valid actor', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setAllowedLatenessMs(100, '' as never), RangeError);
  assert.throws(() => bus.setAllowedLatenessMs(100, 42 as never), RangeError);
  // The failed calls changed nothing: no audit, default still 0.
  assert.equal(audit(bus).length, 0);
  bus.publish('t', 'a', { eventTime: 5000 });
  assert.equal(topicStats(bus, 't').watermark, 5000);
  bus.close();
});

test('an identical revision is a no-op with no audit', () => {
  const bus = new EventBus({ allowedLatenessMs: 1000 });
  bus.setAllowedLatenessMs(1000);
  assert.equal(audit(bus).length, 0);

  const defaulted = new EventBus();
  // The default lateness is 0: re-affirming it is a no-op.
  defaulted.setAllowedLatenessMs(0);
  assert.equal(audit(defaulted).length, 0);
  bus.close();
  defaulted.close();
});

test('loosening admits more on-time arrivals; already-late messages stay late', async () => {
  const bus = new EventBus();
  bus.publish('t', 'peak', { eventTime: 5000 });
  bus.publish('t', 'late', { eventTime: 4500 }); // late under 0 (wm 5000)
  await flush();
  assert.equal(topicStats(bus, 't').lateMessages, 1);
  assert.equal(bus.getStats().lateMessages, 1);

  bus.setAllowedLatenessMs(1000);
  assert.deepEqual(audit(bus)[0].detail, { from: 0, to: 1000 });
  // The next getStats() reading reflects the new default immediately.
  assert.equal(topicStats(bus, 't').watermark, 4000);
  // The same event time is on time now — but the earlier late judgment
  // stands: loosening never resurrects a late message.
  bus.publish('t', 'again', { eventTime: 4500 });
  bus.publish('t', 'inside', { eventTime: 4200 });
  await flush();
  assert.equal(topicStats(bus, 't').lateMessages, 1);
  assert.equal(bus.getStats().lateMessages, 1);
  // Below the loosened watermark it is still late.
  bus.publish('t', 'late2', { eventTime: 3900 });
  await flush();
  assert.equal(topicStats(bus, 't').lateMessages, 2);
  bus.close();
});

test('tightening judges the next observation against a stricter cutoff right away', async () => {
  const bus = new EventBus({ allowedLatenessMs: 1000 });
  bus.publish('t', 'peak', { eventTime: 5000 });
  bus.publish('t', 'inside', { eventTime: 4500 }); // on time under 1000
  await flush();
  assert.equal(topicStats(bus, 't').lateMessages, 0);

  bus.setAllowedLatenessMs(0);
  assert.deepEqual(audit(bus)[0].detail, { from: 1000, to: 0 });
  assert.equal(topicStats(bus, 't').watermark, 5000);
  // The same event time is late now, judged at the next observation.
  bus.publish('t', 'late', { eventTime: 4500 });
  await flush();
  assert.equal(topicStats(bus, 't').lateMessages, 1);
  // The peak never rolled back: a new peak advances the watermark.
  bus.publish('t', 'new-peak', { eventTime: 6000 });
  await flush();
  assert.equal(topicStats(bus, 't').watermark, 6000);
  assert.equal(topicStats(bus, 't').lateMessages, 1);
  bus.close();
});

test('per-topic overrides still win over the revised default', async () => {
  const bus = new EventBus();
  bus.setTopicAllowedLateness('t', 1000);
  bus.publish('t', 'peak', { eventTime: 5000 });
  bus.publish('u', 'peak', { eventTime: 5000 });
  await flush();
  assert.equal(topicStats(bus, 't').watermark, 4000);
  assert.equal(topicStats(bus, 'u').watermark, 5000);

  bus.setAllowedLatenessMs(500);
  // The override still wins for 't'; 'u' follows the new default.
  assert.equal(topicStats(bus, 't').watermark, 4000);
  assert.equal(topicStats(bus, 'u').watermark, 4500);
  // 4200 is on time under the 't' override (wm 4000) but would be late
  // under the new default (wm 4500) — the override wins.
  bus.publish('t', 'inside', { eventTime: 4200 });
  // 4600 is on time under the new default (wm 4500) but would be late
  // under the old default (wm 5000) — the revision governs 'u'.
  bus.publish('u', 'inside', { eventTime: 4600 });
  await flush();
  assert.equal(topicStats(bus, 't').lateMessages, 0);
  assert.equal(topicStats(bus, 'u').lateMessages, 0);
  bus.close();
});

test('onLate reports the live lateness on the next late publish', async () => {
  const late: LateMessageEvent[] = [];
  const bus = new EventBus({ onLate: (e) => late.push(e) });
  bus.publish('t', 'peak', { eventTime: 5000 });
  bus.setAllowedLatenessMs(100);
  bus.publish('t', 'late', { eventTime: 4800 });
  await flush();
  assert.equal(late.length, 1);
  assert.equal(late[0].topic, 't');
  assert.equal(late[0].eventTime, 4800);
  assert.equal(late[0].watermark, 4900);
  assert.equal(late[0].allowedLatenessMs, 100);
  bus.close();
});

test('revision needs no durableLogDir and audits with from/to', async () => {
  const bus = new EventBus();
  bus.setAllowedLatenessMs(250, 'operator');
  bus.setAllowedLatenessMs(0);
  const entries = audit(bus);
  assert.equal(entries.length, 2);
  assert.deepEqual(entries[0].detail, { from: 0, to: 250 });
  assert.deepEqual(entries[1].detail, { from: 250, to: 0 });
  assert.equal(entries[0].actor, 'operator');
  bus.close();
});
