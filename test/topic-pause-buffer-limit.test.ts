import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  EventBus,
  type AdmissionRejectionEvent,
} from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/**
 * A manually-advanced clock handed to the bus via `EventBusOptions.now`,
 * so pause/TTL tests are deterministic instead of racing wall time.
 */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-pause-buffer-limit-'));

/** Collects admission-rejection hook events in order. */
function makeSink() {
  const events: AdmissionRejectionEvent[] = [];
  const hook = (event: AdmissionRejectionEvent) => {
    events.push(event);
  };
  return { events, hook };
}

// EB-401: topic pause queue-mode buffer bound live revision.

test('EB-401: pauseTopic with maxBuffered stores the bound (getter + stats row)', () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  bus.pauseTopic('billing.*', { mode: 'queue', maxBuffered: 2 });
  assert.equal(bus.getTopicPauseBufferLimit('billing.*'), 2);
  assert.deepEqual(bus.getStats().pausedTopics, [
    { topicPattern: 'billing.*', mode: 'queue', bufferedMessages: 0, maxBuffered: 2 },
  ]);
});

test('EB-401: under the bound, publishes buffer normally', async () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  const received: unknown[] = [];
  bus.subscribe('t', (m) => received.push(m.payload));
  bus.pauseTopic('t', { mode: 'queue', maxBuffered: 2 });
  assert.equal(bus.publish('t', 'a'), 0);
  assert.equal(bus.publish('t', 'b'), 0);
  await flush();
  assert.deepEqual(received, []);
  assert.equal(bus.getStats().pausedTopics[0].bufferedMessages, 2);
  assert.equal(bus.getStats().pausedMessages, 0);
  assert.equal(bus.resumeTopic('t'), true);
  await flush();
  assert.deepEqual(received, ['a', 'b']);
});

test('EB-401: a full buffer sheds the publish — 0, reason topic-paused, buffer unchanged, no seq burned', async () => {
  const sink = makeSink();
  const bus = new EventBus({ durableLogDir: freshDir(), onAdmissionRejected: sink.hook });
  const received: string[] = [];
  const seqs: number[] = [];
  bus.subscribe('t', (m) => {
    received.push(m.payload as string);
    seqs.push(m.seq);
  });
  bus.pauseTopic('t', { mode: 'queue', maxBuffered: 2 });
  assert.equal(bus.publish('t', 'a'), 0);
  assert.equal(bus.publish('t', 'b'), 0);
  // Full now: the third publish sheds like shed mode.
  assert.equal(bus.publish('t', 'c'), 0);
  assert.equal(sink.events.length, 1);
  assert.equal(sink.events[0].topic, 't');
  assert.equal(sink.events[0].reason, 'topic-paused');
  assert.equal(bus.getStats().pausedMessages, 1);
  assert.equal(bus.getStats().pausedTopics[0].bufferedMessages, 2);
  // No sequence number burned by the overflow shed.
  assert.equal(bus.getStats().totalPublished, 0);
  assert.equal(bus.resumeTopic('t'), true);
  await flush();
  assert.deepEqual(received, ['a', 'b']);
  assert.deepEqual(seqs, [1, 2]);
  assert.equal(bus.publish('t', 'd'), 1);
  await flush();
  assert.deepEqual(seqs, [1, 2, 3], 'the shed message left no gap');
});

test('EB-401: live tightening never evicts buffered entries; later publishes shed per the new bound', async () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  const received: unknown[] = [];
  bus.subscribe('t', (m) => received.push(m.payload));
  bus.pauseTopic('t', { mode: 'queue', maxBuffered: 3 });
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  bus.publish('t', 'c');
  assert.equal(bus.getStats().pausedTopics[0].bufferedMessages, 3);
  bus.setTopicPauseBufferLimit('t', 1);
  // Already-buffered entries stay put.
  assert.equal(bus.getStats().pausedTopics[0].bufferedMessages, 3);
  assert.equal(bus.publish('t', 'd'), 0);
  assert.equal(bus.getStats().pausedMessages, 1);
  assert.equal(bus.getStats().pausedTopics[0].bufferedMessages, 3);
  assert.equal(bus.resumeTopic('t'), true);
  await flush();
  assert.deepEqual(received, ['a', 'b', 'c'], 'the bound drops nothing at resume');
});

test('EB-401: live loosening resumes buffering at once', async () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  const received: unknown[] = [];
  bus.subscribe('t', (m) => received.push(m.payload));
  bus.pauseTopic('t', { mode: 'queue', maxBuffered: 1 });
  bus.publish('t', 'a');
  bus.publish('t', 'b'); // shed: buffer full
  assert.equal(bus.getStats().pausedMessages, 1);
  bus.setTopicPauseBufferLimit('t', 3);
  bus.publish('t', 'c'); // buffered again
  assert.equal(bus.getStats().pausedMessages, 1);
  assert.equal(bus.getStats().pausedTopics[0].bufferedMessages, 2);
  assert.equal(bus.resumeTopic('t'), true);
  await flush();
  assert.deepEqual(received, ['a', 'c']);
});

test('EB-401: null clears the bound back to unbounded', async () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  const received: unknown[] = [];
  bus.subscribe('t', (m) => received.push(m.payload));
  bus.pauseTopic('t', { mode: 'queue', maxBuffered: 1 });
  bus.publish('t', 'a');
  bus.publish('t', 'b'); // shed: buffer full
  assert.equal(bus.getStats().pausedMessages, 1);
  bus.setTopicPauseBufferLimit('t', null);
  assert.equal(bus.getTopicPauseBufferLimit('t'), undefined);
  assert.deepEqual(bus.getStats().pausedTopics, [
    { topicPattern: 't', mode: 'queue', bufferedMessages: 1 },
  ]);
  bus.publish('t', 'c');
  bus.publish('t', 'd');
  assert.equal(bus.getStats().pausedMessages, 1);
  assert.equal(bus.getStats().pausedTopics[0].bufferedMessages, 3);
  assert.equal(bus.resumeTopic('t'), true);
  await flush();
  assert.deepEqual(received, ['a', 'c', 'd']);
});

test('EB-401: revising an unknown pattern throws RangeError', () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  assert.throws(() => bus.setTopicPauseBufferLimit('nope', 5), RangeError);
  assert.throws(() => bus.setTopicPauseBufferLimit('', 5), RangeError);
  assert.throws(() => bus.setTopicPauseBufferLimit(42 as never, 5), TypeError);
  assert.throws(() => bus.setTopicPauseBufferLimit('nope', 5, ''), RangeError);
  assert.equal(bus.getTopicPauseBufferLimit('nope'), undefined);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'topic_pause_buffer_limit_revised' }).length,
    0,
  );
});

test('EB-401: shed mode plus maxBuffered throws RangeError at pause time', () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  assert.throws(() => bus.pauseTopic('t', { mode: 'shed', maxBuffered: 5 }), RangeError);
  assert.throws(() => bus.pauseTopic('t', { maxBuffered: 5 }), RangeError);
  assert.deepEqual(bus.getStats().pausedTopics, [], 'no rule registered by the failed call');
});

test('EB-401: an illegal bound throws RangeError and leaves the live bound untouched', () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  bus.pauseTopic('t', { mode: 'queue', maxBuffered: 2 });
  for (const bad of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, '2'] as unknown[]) {
    assert.throws(
      () => bus.setTopicPauseBufferLimit('t', bad as number),
      RangeError,
      `maxBuffered=${String(bad)}`,
    );
  }
  for (const bad of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY] as unknown[]) {
    assert.throws(
      () => bus.pauseTopic('u', { mode: 'queue', maxBuffered: bad as number }),
      RangeError,
      `pauseTopic maxBuffered=${String(bad)}`,
    );
  }
  assert.equal(bus.getTopicPauseBufferLimit('t'), 2);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'topic_pause_buffer_limit_revised' }).length,
    0,
  );
});

test('EB-401: revising to the current bound is a no-op with no audit', () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  bus.pauseTopic('t', { mode: 'queue', maxBuffered: 2 });
  bus.setTopicPauseBufferLimit('t', 2);
  // Clearing an already-unbounded rule is likewise a no-op.
  bus.pauseTopic('u', { mode: 'queue' });
  bus.setTopicPauseBufferLimit('u', null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'topic_pause_buffer_limit_revised' }).length,
    0,
  );
});

test('EB-401: audit detail carries previous/max exactly, with actor attribution, and the getter follows', () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  bus.pauseTopic('t', { mode: 'queue' });
  bus.setTopicPauseBufferLimit('t', 2, 'ops');
  bus.setTopicPauseBufferLimit('t', 5, 'ops');
  assert.equal(bus.getTopicPauseBufferLimit('t'), 5);
  const audit = bus.getSubscriptionAudit({ action: 'topic_pause_buffer_limit_revised' });
  assert.equal(audit.length, 2);
  assert.equal(audit[0].pattern, 't');
  assert.equal(audit[0].actor, 'ops');
  assert.deepEqual(audit[0].detail, { previous_max_buffered: null, max_buffered: 2 });
  assert.deepEqual(audit[1].detail, { previous_max_buffered: 2, max_buffered: 5 });
  bus.setTopicPauseBufferLimit('t', null, 'ops');
  const cleared = bus.getSubscriptionAudit({ action: 'topic_pause_buffer_limit_revised' });
  assert.equal(cleared.length, 3);
  assert.deepEqual(cleared[2].detail, { previous_max_buffered: 5, max_buffered: null });
});

test('EB-401: delayed-sweep deferral sheds on overflow and the schedule never re-fans', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, durableLogDir: freshDir() });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.pauseTopic('t', { mode: 'queue', maxBuffered: 1 });
  bus.publish('t', 'live'); // fills the buffer
  const id = bus.publishDelayed('t', 'late', { delayMs: 1000 });
  assert.match(id as string, /^delayed-\d+$/);
  assert.equal(bus.getStats().pendingDelayed, 1);
  clock.nowMs = 2_000;
  bus.publish('other', 'poke'); // the flush sweep finds the due entry paused
  await flush();
  // The due entry shed on the full buffer: schedule closed, never re-fanned.
  assert.deepEqual(received, []);
  assert.equal(bus.getStats().pendingDelayed, 0);
  assert.equal(bus.getStats().pausedTopics[0].bufferedMessages, 1);
  assert.equal(bus.getStats().pausedMessages, 1);
  // No sequence number burned on 't' by the overflow shed (the poke
  // publish to 'other' is the only seq the bus stamped).
  const tStats = bus.getStats().topics.find((s) => s.topic === 't');
  assert.equal(tStats?.lastSeq, 0, 'the overflow shed burned no seq');
  assert.equal(bus.resumeTopic('t'), true);
  await flush();
  assert.deepEqual(received, ['live']);
  // Advancing the clock further never resurrects the shed schedule.
  clock.nowMs = 5_000;
  bus.publish('other', 'poke');
  await flush();
  assert.deepEqual(received, ['live']);
});

test('EB-401: resume drains in original order and the bound drops nothing', async () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  const received: unknown[] = [];
  bus.subscribe('t', (m) => received.push(m.payload));
  bus.pauseTopic('t', { mode: 'queue', maxBuffered: 2 });
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  bus.publish('t', 'c'); // shed: buffer full
  assert.equal(bus.getStats().pausedMessages, 1);
  assert.equal(bus.resumeTopic('t'), true);
  await flush();
  assert.deepEqual(received, ['a', 'b']);
});
