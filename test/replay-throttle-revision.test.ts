import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type BusMessage, type ReplayProgressEvent } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-replay-throttle-rev-'));

/**
 * Waits until `received` holds `n` messages. Replay progress events
 * fire synchronously during the replay, but handler delivery drains in
 * chunks across flushes, so a single flush is not enough for a large
 * replay.
 */
async function drainUntil(received: unknown[], n: number): Promise<void> {
  for (let i = 0; i < 100 && received.length < n; i += 1) {
    await flush();
  }
  assert.equal(received.length, n);
}

/** The `replay_throttle_revised` audit events on the bus. */
function throttleRevisionAudits(bus: EventBus) {
  return bus.getSubscriptionAudit({ action: 'replay_throttle_revised' });
}

/** Publishes `n` messages to 't' and subscribes with a throttled resume replay. */
function busWithThrottle(
  n = 5,
  throttle: { messagesPerSec: number; burst: number } = { messagesPerSec: 10, burst: 2 },
  clock = controllableClock(),
) {
  const bus = new EventBus({ durableLogDir: freshDir(), now: clock.now });
  for (let i = 0; i < n; i += 1) bus.publish('t', i);
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    resumeFromSeq: 0,
    queueSize: n + 50,
    replayThrottle: throttle,
  });
  return { bus, sub, received, clock };
}

/** Advances the injected clock and flushes, so a paused pass resumes deterministically. */
async function advanceAndFlush(bus: EventBus, clock: { nowMs: number }, ms: number): Promise<void> {
  clock.nowMs += ms;
  bus.publish('zz', {}); // unmatched topic: only triggers a flush
  await flush();
}

/** Advances the injected clock in refill windows until `received` holds `n` messages. */
async function drainThrottled(bus: EventBus, clock: { nowMs: number }, received: unknown[], n: number): Promise<void> {
  for (let i = 0; i < 10 && received.length < n; i += 1) {
    await advanceAndFlush(bus, clock, 200);
  }
  assert.equal(received.length, n);
}

describe('replay throttle revision (EB-448)', () => {
  it('a revised rate paces the next seek replay', async () => {
    const { bus, sub, received, clock } = busWithThrottle();
    // First pass: burst of 2 admitted, the pass pauses at 2/5.
    await flush();
    assert.deepEqual(received, [0, 1]);
    assert.equal(bus.getStats().replayProgress[0].replayed, 2);
    assert.equal(bus.getStats().replayProgress[0].done, false);
    await drainThrottled(bus, clock, received, 5);
    assert.deepEqual(received, [0, 1, 2, 3, 4]);
    assert.equal(bus.getStats().replayProgress[0].done, true);

    // Retune only the rate — the burst stays 2.
    bus.setReplayThrottle(sub.id, { messagesPerSec: 1000 });
    assert.deepEqual(throttleRevisionAudits(bus)[0].detail, {
      throttle: { messagesPerSec: 1000, burst: 2 },
      oldThrottle: { messagesPerSec: 10, burst: 2 },
    });

    bus.seek(sub.id, 'earliest');
    await flush();
    assert.deepEqual(received, [0, 1, 2, 3, 4, 0, 1]);
    // +100ms at the new 1000 msgs/s refills the burst of 2 (the old
    // 10 msgs/s would refill only 1 token in the same window).
    await advanceAndFlush(bus, clock, 100);
    assert.deepEqual(received, [0, 1, 2, 3, 4, 0, 1, 2, 3]);
    await advanceAndFlush(bus, clock, 100);
    assert.deepEqual(received, [0, 1, 2, 3, 4, 0, 1, 2, 3, 4]);
    assert.equal(bus.getStats().replayProgress[0].done, true);
    bus.destroy?.();
  });

  it('a revised burst retunes the next seek replay', async () => {
    const { bus, sub, received, clock } = busWithThrottle();
    await flush();
    await drainThrottled(bus, clock, received, 5);
    assert.deepEqual(received, [0, 1, 2, 3, 4]);

    bus.setReplayThrottle(sub.id, { burst: 100 });
    assert.deepEqual(throttleRevisionAudits(bus)[0].detail, {
      throttle: { messagesPerSec: 10, burst: 100 },
      oldThrottle: { messagesPerSec: 10, burst: 2 },
    });

    bus.seek(sub.id, 'earliest');
    await flush();
    // The whole 5-entry pass fits the new burst: no pause, no refill wait.
    assert.deepEqual(received, [0, 1, 2, 3, 4, 0, 1, 2, 3, 4]);
    assert.equal(bus.getStats().replayThrottled, 0);
    bus.destroy?.();
  });

  it('a gap-repair replay likewise uses the new rate', async () => {
    const { bus, sub, received, clock } = busWithThrottle();
    await flush();
    await drainThrottled(bus, clock, received, 5);
    assert.deepEqual(received, [0, 1, 2, 3, 4]);

    bus.setReplayThrottle(sub.id, { burst: 100 });
    const result = bus.requestMissingReplay(sub.id, 1, 5);
    // All 5 fit the new burst, so the repair admits them synchronously.
    assert.equal(result.replayed, 5);
    assert.equal(result.unavailable, false);
    await flush();
    assert.deepEqual(received, [0, 1, 2, 3, 4, 0, 1, 2, 3, 4]);
    bus.destroy?.();
  });

  it('a revision mid-pass leaves the pass in flight on its start bucket; the next pass uses the new one', async () => {
    const { bus, sub, received, clock } = busWithThrottle();
    await flush();
    assert.deepEqual(received, [0, 1]); // paused at 2/5 on the old bucket

    bus.setReplayThrottle(sub.id, { messagesPerSec: 1000, burst: 100 });
    // +100ms: the in-flight pass keeps its start bucket (10 msgs/s,
    // burst 2) — one token refills, one record admitted. Had the new
    // bucket applied, all 3 remaining records would arrive at once.
    await advanceAndFlush(bus, clock, 100);
    assert.deepEqual(received, [0, 1, 2]);
    assert.equal(bus.getStats().replayProgress[0].done, false);
    await advanceAndFlush(bus, clock, 100);
    assert.deepEqual(received, [0, 1, 2, 3]);
    await advanceAndFlush(bus, clock, 100);
    assert.deepEqual(received, [0, 1, 2, 3, 4]);
    assert.equal(bus.getStats().replayProgress[0].done, true);

    // The next pass builds its bucket from the live config.
    bus.seek(sub.id, 'earliest');
    await flush();
    assert.deepEqual(received, [0, 1, 2, 3, 4, 0, 1, 2, 3, 4]);
    bus.destroy?.();
  });

  it('clearing with null leaves the next pass unthrottled', async () => {
    const { bus, sub, received, clock } = busWithThrottle();
    await flush();
    await drainThrottled(bus, clock, received, 5);
    assert.deepEqual(received, [0, 1, 2, 3, 4]);

    bus.setReplayThrottle(sub.id, null);
    assert.deepEqual(throttleRevisionAudits(bus)[0].detail, {
      throttle: null,
      oldThrottle: { messagesPerSec: 10, burst: 2 },
    });

    bus.seek(sub.id, 'earliest');
    await flush();
    assert.deepEqual(received, [0, 1, 2, 3, 4, 0, 1, 2, 3, 4]);
    assert.equal(bus.getStats().replayThrottled, 0);
    // The gate is the live config, not the history: a cleared
    // throttle cannot be retuned until the subscription opts in again.
    assert.throws(() => bus.setReplayThrottle(sub.id, { messagesPerSec: 5 }), RangeError);
    bus.destroy?.();
  });

  it('unknown, empty, never-configured, and illegal values throw RangeError with the live throttle untouched', async () => {
    const { bus, sub, received, clock } = busWithThrottle();
    await flush();
    await drainThrottled(bus, clock, received, 5);
    assert.deepEqual(received, [0, 1, 2, 3, 4]);

    // No throttle at all: a plain subscription, and a resume replay
    // that ran unthrottled — retuning a throttle nobody configured is
    // a different decision from retuning an existing one.
    const plain = bus.subscribe('plain', () => {});
    const unthrottled = bus.subscribe('unthr', () => {}, { resumeFromSeq: 0 });
    await flush();
    const bad = (value: unknown) =>
      assert.throws(() => bus.setReplayThrottle(sub.id, value as never), RangeError);
    assert.throws(() => bus.setReplayThrottle('', { messagesPerSec: 5 }), RangeError);
    assert.throws(() => bus.setReplayThrottle('no-such-sub', { messagesPerSec: 5 }), RangeError);
    assert.throws(() => bus.setReplayThrottle(plain.id, { messagesPerSec: 5 }), RangeError);
    assert.throws(() => bus.setReplayThrottle(unthrottled.id, { messagesPerSec: 5 }), RangeError);
    bad({});
    bad({ messagesPerSec: 0 });
    bad({ messagesPerSec: -1 });
    bad({ messagesPerSec: Number.NaN });
    bad({ messagesPerSec: Number.POSITIVE_INFINITY });
    bad({ burst: 0 });
    bad({ burst: 0.5 });
    bad({ burst: Number.NaN });
    bad('fast');
    bad(42);
    bad(undefined);
    bad([]);
    assert.throws(() => bus.setReplayThrottle(sub.id, { messagesPerSec: 5 }, ''), RangeError);
    assert.equal(throttleRevisionAudits(bus).length, 0, 'failed revisions leave no audit');

    // The live throttle is untouched: the next seek replay still
    // paces at the old 10 msgs/s with a burst of 2.
    bus.seek(sub.id, 'earliest');
    await flush();
    assert.deepEqual(received, [0, 1, 2, 3, 4, 0, 1]);
    assert.equal(bus.getStats().replayProgress[0].done, false);

    // A closed bus refuses the revision (EB-154).
    const closedBus = busWithThrottle(3);
    closedBus.bus.close();
    assert.throws(
      () => closedBus.bus.setReplayThrottle(closedBus.sub.id, { messagesPerSec: 5 }),
      /bus is closed/,
    );
    bus.destroy?.();
  });

  it('re-affirming the live throttle is a silent no-op', async () => {
    const { bus, sub, received, clock } = busWithThrottle();
    await flush();
    await drainThrottled(bus, clock, received, 5);
    assert.deepEqual(received, [0, 1, 2, 3, 4]);

    bus.setReplayThrottle(sub.id, { messagesPerSec: 10, burst: 2 });
    bus.setReplayThrottle(sub.id, { messagesPerSec: 10 });
    bus.setReplayThrottle(sub.id, { burst: 2 });
    assert.equal(throttleRevisionAudits(bus).length, 0, 'no-op revisions leave no audit');

    // The live throttle still paces the next pass at the old rate.
    bus.seek(sub.id, 'earliest');
    await flush();
    assert.deepEqual(received, [0, 1, 2, 3, 4, 0, 1]);
    bus.destroy?.();
  });

  it('audit event records the new and old throttle exactly, and the actor', async () => {
    const { bus, sub, received, clock } = busWithThrottle();
    await flush();
    await drainThrottled(bus, clock, received, 5);
    assert.deepEqual(received, [0, 1, 2, 3, 4]);

    bus.setReplayThrottle(sub.id, { messagesPerSec: 20, burst: 5 }, 'operator:alice');
    const audits = throttleRevisionAudits(bus);
    assert.equal(audits.length, 1);
    assert.equal(audits[0].action, 'replay_throttle_revised');
    assert.equal(audits[0].actor, 'operator:alice');
    assert.equal(audits[0].subId, sub.id);
    assert.equal(audits[0].pattern, 't');
    assert.deepEqual(audits[0].detail, {
      throttle: { messagesPerSec: 20, burst: 5 },
      oldThrottle: { messagesPerSec: 10, burst: 2 },
    });
    bus.destroy?.();
  });

  it("a group member's throttle is revised the same way", async () => {
    const clock = controllableClock();
    const bus = new EventBus({ durableLogDir: freshDir(), now: clock.now });
    for (let i = 0; i < 5; i += 1) bus.publish('t', i);
    const received: unknown[] = [];
    const member = bus.subscribeToGroup('g1', 't', (msg) => received.push(msg.payload), {
      resumeFromSeq: 0,
      queueSize: 55,
      replayThrottle: { messagesPerSec: 10, burst: 2 },
    });
    await flush();
    assert.deepEqual(received, [0, 1]);
    await drainThrottled(bus, clock, received, 5);
    assert.deepEqual(received, [0, 1, 2, 3, 4]);

    bus.setReplayThrottle(member.id, { burst: 100 });
    assert.deepEqual(throttleRevisionAudits(bus)[0].detail, {
      throttle: { messagesPerSec: 10, burst: 100 },
      oldThrottle: { messagesPerSec: 10, burst: 2 },
    });

    bus.seekGroupOffsets('g1', 't', { t: 'earliest' });
    await drainUntil(received, 10);
    assert.deepEqual(received, [0, 1, 2, 3, 4, 0, 1, 2, 3, 4]);
    bus.destroy?.();
  });

  it('revising the throttle does not move the progress observer or the interval', async () => {
    const clock = controllableClock();
    const bus = new EventBus({ durableLogDir: freshDir(), now: clock.now });
    for (let i = 0; i < 250; i += 1) bus.publish('t', i);
    const received: BusMessage[] = [];
    const events: ReplayProgressEvent[] = [];
    const sub = bus.subscribe('t', (msg) => void received.push(msg), {
      resumeFromSeq: 0,
      queueSize: 300,
      replayThrottle: { messagesPerSec: 10, burst: 300 },
      onReplayProgress: (e) => void events.push({ ...e }),
      progressInterval: 100,
    });
    await drainUntil(received, 250);
    const throttledCounts = (evts: ReplayProgressEvent[]) =>
      evts.filter((e) => !e.done && !e.failed).map((e) => e.replayed);
    assert.deepEqual(throttledCounts(events), [100, 200]);
    events.length = 0;

    bus.setReplayThrottle(sub.id, { messagesPerSec: 5 });
    bus.seek(sub.id, 'earliest');
    await drainUntil(received, 500);
    // The observer still fires at the old interval, and the completion
    // event still fires exactly once for the whole pass.
    assert.deepEqual(throttledCounts(events), [100, 200]);
    const done = events.filter((e) => e.done);
    assert.equal(done.length, 1);
    assert.equal(done[0].replayed, 250);
    sub.unsubscribe();
    bus.destroy?.();
  });
});
