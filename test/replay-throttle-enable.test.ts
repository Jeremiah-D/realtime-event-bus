import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-replay-throttle-en-'));

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

/**
 * Publishes `n` messages to 't' and subscribes plainly — no resume
 * cursor, no `replayThrottle` opt-in — so the subscriber starts
 * unthrottled and can only gain a throttle through `setReplayThrottle`.
 */
function busWithPlainSub(n = 5, clock = controllableClock()) {
  const bus = new EventBus({ durableLogDir: freshDir(), now: clock.now });
  for (let i = 0; i < n; i += 1) bus.publish('t', i);
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), { queueSize: n + 50 });
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

describe('replay throttle enable (EB-534)', () => {
  it('enabling paces the next seek replay', async () => {
    const { bus, sub, received, clock } = busWithPlainSub();
    await flush();
    assert.deepEqual(received, []);

    bus.setReplayThrottle(sub.id, { messagesPerSec: 10, burst: 2 });
    // An enable is audited like a revision, with oldThrottle null.
    assert.deepEqual(throttleRevisionAudits(bus)[0].detail, {
      throttle: { messagesPerSec: 10, burst: 2 },
      oldThrottle: null,
    });

    bus.seek(sub.id, 'earliest');
    await flush();
    // First pass: burst of 2 admitted, the pass pauses at 2/5.
    assert.deepEqual(received, [0, 1]);
    assert.equal(bus.getStats().replayProgress[0].replayed, 2);
    assert.equal(bus.getStats().replayProgress[0].done, false);
    await drainThrottled(bus, clock, received, 5);
    assert.deepEqual(received, [0, 1, 2, 3, 4]);
    assert.equal(bus.getStats().replayProgress[0].done, true);
    bus.destroy?.();
  });

  it('a gap-repair replay likewise uses the new rate', async () => {
    const { bus, sub, received, clock } = busWithPlainSub();
    await flush();

    bus.setReplayThrottle(sub.id, { messagesPerSec: 10, burst: 2 });
    const result = bus.requestMissingReplay(sub.id, 1, 5);
    // Only the burst fits at first: the repair pauses at 2/5.
    assert.equal(result.replayed, 2);
    assert.equal(result.unavailable, false);
    await flush();
    assert.deepEqual(received, [0, 1]);
    await drainThrottled(bus, clock, received, 5);
    assert.deepEqual(received, [0, 1, 2, 3, 4]);
    bus.destroy?.();
  });

  it('true enables the subscribe-time default of 1000/1000', async () => {
    const { bus, sub, received } = busWithPlainSub();
    await flush();

    bus.setReplayThrottle(sub.id, true, 'operator:bob');
    const audits = throttleRevisionAudits(bus);
    assert.equal(audits.length, 1);
    assert.equal(audits[0].action, 'replay_throttle_revised');
    assert.equal(audits[0].actor, 'operator:bob');
    assert.equal(audits[0].subId, sub.id);
    assert.equal(audits[0].pattern, 't');
    assert.deepEqual(audits[0].detail, {
      throttle: { messagesPerSec: 1000, burst: 1000 },
      oldThrottle: null,
    });

    bus.seek(sub.id, 'earliest');
    await flush();
    // The whole 5-entry pass fits the default burst: no pause, no refill wait.
    assert.deepEqual(received, [0, 1, 2, 3, 4]);
    assert.equal(bus.getStats().replayThrottled, 0);
    bus.destroy?.();
  });

  it('messagesPerSec alone defaults burst to ceil(messagesPerSec)', async () => {
    const { bus: busA, sub: subA } = busWithPlainSub();
    busA.setReplayThrottle(subA.id, { messagesPerSec: 7 });
    assert.deepEqual(throttleRevisionAudits(busA)[0].detail, {
      throttle: { messagesPerSec: 7, burst: 7 },
      oldThrottle: null,
    });
    busA.destroy?.();

    const { bus: busB, sub: subB } = busWithPlainSub();
    busB.setReplayThrottle(subB.id, { messagesPerSec: 2.5 });
    assert.deepEqual(throttleRevisionAudits(busB)[0].detail, {
      throttle: { messagesPerSec: 2.5, burst: 3 },
      oldThrottle: null,
    });
    busB.destroy?.();
  });

  it('enabling without a durable log throws RangeError and leaves nothing behind', async () => {
    const bus = new EventBus({});
    const sub = bus.subscribe('t', () => {});
    await flush();

    assert.throws(
      () => bus.setReplayThrottle(sub.id, { messagesPerSec: 10 }),
      /enabling replay throttle requires a durable log/,
    );
    assert.throws(
      () => bus.setReplayThrottle(sub.id, true),
      /enabling replay throttle requires a durable log/,
    );
    // A failed enable leaves the live throttle — still none — and
    // the audit log untouched: `null` is still a silent no-op.
    assert.equal(throttleRevisionAudits(bus).length, 0);
    bus.setReplayThrottle(sub.id, null);
    assert.equal(throttleRevisionAudits(bus).length, 0);
    bus.destroy?.();
  });

  it('null on a never-throttled subscriber is a silent no-op with no audit', async () => {
    const { bus, sub, received } = busWithPlainSub();
    await flush();

    bus.setReplayThrottle(sub.id, null);
    assert.equal(throttleRevisionAudits(bus).length, 0, 'a null no-op leaves no audit');

    bus.seek(sub.id, 'earliest');
    await flush();
    // Unpaced: the whole pass admits at once.
    assert.deepEqual(received, [0, 1, 2, 3, 4]);
    assert.equal(bus.getStats().replayThrottled, 0);
    bus.destroy?.();
  });

  it('a cleared throttle can be enabled again, and the next pass paces', async () => {
    const clock = controllableClock();
    const bus = new EventBus({ durableLogDir: freshDir(), now: clock.now });
    for (let i = 0; i < 5; i += 1) bus.publish('t', i);
    const received: unknown[] = [];
    const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
      resumeFromSeq: 0,
      queueSize: 55,
      replayThrottle: { messagesPerSec: 10, burst: 2 },
    });
    await flush();
    await drainThrottled(bus, clock, received, 5);
    assert.deepEqual(received, [0, 1, 2, 3, 4]);

    bus.setReplayThrottle(sub.id, null);
    assert.deepEqual(throttleRevisionAudits(bus)[0].detail, {
      throttle: null,
      oldThrottle: { messagesPerSec: 10, burst: 2 },
    });
    // The clear is not terminal: a fresh enable is allowed, audited
    // with oldThrottle null.
    bus.setReplayThrottle(sub.id, { messagesPerSec: 10, burst: 2 });
    assert.deepEqual(throttleRevisionAudits(bus)[1].detail, {
      throttle: { messagesPerSec: 10, burst: 2 },
      oldThrottle: null,
    });

    bus.seek(sub.id, 'earliest');
    await flush();
    assert.deepEqual(received, [0, 1, 2, 3, 4, 0, 1]);
    assert.equal(bus.getStats().replayProgress[0].done, false);
    await drainThrottled(bus, clock, received, 10);
    assert.deepEqual(received, [0, 1, 2, 3, 4, 0, 1, 2, 3, 4]);
    bus.destroy?.();
  });

  it('a partial revision after enabling still merges the live values', async () => {
    const { bus, sub, received, clock } = busWithPlainSub();
    await flush();

    bus.setReplayThrottle(sub.id, { messagesPerSec: 10, burst: 2 });
    bus.setReplayThrottle(sub.id, { messagesPerSec: 1000 });
    assert.deepEqual(throttleRevisionAudits(bus)[1].detail, {
      throttle: { messagesPerSec: 1000, burst: 2 },
      oldThrottle: { messagesPerSec: 10, burst: 2 },
    });

    bus.seek(sub.id, 'earliest');
    await flush();
    assert.deepEqual(received, [0, 1]);
    // +100ms at the new 1000 msgs/s refills the burst of 2 (the old
    // 10 msgs/s would refill only 1 token in the same window).
    await advanceAndFlush(bus, clock, 100);
    assert.deepEqual(received, [0, 1, 2, 3]);
    await advanceAndFlush(bus, clock, 100);
    assert.deepEqual(received, [0, 1, 2, 3, 4]);
    bus.destroy?.();
  });

  it('enabling, then revising mid-pass, leaves the pass in flight on its start bucket', async () => {
    const { bus, sub, received, clock } = busWithPlainSub();
    await flush();

    bus.setReplayThrottle(sub.id, { messagesPerSec: 10, burst: 2 });
    bus.seek(sub.id, 'earliest');
    await flush();
    assert.deepEqual(received, [0, 1]); // paused at 2/5 on the enabled bucket

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

  it("a group member's throttle is enabled the same way", async () => {
    const clock = controllableClock();
    const bus = new EventBus({ durableLogDir: freshDir(), now: clock.now });
    for (let i = 0; i < 5; i += 1) bus.publish('t', i);
    const received: unknown[] = [];
    const member = bus.subscribeToGroup('g1', 't', (msg) => received.push(msg.payload), {
      queueSize: 55,
    });
    await flush();
    // The group's initial backlog replay runs unthrottled: the member
    // has no throttle yet.
    assert.deepEqual(received, [0, 1, 2, 3, 4]);

    bus.setReplayThrottle(member.id, { messagesPerSec: 10, burst: 2 });
    assert.deepEqual(throttleRevisionAudits(bus)[0].detail, {
      throttle: { messagesPerSec: 10, burst: 2 },
      oldThrottle: null,
    });

    bus.seekGroupOffsets('g1', 't', { t: 'earliest' });
    await flush();
    assert.deepEqual(received, [0, 1, 2, 3, 4, 0, 1]);
    await drainThrottled(bus, clock, received, 10);
    assert.deepEqual(received, [0, 1, 2, 3, 4, 0, 1, 2, 3, 4]);
    bus.destroy?.();
  });

  it('unknown, empty, and illegal values throw RangeError with the live throttle untouched', async () => {
    const { bus, sub, received } = busWithPlainSub();
    await flush();

    const bad = (value: unknown) =>
      assert.throws(() => bus.setReplayThrottle(sub.id, value as never), RangeError);
    assert.throws(() => bus.setReplayThrottle('', { messagesPerSec: 5 }), RangeError);
    assert.throws(() => bus.setReplayThrottle('no-such-sub', { messagesPerSec: 5 }), RangeError);
    // An enable patch must set messagesPerSec: there is no live
    // value to merge a burst-only patch against.
    bad({});
    bad({ burst: 5 });
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
    assert.equal(throttleRevisionAudits(bus).length, 0, 'failed enables leave no audit');

    // The live throttle is still none: the next seek replay runs unpaced.
    bus.seek(sub.id, 'earliest');
    await flush();
    assert.deepEqual(received, [0, 1, 2, 3, 4]);
    assert.equal(bus.getStats().replayThrottled, 0);

    // A closed bus refuses the enable (EB-154).
    const closed = busWithPlainSub(3);
    closed.bus.close();
    assert.throws(
      () => closed.bus.setReplayThrottle(closed.sub.id, { messagesPerSec: 5 }),
      /bus is closed/,
    );
    bus.destroy?.();
  });
});
