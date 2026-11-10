import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type BusMessage, type ReplayProgressEvent } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/**
 * Waits until `received` holds `n` messages. Replay progress events
 * fire synchronously during the replay, but handler delivery drains in
 * chunks across flushes, so a single flush is not enough for a large
 * replay.
 */
async function drainUntil(received: BusMessage[], n: number): Promise<void> {
  for (let i = 0; i < 100 && received.length < n; i += 1) {
    await flush();
  }
  assert.equal(received.length, n);
}

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-replayprogress-interval-rev-'));

/** The `replay_progress_interval_revised` audit events on the bus. */
function intervalRevisionAudits(bus: EventBus) {
  return bus.getSubscriptionAudit({ action: 'replay_progress_interval_revised' });
}

/** Publishes `n` messages to 't' and subscribes with a resume replay. */
function busWithReplay(
  n = 250,
  opts: { onReplayProgress?: (e: ReplayProgressEvent) => void; progressInterval?: number } = {},
) {
  const bus = new EventBus({ durableLogDir: freshDir() });
  for (let i = 0; i < n; i += 1) bus.publish('t', `m${i}`);
  const received: BusMessage[] = [];
  const sub = bus.subscribe('t', (msg) => void received.push(msg), {
    resumeFromSeq: 0,
    // The per-subscriber queue defaults to 100: a 250-entry replay
    // would shed past the cap, so size it past the replay.
    queueSize: n + 50,
    // progressInterval requires an observer; only default it when one
    // is passed (callers may still override it via opts).
    ...(opts.onReplayProgress !== undefined ? { progressInterval: 100 } : {}),
    ...opts,
  });
  return { bus, sub, received };
}

/** The `replayed` counts of the non-completion fires, in order. */
const throttledCounts = (events: ReplayProgressEvent[]) =>
  events.filter((e) => !e.done && !e.failed).map((e) => e.replayed);

describe('replay progress interval revision (EB-447)', () => {
  it('fires at the default interval of 100, plus once on completion', async () => {
    const events: ReplayProgressEvent[] = [];
    const { bus, sub, received } = busWithReplay(250, {
      onReplayProgress: (e) => void events.push({ ...e }),
    });
    await drainUntil(received, 250);
    assert.deepEqual(throttledCounts(events), [100, 200]);
    const done = events.filter((e) => e.done);
    assert.equal(done.length, 1);
    assert.equal(done[0].replayed, 250);
    assert.equal(done[0].total, 250);
    sub.unsubscribe();
  });

  it('a smaller interval retunes the next seek replay to fire more often', async () => {
    const events: ReplayProgressEvent[] = [];
    const { bus, sub, received } = busWithReplay(250, {
      onReplayProgress: (e) => void events.push({ ...e }),
    });
    await drainUntil(received, 250);
    assert.deepEqual(throttledCounts(events), [100, 200]);
    events.length = 0;

    bus.setReplayProgressInterval(sub.id, 50);
    bus.seek(sub.id, 'earliest');
    await drainUntil(received, 500);
    // The interval divides the total, so the last throttled fire
    // lands exactly on 250, then the completion event follows.
    assert.deepEqual(throttledCounts(events), [50, 100, 150, 200, 250]);
    const done = events.filter((e) => e.done);
    assert.equal(done.length, 1);
    assert.equal(done[0].replayed, 250);
    assert.deepEqual(intervalRevisionAudits(bus)[0].detail, {
      progressInterval: 50,
      oldProgressInterval: 100,
    });
    sub.unsubscribe();
  });

  it('a larger interval retunes the next seek replay to fire only on completion', async () => {
    const events: ReplayProgressEvent[] = [];
    const { bus, sub, received } = busWithReplay(250, {
      onReplayProgress: (e) => void events.push({ ...e }),
    });
    await drainUntil(received, 250);
    events.length = 0;

    bus.setReplayProgressInterval(sub.id, 1000);
    bus.seek(sub.id, 'earliest');
    await drainUntil(received, 500);
    assert.deepEqual(throttledCounts(events), []);
    const done = events.filter((e) => e.done);
    assert.equal(done.length, 1, 'the completion event always fires exactly once');
    assert.equal(done[0].replayed, 250);
    assert.deepEqual(intervalRevisionAudits(bus)[0].detail, {
      progressInterval: 1000,
      oldProgressInterval: 100,
    });
    sub.unsubscribe();
  });

  it('a revision mid-pass leaves the pass in flight on its start interval; the next pass uses the new one', async () => {
    // Built inline (not via busWithReplay): the observer fires during
    // the subscribe replay itself, so it must close over a `bus` that
    // already exists and read the subscriber id from the event —
    // closing over the `sub` const would hit its TDZ.
    const bus = new EventBus({ durableLogDir: freshDir() });
    for (let i = 0; i < 250; i += 1) bus.publish('t', `m${i}`);
    const received: BusMessage[] = [];
    const events: ReplayProgressEvent[] = [];
    let revised = false;
    const sub = bus.subscribe('t', (msg) => void received.push(msg), {
      resumeFromSeq: 0,
      progressInterval: 100,
      queueSize: 300,
      onReplayProgress: (e) => {
        events.push({ ...e });
        // Retune from inside the observer, mid-pass: the pass in
        // flight must keep the interval it started with (100).
        if (!revised && !e.done) {
          revised = true;
          bus.setReplayProgressInterval(e.subscriberId, 1);
        }
      },
    });
    await drainUntil(received, 250);
    assert.equal(revised, true, 'the observer revised the interval mid-pass');
    // The in-flight pass still fired at 100/200/done, not at 1.
    assert.deepEqual(throttledCounts(events), [100, 200]);
    assert.equal(events.filter((e) => e.done).length, 1);
    events.length = 0;

    // The next pass reads the live interval: every entry fires.
    bus.seek(sub.id, 'earliest');
    await drainUntil(received, 500);
    assert.equal(throttledCounts(events).length, 250);
    assert.deepEqual(throttledCounts(events).slice(0, 3), [1, 2, 3]);
    assert.equal(events.filter((e) => e.done).length, 1);
    assert.deepEqual(intervalRevisionAudits(bus)[0].detail, {
      progressInterval: 1,
      oldProgressInterval: 100,
    });
    sub.unsubscribe();
  });

  it('a group member seek likewise uses the new interval', async () => {
    const bus = new EventBus({ durableLogDir: freshDir() });
    for (let i = 0; i < 250; i += 1) bus.publish('t', `m${i}`);
    const events: ReplayProgressEvent[] = [];
    const sub = bus.subscribeToGroup('g', 't', () => {}, {
      resumeFromSeq: 0,
      progressInterval: 100,
      queueSize: 300,
      onReplayProgress: (e) => void events.push({ ...e }),
    });
    await flush();
    assert.deepEqual(throttledCounts(events), [100, 200]);
    events.length = 0;

    bus.setReplayProgressInterval(sub.id, 50);
    bus.seekGroupOffsets('g', 't', { t: 'earliest' });
    await flush();
    // The interval divides the total, so the last throttled fire lands
    // exactly on 250, then the completion event follows.
    assert.deepEqual(throttledCounts(events), [50, 100, 150, 200, 250]);
    const done = events.filter((e) => e.done);
    assert.equal(done.length, 1);
    assert.equal(done[0].subscriberId, sub.id);
    assert.equal(done[0].pattern, 't');
    assert.equal(done[0].total, 250);
    assert.deepEqual(intervalRevisionAudits(bus)[0].detail, {
      progressInterval: 50,
      oldProgressInterval: 100,
    });
    sub.unsubscribe();
  });

  it('a gap-repair replay likewise uses the new interval', async () => {
    const events: ReplayProgressEvent[] = [];
    const { bus, sub, received } = busWithReplay(250, {
      onReplayProgress: (e) => void events.push({ ...e }),
    });
    await flush();
    events.length = 0;

    bus.setReplayProgressInterval(sub.id, 50);
    const result = bus.requestMissingReplay(sub.id, 1, 250);
    assert.equal(result.replayed, 250);
    assert.equal(result.unavailable, false);
    await drainUntil(received, 500);
    assert.deepEqual(throttledCounts(events), [50, 100, 150, 200, 250]);
    const done = events.filter((e) => e.done);
    assert.equal(done.length, 1);
    assert.equal(done[0].total, 250);
    sub.unsubscribe();
  });

  it('unknown, empty, observer-less and illegal values throw RangeError with the live interval untouched', async () => {
    const events: ReplayProgressEvent[] = [];
    const cb = (e: ReplayProgressEvent) => void events.push({ ...e });
    const { bus, sub, received } = busWithReplay(250, { onReplayProgress: cb });
    await drainUntil(received, 250);
    events.length = 0;

    // No observer at all: a plain subscription, and a resume replay
    // that ran unobserved — tuning an interval nobody hears is
    // meaningless, the mirror of the subscribe-time rule.
    const plain = bus.subscribe('plain', () => {});
    const unobserved = bus.subscribe('unobs', () => {}, { resumeFromSeq: 0 });
    await flush();
    const bad = (value: unknown) =>
      assert.throws(() => bus.setReplayProgressInterval(sub.id, value as never), RangeError);
    assert.throws(() => bus.setReplayProgressInterval('', 50), RangeError);
    assert.throws(() => bus.setReplayProgressInterval('no-such-sub', 50), RangeError);
    assert.throws(() => bus.setReplayProgressInterval(plain.id, 50), RangeError);
    assert.throws(() => bus.setReplayProgressInterval(unobserved.id, 50), RangeError);
    bad(0);
    bad(-1);
    bad(1.5);
    bad(Number.NaN);
    bad(Number.POSITIVE_INFINITY);
    bad('50');
    bad(null);
    bad(undefined);
    bad({});
    assert.equal(intervalRevisionAudits(bus).length, 0, 'failed revisions leave no audit');

    // The live interval is untouched: the next seek replay still
    // fires at 100/200/done.
    bus.seek(sub.id, 'earliest');
    await flush();
    assert.deepEqual(throttledCounts(events), [100, 200]);
    assert.equal(events.filter((e) => e.done).length, 1);
    assert.equal(intervalRevisionAudits(bus).length, 0);
    sub.unsubscribe();
    plain.unsubscribe();
    unobserved.unsubscribe();
  });

  it('re-affirming the live interval is a no-op with no audit', async () => {
    const events: ReplayProgressEvent[] = [];
    const { bus, sub } = busWithReplay(5, {
      progressInterval: 2,
      onReplayProgress: (e) => void events.push({ ...e }),
    });
    await flush();
    bus.setReplayProgressInterval(sub.id, 2);
    assert.equal(intervalRevisionAudits(bus).length, 0);
    bus.seek(sub.id, 'earliest');
    await flush();
    assert.deepEqual(throttledCounts(events), [2, 4, 2, 4]);
    sub.unsubscribe();
  });

  it('revising the interval leaves the observer identity and the replay state shape untouched', async () => {
    const events: ReplayProgressEvent[] = [];
    const cb = (e: ReplayProgressEvent) => void events.push({ ...e });
    const { bus, sub, received } = busWithReplay(250, { onReplayProgress: cb });
    await drainUntil(received, 250);
    const before = bus.getStats().replayProgress[0];
    assert.deepEqual(
      { total: before.total, replayed: before.replayed, skipped: before.skipped, done: before.done, failed: before.failed },
      { total: 250, replayed: 250, skipped: 0, done: true, failed: false },
    );
    events.length = 0;

    bus.setReplayProgressInterval(sub.id, 50);
    bus.seek(sub.id, 'earliest');
    await drainUntil(received, 500);
    // The EB-443 observer is still the same function: it kept firing.
    assert.ok(events.length > 0, 'the same observer still governs the next pass');
    assert.equal(events.filter((e) => e.done).length, 1);
    const after = bus.getStats().replayProgress[0];
    assert.deepEqual(
      { total: after.total, replayed: after.replayed, skipped: after.skipped, done: after.done, failed: after.failed },
      { total: 250, replayed: 250, skipped: 0, done: true, failed: false },
    );
    // A callback swap after the interval revision still works: the two
    // revision entries are independent.
    const second: ReplayProgressEvent[] = [];
    bus.setReplayProgressCallback(sub.id, (e) => void second.push({ ...e }));
    events.length = 0;
    bus.seek(sub.id, 'earliest');
    await drainUntil(received, 750);
    assert.equal(events.length, 0, 'the old callback never fires again');
    assert.deepEqual(throttledCounts(second), [50, 100, 150, 200, 250]);
    assert.equal(second.filter((e) => e.done).length, 1);
    sub.unsubscribe();
  });

  it('a closed bus refuses the revision', async () => {
    const { bus, sub } = busWithReplay(5, {
      onReplayProgress: () => {},
    });
    await flush();
    await bus.close();
    assert.throws(() => bus.setReplayProgressInterval(sub.id, 50), RangeError);
    assert.equal(intervalRevisionAudits(bus).length, 0);
  });

  it('a closed subscription throws RangeError', async () => {
    const { bus, sub } = busWithReplay(5, { onReplayProgress: () => {} });
    await flush();
    sub.unsubscribe();
    assert.throws(() => bus.setReplayProgressInterval(sub.id, 50), RangeError);
    assert.equal(intervalRevisionAudits(bus).length, 0);
  });
});
