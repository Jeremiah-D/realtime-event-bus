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

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-replayprogress-rev-'));

/** The `replay_progress_callback_revised` audit events on the bus. */
function callbackRevisionAudits(bus: EventBus) {
  return bus.getSubscriptionAudit({ action: 'replay_progress_callback_revised' });
}

/** Publishes `n` messages to 't' and subscribes with a resume replay. */
function busWithReplay(
  n = 5,
  opts: { onReplayProgress?: (e: ReplayProgressEvent) => void; progressInterval?: number } = {},
) {
  const bus = new EventBus({ durableLogDir: freshDir() });
  for (let i = 0; i < n; i += 1) bus.publish('t', `m${i}`);
  const received: BusMessage[] = [];
  const sub = bus.subscribe('t', (msg) => void received.push(msg), {
    resumeFromSeq: 0,
    // progressInterval requires an observer; only default it when one
    // is passed (callers may still override it via opts).
    ...(opts.onReplayProgress !== undefined ? { progressInterval: 1 } : {}),
    ...opts,
  });
  return { bus, sub, received };
}

describe('replay progress callback revision (EB-443)', () => {
  it('swapping the callback routes the next seek replay to the new callback with exact event fields', async () => {
    const first: ReplayProgressEvent[] = [];
    const second: ReplayProgressEvent[] = [];
    const { bus, sub, received } = busWithReplay(5, {
      onReplayProgress: (e) => void first.push({ ...e }),
    });
    await flush();
    assert.equal(received.length, 5);
    const firstDone = first.filter((e) => e.done);
    assert.equal(firstDone.length, 1);
    assert.deepEqual(
      { ...firstDone[0] },
      {
        subscriberId: sub.id,
        pattern: 't',
        total: 5,
        replayed: 5,
        skipped: 0,
        done: true,
        failed: false,
      },
    );

    // Swap, then rewind: the whole second pass reports to the new
    // callback only, with the same exact event shape.
    bus.setReplayProgressCallback(sub.id, (e) => void second.push({ ...e }));
    bus.seek(sub.id, 'earliest');
    await flush();
    assert.equal(first.length, 6, 'the old callback never fires again');
    assert.equal(second.length, 6);
    const secondDone = second.filter((e) => e.done);
    assert.equal(secondDone.length, 1);
    assert.deepEqual(
      { ...secondDone[0] },
      {
        subscriberId: sub.id,
        pattern: 't',
        total: 5,
        replayed: 5,
        skipped: 0,
        done: true,
        failed: false,
      },
    );
    assert.equal(received.length, 10, 'the seek replay redelivers');
    assert.deepEqual(callbackRevisionAudits(bus)[0].detail, {
      hasCallback: true,
      oldHasCallback: true,
    });
    sub.unsubscribe();
  });

  it('a gap-repair replay likewise goes to the new callback', async () => {
    const first: ReplayProgressEvent[] = [];
    const second: ReplayProgressEvent[] = [];
    const { bus, sub, received } = busWithReplay(5, {
      onReplayProgress: (e) => void first.push({ ...e }),
    });
    await flush();
    assert.equal(received.length, 5);
    first.length = 0;

    bus.setReplayProgressCallback(sub.id, (e) => void second.push({ ...e }));
    const result = bus.requestMissingReplay(sub.id, 1, 5);
    assert.equal(result.replayed, 5);
    assert.equal(result.unavailable, false);
    await flush();
    assert.equal(first.length, 0, 'the old callback never fires again');
    const done = second.filter((e) => e.done);
    assert.equal(done.length, 1);
    assert.equal(done[0].total, 5);
    assert.equal(done[0].replayed, 5);
    assert.equal(done[0].skipped, 0);
    assert.equal(done[0].failed, false);
    assert.equal(done[0].subscriberId, sub.id);
    assert.equal(done[0].pattern, 't');
    assert.equal(received.length, 10);
    sub.unsubscribe();
  });

  it('clearing with null keeps the replay running and the stats updating with no callback', async () => {
    const events: ReplayProgressEvent[] = [];
    const { bus, sub, received } = busWithReplay(5, {
      onReplayProgress: (e) => void events.push({ ...e }),
    });
    await flush();
    assert.equal(received.length, 5);
    assert.ok(events.length > 0);
    events.length = 0;

    bus.setReplayProgressCallback(sub.id, null);
    bus.seek(sub.id, 'earliest');
    await flush();
    assert.equal(events.length, 0, 'no callback fires after the clear');
    assert.equal(received.length, 10, 'the seek replay still redelivers');
    const rows = bus.getStats().replayProgress;
    assert.equal(rows.length, 1);
    assert.deepEqual(
      {
        total: rows[0].total,
        replayed: rows[0].replayed,
        skipped: rows[0].skipped,
        done: rows[0].done,
        failed: rows[0].failed,
      },
      { total: 5, replayed: 5, skipped: 0, done: true, failed: false },
    );
    assert.deepEqual(callbackRevisionAudits(bus)[0].detail, {
      hasCallback: false,
      oldHasCallback: true,
    });
    sub.unsubscribe();
  });

  it('unknown, empty, never-replayed and illegal values throw RangeError with the live callback untouched', async () => {
    const events: ReplayProgressEvent[] = [];
    const cb = (e: ReplayProgressEvent) => void events.push({ ...e });
    const { bus, sub, received } = busWithReplay(5, { onReplayProgress: cb });
    await flush();
    assert.equal(received.length, 5);
    events.length = 0;
    const plain = bus.subscribe('plain', () => {});
    const bad = (value: unknown) =>
      assert.throws(() => bus.setReplayProgressCallback(sub.id, value as never), RangeError);
    assert.throws(() => bus.setReplayProgressCallback('', cb), RangeError);
    assert.throws(() => bus.setReplayProgressCallback('no-such-sub', cb), RangeError);
    // Never ran a progress-tracked replay: no resume cursor at all.
    assert.throws(() => bus.setReplayProgressCallback(plain.id, cb), RangeError);
    bad('not-a-function');
    bad(42);
    bad(undefined);
    bad({});
    bad([]);
    assert.equal(callbackRevisionAudits(bus).length, 0, 'failed revisions leave no audit');

    // The live callback is untouched: the next seek replay still
    // fires it.
    bus.seek(sub.id, 'earliest');
    await flush();
    assert.ok(events.length > 0, 'the live callback still governs');
    assert.equal(events.filter((e) => e.done).length, 1);
    assert.equal(callbackRevisionAudits(bus).length, 0);
    sub.unsubscribe();
    plain.unsubscribe();
  });

  it('a subscription that replayed without an observer can install one', async () => {
    const bus = new EventBus({ durableLogDir: freshDir() });
    for (let i = 0; i < 5; i += 1) bus.publish('t', `m${i}`);
    const received: BusMessage[] = [];
    // No onReplayProgress at subscribe time: the pass still tracked,
    // so the row exists and the gate is open.
    const sub = bus.subscribe('t', (msg) => void received.push(msg), { resumeFromSeq: 0 });
    await flush();
    assert.equal(bus.getStats().replayProgress.length, 1);
    assert.equal(bus.getStats().replayProgress[0].done, true);

    const events: ReplayProgressEvent[] = [];
    bus.setReplayProgressCallback(sub.id, (e) => void events.push({ ...e }));
    bus.seek(sub.id, 'earliest');
    await flush();
    const done = events.filter((e) => e.done);
    assert.equal(done.length, 1);
    assert.equal(done[0].replayed, 5);
    assert.equal(done[0].total, 5);
    assert.deepEqual(callbackRevisionAudits(bus)[0].detail, {
      hasCallback: true,
      oldHasCallback: false,
    });
    sub.unsubscribe();
  });

  it('re-affirming the live callback and clearing an unset callback are no-ops with no audit', async () => {
    const events: ReplayProgressEvent[] = [];
    const cb = (e: ReplayProgressEvent) => void events.push({ ...e });
    const bus = new EventBus({ durableLogDir: freshDir() });
    for (let i = 0; i < 5; i += 1) bus.publish('t', `m${i}`);
    const sub = bus.subscribe('t', () => {}, { resumeFromSeq: 0 });
    await flush();
    // Clearing a callback that was never set: no swap, no audit.
    bus.setReplayProgressCallback(sub.id, null);
    assert.equal(callbackRevisionAudits(bus).length, 0);

    // Installing, then re-affirming the same reference: one audit total.
    bus.setReplayProgressCallback(sub.id, cb);
    bus.setReplayProgressCallback(sub.id, cb);
    assert.equal(callbackRevisionAudits(bus).length, 1);
    assert.deepEqual(callbackRevisionAudits(bus)[0].detail, {
      hasCallback: true,
      oldHasCallback: false,
    });

    bus.seek(sub.id, 'earliest');
    await flush();
    assert.ok(events.length > 0, 'the installed callback still governs');
    assert.equal(events.filter((e) => e.done).length, 1);
    sub.unsubscribe();
  });

  it('a throwing callback is swallowed and the replay path is unaffected', async () => {
    const bus = new EventBus({ durableLogDir: freshDir() });
    for (let i = 0; i < 5; i += 1) bus.publish('t', `m${i}`);
    const received: BusMessage[] = [];
    const sub = bus.subscribe('t', (msg) => void received.push(msg), { resumeFromSeq: 0 });
    await flush();
    assert.equal(received.length, 5);

    bus.setReplayProgressCallback(sub.id, () => {
      throw new Error('observer blew up');
    });
    // The seek replay itself must not throw out of the operator call.
    assert.doesNotThrow(() => bus.seek(sub.id, 'earliest'));
    await flush();
    assert.equal(received.length, 10, 'the replay still redelivers');
    const row = bus.getStats().replayProgress[0];
    assert.equal(row.done, true);
    assert.equal(row.failed, false);
    assert.equal(row.replayed, 5);
    sub.unsubscribe();
  });

  it('the audit event records the callback presence exactly', () => {
    const { bus, sub } = busWithReplay(3, {});
    const cb = () => {};
    bus.setReplayProgressCallback(sub.id, cb as never);
    const installed = callbackRevisionAudits(bus);
    assert.equal(installed.length, 1);
    assert.equal(installed[0].action, 'replay_progress_callback_revised');
    assert.equal(installed[0].subId, sub.id);
    assert.equal(installed[0].pattern, 't');
    assert.deepEqual(installed[0].detail, { hasCallback: true, oldHasCallback: false });

    bus.setReplayProgressCallback(sub.id, null);
    const cleared = callbackRevisionAudits(bus);
    assert.equal(cleared.length, 2);
    assert.deepEqual(cleared[1].detail, { hasCallback: false, oldHasCallback: true });
    sub.unsubscribe();
  });

  it('swapping mid-pass does not disturb the pass in flight', async () => {
    const clock = controllableClock();
    const bus = new EventBus({ durableLogDir: freshDir(), now: clock.now });
    for (let i = 0; i < 5; i += 1) bus.publish('t', i);
    const received: BusMessage[] = [];
    const first: ReplayProgressEvent[] = [];
    const second: ReplayProgressEvent[] = [];
    const sub = bus.subscribe('t', (msg) => void received.push(msg), {
      resumeFromSeq: 0,
      replayThrottle: { messagesPerSec: 10, burst: 2 },
      onReplayProgress: (e) => void first.push({ ...e }),
      progressInterval: 1,
    });
    // First chunk ran synchronously inside subscribe: burst of 2
    // admitted, the pass is paused — not done, not failed.
    assert.equal(bus.getStats().replayProgress[0].replayed, 2);
    assert.equal(bus.getStats().replayProgress[0].done, false);
    assert.equal(bus.getStats().replayThrottled, 1);
    assert.equal(first.length, 2, 'the first chunk fired the old callback twice');

    // Swap while the pass is paused: the in-flight pass keeps the
    // observer it started with, including its completion event.
    bus.setReplayProgressCallback(sub.id, (e) => void second.push({ ...e }));
    clock.nowMs += 200;
    bus.publish('zz', {}); // unmatched topic: only triggers a flush
    await flush();
    assert.deepEqual(
      received.map((m) => m.payload),
      [0, 1, 2, 3],
    );
    assert.equal(second.length, 0, 'the new callback never fires mid-pass');
    clock.nowMs += 100;
    bus.publish('zz', {});
    await flush();
    assert.deepEqual(
      received.map((m) => m.payload),
      [0, 1, 2, 3, 4],
    );
    const done = bus.getStats().replayProgress[0];
    assert.equal(done.done, true);
    assert.equal(done.replayed, 5);
    assert.equal(first.filter((e) => e.done).length, 1, 'the old callback got the completion');
    assert.equal(second.length, 0);

    // The next pass — a seek replay — goes to the new callback. The
    // seek replay is throttled like the first pass (same burst of 2),
    // so resume it the same way before asserting completion.
    bus.seek(sub.id, 'earliest');
    clock.nowMs += 200;
    bus.publish('zz', {});
    await flush();
    clock.nowMs += 100;
    bus.publish('zz', {});
    await flush();
    assert.ok(second.length > 0, 'the new callback governs the next pass');
    assert.equal(second.filter((e) => e.done).length, 1);
    assert.equal(first.filter((e) => e.done).length, 1, 'the old callback never fires again');
    sub.unsubscribe();
    bus.destroy?.();
  });

  it('a group member seek likewise goes to the new callback', async () => {
    const bus = new EventBus({ durableLogDir: freshDir() });
    for (let i = 0; i < 5; i += 1) bus.publish('t', `m${i}`);
    const first: ReplayProgressEvent[] = [];
    const second: ReplayProgressEvent[] = [];
    const sub = bus.subscribeToGroup('g', 't', () => {}, {
      resumeFromSeq: 0,
      progressInterval: 1,
      onReplayProgress: (e) => void first.push({ ...e }),
    });
    await flush();
    assert.equal(first.filter((e) => e.done).length, 1);
    first.length = 0;

    bus.setReplayProgressCallback(sub.id, (e) => void second.push({ ...e }));
    bus.seekGroupOffsets('g', 't', { t: 'earliest' });
    await flush();
    assert.equal(first.length, 0, 'the old callback never fires again');
    const done = second.filter((e) => e.done);
    assert.equal(done.length, 1);
    assert.equal(done[0].subscriberId, sub.id);
    assert.equal(done[0].pattern, 't');
    assert.equal(done[0].total, 5);
    assert.equal(done[0].replayed, 5);
    assert.equal(done[0].done, true);
    assert.equal(done[0].failed, false);
    assert.deepEqual(callbackRevisionAudits(bus)[0].detail, {
      hasCallback: true,
      oldHasCallback: true,
    });
    sub.unsubscribe();
  });

  it('a closed subscription throws RangeError', async () => {
    const { bus, sub } = busWithReplay(3, {});
    await flush();
    sub.unsubscribe();
    assert.throws(
      () => bus.setReplayProgressCallback(sub.id, () => {}),
      RangeError,
    );
    assert.equal(callbackRevisionAudits(bus).length, 0);
  });
});
