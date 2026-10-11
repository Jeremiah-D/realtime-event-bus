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

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-replayprogress-'));

describe('durable replay progress (EB-71)', () => {
  it('exposes per-subscriber progress in getStats().replayProgress for resumeFromSeq', async () => {
    const bus = new EventBus({ durableLogDir: freshDir() });
    for (let i = 0; i < 5; i += 1) bus.publish('t', `m${i}`);
    const received: BusMessage[] = [];
    const sub = bus.subscribe('t', (msg) => void received.push(msg), { resumeFromSeq: 0 });
    await flush();

    const rows = bus.getStats().replayProgress;
    assert.equal(rows.length, 1);
    assert.deepEqual(rows[0], {
      subscriberId: sub.id,
      pattern: 't',
      total: 5,
      replayed: 5,
      skipped: 0,
      done: true,
      failed: false,
    });
    assert.equal(received.length, 5);
    sub.unsubscribe();
    assert.deepEqual(bus.getStats().replayProgress, []);
  });

  it('exposes progress for resumeFromTime replays', async () => {
    const clock = controllableClock(1_000);
    const bus = new EventBus({ durableLogDir: freshDir(), now: clock.now });
    bus.publish('t', 'old-1');
    bus.publish('t', 'old-2');
    clock.nowMs = 2_000;
    bus.publish('t', 'new-1');
    bus.publish('t', 'new-2');
    const received: BusMessage[] = [];
    bus.subscribe('t', (msg) => void received.push(msg), { resumeFromTime: 1_500 });
    await flush();

    const rows = bus.getStats().replayProgress;
    assert.equal(rows.length, 1);
    assert.equal(rows[0].total, 2);
    assert.equal(rows[0].replayed, 2);
    assert.equal(rows[0].skipped, 0);
    assert.equal(rows[0].done, true);
    assert.equal(rows[0].failed, false);
    assert.deepEqual(
      received.map((m) => m.payload),
      ['new-1', 'new-2'],
    );
  });

  it('leaves no row for subscriptions that never replayed', () => {
    const bus = new EventBus({ durableLogDir: freshDir() });
    bus.publish('t', 'x');
    bus.subscribe('t', () => {});
    assert.deepEqual(bus.getStats().replayProgress, []);
  });

  it('fires onReplayProgress throttled to every 100 entries by default, plus once on completion', () => {
    const bus = new EventBus({ durableLogDir: freshDir() });
    for (let i = 0; i < 250; i += 1) bus.publish('t', `m${i}`);
    const events: ReplayProgressEvent[] = [];
    const sub = bus.subscribe('t', () => {}, {
      resumeFromSeq: 0,
      onReplayProgress: (e) => void events.push({ ...e }),
    });

    assert.equal(events.length, 3);
    assert.equal(events[0].replayed, 100);
    assert.equal(events[0].done, false);
    assert.equal(events[0].failed, false);
    assert.equal(events[1].replayed, 200);
    assert.equal(events[1].done, false);
    const last = events[2];
    assert.equal(last.replayed, 250);
    assert.equal(last.total, 250);
    assert.equal(last.done, true);
    assert.equal(last.failed, false);
    assert.equal(last.subscriberId, sub.id);
    assert.equal(last.pattern, 't');
    // Progress counters are monotonic across the pass.
    for (let i = 1; i < events.length; i += 1) {
      assert.ok(events[i].replayed >= events[i - 1].replayed);
    }
  });

  it('honors a custom progressInterval', () => {
    const bus = new EventBus({ durableLogDir: freshDir() });
    for (let i = 0; i < 25; i += 1) bus.publish('t', `m${i}`);
    const events: ReplayProgressEvent[] = [];
    bus.subscribe('t', () => {}, {
      resumeFromSeq: 0,
      progressInterval: 10,
      onReplayProgress: (e) => void events.push({ ...e }),
    });

    assert.equal(events.length, 3);
    assert.equal(events[0].replayed, 10);
    assert.equal(events[0].done, false);
    assert.equal(events[1].replayed, 20);
    assert.equal(events[2].replayed, 25);
    assert.equal(events[2].done, true);
  });

  it('counts TTL-expired entries as skipped, not failed', async () => {
    const clock = controllableClock(1_000);
    const bus = new EventBus({ durableLogDir: freshDir(), now: clock.now });
    bus.setTopicTtl('t', 100);
    for (let i = 0; i < 3; i += 1) bus.publish('t', `stale-${i}`);
    bus.clearTopicTtl('t');
    bus.publish('t', 'fresh-1');
    bus.publish('t', 'fresh-2');
    clock.nowMs = 2_000;
    const received: BusMessage[] = [];
    bus.subscribe('t', (msg) => void received.push(msg), { resumeFromSeq: 0 });
    await flush();

    const rows = bus.getStats().replayProgress;
    assert.equal(rows.length, 1);
    assert.equal(rows[0].total, 5);
    assert.equal(rows[0].replayed, 2);
    assert.equal(rows[0].skipped, 3);
    assert.equal(rows[0].done, true);
    assert.equal(rows[0].failed, false);
    assert.equal(rows[0].error, undefined);
    // Unchanged delivery behavior: the expired records were still dropped
    // as expired at drain time, never resurrected to the handler.
    assert.deepEqual(
      received.map((m) => m.payload),
      ['fresh-1', 'fresh-2'],
    );
  });

  it('counts filter and composite rejections as skipped', async () => {
    const bus = new EventBus({ durableLogDir: freshDir() });
    bus.publish('t.keep', 'keep-1');
    bus.publish('t.keep', 'keep-2');
    bus.publish('t.skip', 'skip-1');
    bus.publish('t.skip', 'skip-2');
    const received: BusMessage[] = [];
    bus.subscribe('t.**', (msg) => void received.push(msg), {
      resumeFromSeq: 0,
      patterns: { patterns: ['t.**'], exclude: ['t.skip'] },
    });
    await flush();

    const rows = bus.getStats().replayProgress;
    assert.equal(rows.length, 1);
    assert.equal(rows[0].total, 4);
    assert.equal(rows[0].replayed, 2);
    assert.equal(rows[0].skipped, 2);
    assert.equal(rows[0].done, true);
    assert.equal(rows[0].failed, false);
    assert.deepEqual(
      received.map((m) => m.payload),
      ['keep-1', 'keep-2'],
    );
  });

  it('marks the pass failed and surfaces the error when a record aborts replay', () => {
    const bus = new EventBus({ durableLogDir: freshDir() });
    bus.publish('t', 'm1');
    bus.publish('t', 'm2');
    bus.publish('t', 'boom');
    bus.publish('t', 'm4');
    bus.publish('t', 'm5');
    const events: ReplayProgressEvent[] = [];
    assert.throws(
      () =>
        bus.subscribe('t', () => {}, {
          resumeFromSeq: 0,
          onReplayProgress: (e) => void events.push({ ...e }),
          filter: (payload) => {
            if (payload === 'boom') throw new Error('boom-filter');
            return true;
          },
        }),
      /boom-filter/,
    );

    const rows = bus.getStats().replayProgress;
    assert.equal(rows.length, 1);
    assert.equal(rows[0].total, 5);
    assert.equal(rows[0].replayed, 2);
    assert.equal(rows[0].skipped, 0);
    assert.equal(rows[0].done, false);
    assert.equal(rows[0].failed, true);
    assert.equal(rows[0].error, 'boom-filter');
    // The failure surfaced on the observer exactly once.
    assert.equal(events.length, 1);
    assert.equal(events[0].failed, true);
    assert.equal(events[0].done, false);
    assert.equal(events[0].error, 'boom-filter');
    assert.equal(events[0].replayed, 2);
  });

  it('counts consumer-group members independently', async () => {
    const bus = new EventBus({ durableLogDir: freshDir() });
    for (let i = 0; i < 5; i += 1) bus.publish('t', `m${i}`);
    const sub1 = bus.subscribeToGroup('g', 't', () => {}, { resumeFromSeq: 0 });
    const sub2 = bus.subscribeToGroup('g', 't', () => {}, { resumeFromSeq: 0 });
    await flush();

    const rows = bus.getStats().replayProgress;
    assert.equal(rows.length, 2);
    // Group replay is per member from its own offset: each member
    // replays the full window into its own queue.
    for (const row of rows) {
      assert.equal(row.total, 5);
      assert.equal(row.replayed, 5);
      assert.equal(row.skipped, 0);
      assert.equal(row.done, true);
      assert.equal(row.failed, false);
    }
    assert.deepEqual(
      rows.map((r) => r.subscriberId).sort(),
      [sub1.id, sub2.id].sort(),
    );
    sub1.unsubscribe();
    sub2.unsubscribe();
  });

  it('counts partitioned group members independently over disjoint windows', async () => {
    const bus = new EventBus({ durableLogDir: freshDir() });
    for (let i = 0; i < 10; i += 1) bus.publish('t', `m${i}`, { key: `k${i}` });
    // Join both members without resume first so the assignment is final
    // before either replay runs.
    const first = bus.subscribeToGroup('g', 't', () => {}, { partitions: 8 });
    const second = bus.subscribeToGroup('g', 't', () => {}, { partitions: 8 });
    first.unsubscribe();
    const sub1 = bus.subscribeToGroup('g', 't', () => {}, { partitions: 8, resumeFromSeq: 0 });
    second.unsubscribe();
    const sub2 = bus.subscribeToGroup('g', 't', () => {}, { partitions: 8, resumeFromSeq: 0 });
    await flush();

    const rows = bus.getStats().replayProgress;
    assert.equal(rows.length, 2);
    const byId = new Map(rows.map((r) => [r.subscriberId, r]));
    const row1 = byId.get(sub1.id)!;
    const row2 = byId.get(sub2.id)!;
    // Each member replays exactly its assigned partitions: disjoint,
    // jointly covering the whole window.
    assert.equal(row1.total + row2.total, 10);
    for (const row of [row1, row2]) {
      assert.equal(row.replayed + row.skipped, row.total);
      assert.equal(row.done, true);
      assert.equal(row.failed, false);
    }
    sub1.unsubscribe();
    sub2.unsubscribe();
  });

  it('supports onReplayProgress for group members', () => {
    const bus = new EventBus({ durableLogDir: freshDir() });
    for (let i = 0; i < 5; i += 1) bus.publish('t', `m${i}`);
    const events: ReplayProgressEvent[] = [];
    const sub = bus.subscribeToGroup('g', 't', () => {}, {
      resumeFromSeq: 0,
      progressInterval: 2,
      onReplayProgress: (e) => void events.push({ ...e }),
    });
    const last = events[events.length - 1];
    assert.equal(last.done, true);
    assert.equal(last.replayed, 5);
    assert.equal(last.subscriberId, sub.id);
    sub.unsubscribe();
  });

  it('isolates a throwing onReplayProgress observer from the replay', async () => {
    const bus = new EventBus({ durableLogDir: freshDir() });
    for (let i = 0; i < 5; i += 1) bus.publish('t', `m${i}`);
    const received: BusMessage[] = [];
    bus.subscribe('t', (msg) => void received.push(msg), {
      resumeFromSeq: 0,
      onReplayProgress: () => {
        throw new Error('broken observer');
      },
    });
    await flush();

    const rows = bus.getStats().replayProgress;
    assert.equal(rows.length, 1);
    assert.equal(rows[0].done, true);
    assert.equal(rows[0].failed, false);
    assert.equal(rows[0].replayed, 5);
    assert.equal(received.length, 5);
  });

  it('validates the observer options fail-fast without half-registration', () => {
    const bus = new EventBus({ durableLogDir: freshDir() });
    bus.publish('t', 'x');
    const before = bus.getStats().totalSubscribers;

    // Observer without a replay cursor would never fire.
    assert.throws(
      () => bus.subscribe('t', () => {}, { onReplayProgress: () => {} }),
      /onReplayProgress requires resumeFromSeq or resumeFromTime/,
    );
    // Interval without an observer is meaningless.
    assert.throws(
      () => bus.subscribe('t', () => {}, { resumeFromSeq: 0, progressInterval: 10 }),
      /progressInterval requires onReplayProgress/,
    );
    // Types and ranges.
    assert.throws(
      () => bus.subscribe('t', () => {}, { resumeFromSeq: 0, onReplayProgress: 42 as never }),
      /onReplayProgress must be a function/,
    );
    assert.throws(
      () =>
        bus.subscribe('t', () => {}, {
          resumeFromSeq: 0,
          onReplayProgress: () => {},
          progressInterval: 0,
        }),
      /progressInterval must be a positive integer/,
    );
    assert.throws(
      () =>
        bus.subscribe('t', () => {}, {
          resumeFromSeq: 0,
          onReplayProgress: () => {},
          progressInterval: 1.5,
        }),
      /progressInterval must be a positive integer/,
    );
    // Group subscriptions validate the same way.
    assert.throws(
      () => bus.subscribeToGroup('g', 't', () => {}, { onReplayProgress: () => {} }),
      /onReplayProgress requires resumeFromSeq or resumeFromTime/,
    );
    assert.throws(
      () =>
        bus.subscribeToGroup('g', 't', () => {}, {
          resumeFromSeq: 0,
          onReplayProgress: () => {},
          progressInterval: -3,
        }),
      /progressInterval must be a positive integer/,
    );
    assert.equal(bus.getStats().totalSubscribers, before);
    assert.deepEqual(bus.getStats().replayProgress, []);
  });
});
