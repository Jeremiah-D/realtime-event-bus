/**
 * Bus-level default replay progress interval live revision (EB-522).
 *
 * `setDefaultReplayProgressInterval` decides how many processed replay
 * entries pass between `onReplayProgress` fires for future
 * replay-observed subscribers that omit their own `progressInterval`
 * (see `SubscribeOptions.progressInterval`, EB-71) — `50` to observe
 * every new omit-`progressInterval` replay at fifty-entry granularity
 * fleet-wide with one call, `null` to clear the default back to 100.
 * The priority chain is subscriber-explicit > bus default > 100; an
 * explicit `progressInterval` always wins. The default never enables
 * observation: a subscriber that does not pass `onReplayProgress` is
 * untouched by it. A revision only affects subscribers created after
 * it — already-subscribed subscribers keep the interval they were
 * baked with at subscribe time, and `setReplayProgressInterval`
 * (EB-447) still revises such a subscriber one at a time without
 * touching the bus default, its new interval governing from the next
 * pass. A pass already in flight keeps the interval it started with,
 * and the completion event still fires exactly once regardless of the
 * interval. A revision identical to the current default is a no-op
 * with no audit. Audited as `default_replay_progress_interval_revised`
 * (`{ previous, progressInterval }`).
 */
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

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-default-replay-progress-interval-'));

/** The `default_replay_progress_interval_revised` audit events on the bus. */
function defaultAudits(bus: EventBus) {
  return bus.getSubscriptionAudit({ action: 'default_replay_progress_interval_revised' });
}

/** Publishes `n` messages to 't' on `bus` and subscribes with a resume replay. */
function subscribeWithReplay(
  bus: EventBus,
  n = 250,
  opts: { onReplayProgress?: (e: ReplayProgressEvent) => void; progressInterval?: number } = {},
) {
  for (let i = 0; i < n; i += 1) bus.publish('t', `m${i}`);
  const received: BusMessage[] = [];
  const sub = bus.subscribe('t', (msg) => void received.push(msg), {
    resumeFromSeq: 0,
    // The per-subscriber queue defaults to 100: a 250-entry replay
    // would shed past the cap, so size it past the replay.
    queueSize: n + 50,
    ...opts,
  });
  return { sub, received };
}

/** The `replayed` counts of the non-completion fires, in order. */
const throttledCounts = (events: ReplayProgressEvent[]) =>
  events.filter((e) => !e.done && !e.failed).map((e) => e.replayed);

describe('bus-level default replay progress interval (EB-522)', () => {
  it('factory default is unset — new observed subscribers fire at 100', async () => {
    const bus = new EventBus({ durableLogDir: freshDir() });
    assert.equal(bus.getDefaultReplayProgressInterval(), undefined);
    const events: ReplayProgressEvent[] = [];
    const { sub, received } = subscribeWithReplay(bus, 250, {
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

  it('a bus default of 50 governs new observed subscribers that omit progressInterval', async () => {
    const bus = new EventBus({ durableLogDir: freshDir() });
    bus.setDefaultReplayProgressInterval(50);
    assert.equal(bus.getDefaultReplayProgressInterval(), 50);
    const events: ReplayProgressEvent[] = [];
    const { sub, received } = subscribeWithReplay(bus, 250, {
      onReplayProgress: (e) => void events.push({ ...e }),
    });
    await drainUntil(received, 250);
    // The interval divides the total, so the last throttled fire lands
    // exactly on 250, then the completion event follows.
    assert.deepEqual(throttledCounts(events), [50, 100, 150, 200, 250]);
    const done = events.filter((e) => e.done);
    assert.equal(done.length, 1, 'the completion event always fires exactly once');
    assert.equal(done[0].replayed, 250);
    assert.deepEqual(defaultAudits(bus)[0].detail, { previous: null, progressInterval: 50 });
    sub.unsubscribe();
  });

  it('an explicit progressInterval always beats the bus default', async () => {
    const bus = new EventBus({ durableLogDir: freshDir() });
    bus.setDefaultReplayProgressInterval(50);
    const events: ReplayProgressEvent[] = [];
    const { received } = subscribeWithReplay(bus, 25, {
      progressInterval: 10,
      onReplayProgress: (e) => void events.push({ ...e }),
    });
    await drainUntil(received, 25);
    assert.deepEqual(throttledCounts(events), [10, 20]);
    const done = events.filter((e) => e.done);
    assert.equal(done.length, 1);
    assert.equal(done[0].replayed, 25);
  });

  it('clearing to null restores 100 for new observed subscribers', async () => {
    const bus = new EventBus({ durableLogDir: freshDir() });
    bus.setDefaultReplayProgressInterval(50);
    bus.setDefaultReplayProgressInterval(null);
    assert.equal(bus.getDefaultReplayProgressInterval(), undefined);
    const events: ReplayProgressEvent[] = [];
    const { sub, received } = subscribeWithReplay(bus, 250, {
      onReplayProgress: (e) => void events.push({ ...e }),
    });
    await drainUntil(received, 250);
    assert.deepEqual(throttledCounts(events), [100, 200]);
    const done = events.filter((e) => e.done);
    assert.equal(done.length, 1);
    assert.equal(done[0].replayed, 250);
    sub.unsubscribe();
  });

  it('revising to the current default is a no-op with no audit, including clearing an already-unset default', () => {
    const bus = new EventBus({ durableLogDir: freshDir() });
    bus.setDefaultReplayProgressInterval(null);
    assert.equal(
      defaultAudits(bus).length,
      0,
      'clearing an already-unset default audits nothing',
    );
    bus.setDefaultReplayProgressInterval(50);
    bus.setDefaultReplayProgressInterval(50);
    const audit = defaultAudits(bus);
    assert.equal(audit.length, 1);
    assert.deepEqual(audit[0].detail, { previous: null, progressInterval: 50 });
    bus.setDefaultReplayProgressInterval(null);
    bus.setDefaultReplayProgressInterval(null);
    assert.equal(defaultAudits(bus).length, 2, 're-clearing audits nothing');
    assert.deepEqual(defaultAudits(bus)[1].detail, { previous: 50, progressInterval: null });
    assert.equal(bus.getDefaultReplayProgressInterval(), undefined);
  });

  it('invalid values throw RangeError and leave the live default untouched', () => {
    const bus = new EventBus({ durableLogDir: freshDir() });
    bus.setDefaultReplayProgressInterval(50);
    for (const bad of [0, -1, 1.5, Number.NaN, '50', undefined] as unknown[]) {
      assert.throws(
        () => bus.setDefaultReplayProgressInterval(bad as number),
        RangeError,
        `value ${String(bad)} must throw`,
      );
      assert.equal(bus.getDefaultReplayProgressInterval(), 50, 'live default untouched');
    }
    assert.throws(
      () => bus.setDefaultReplayProgressInterval(25, ''),
      RangeError,
      'an illegal actor throws',
    );
    assert.equal(bus.getDefaultReplayProgressInterval(), 50);
    assert.equal(defaultAudits(bus).length, 1, 'failed revisions must not audit');
  });

  it('a revision does not move an already-subscribed subscriber interval', async () => {
    const bus = new EventBus({ durableLogDir: freshDir() });
    bus.setDefaultReplayProgressInterval(50);
    const events: ReplayProgressEvent[] = [];
    const { sub, received } = subscribeWithReplay(bus, 250, {
      onReplayProgress: (e) => void events.push({ ...e }),
    });
    await drainUntil(received, 250);
    assert.deepEqual(throttledCounts(events), [50, 100, 150, 200, 250]);
    events.length = 0;

    // The subscriber keeps the 50 it was baked with, even after the
    // bus default moves to 25.
    bus.setDefaultReplayProgressInterval(25);
    bus.seek(sub.id, 'earliest');
    await drainUntil(received, 500);
    assert.deepEqual(throttledCounts(events), [50, 100, 150, 200, 250]);
    const done = events.filter((e) => e.done);
    assert.equal(done.length, 1);
    assert.equal(done[0].replayed, 250);
    sub.unsubscribe();
  });

  it('the default never enables observation — a subscriber without onReplayProgress stays silent', async () => {
    const bus = new EventBus({ durableLogDir: freshDir() });
    bus.setDefaultReplayProgressInterval(50);
    for (let i = 0; i < 25; i += 1) bus.publish('t', `m${i}`);
    const received: BusMessage[] = [];
    const sub = bus.subscribe('t', (msg) => void received.push(msg), {
      resumeFromSeq: 0,
      queueSize: 75,
    });
    await drainUntil(received, 25);
    // No observer was ever attached, so nothing could fire — but the
    // progress is still visible in getStats().replayProgress, exactly
    // as without a default.
    const rows = bus.getStats().replayProgress;
    assert.equal(rows.length, 1);
    assert.equal(rows[0].done, true);
    assert.equal(rows[0].replayed, 25);
    sub.unsubscribe();
  });

  it('passing progressInterval alone still throws — the subscribe-time gate is unchanged', () => {
    const bus = new EventBus({ durableLogDir: freshDir() });
    bus.setDefaultReplayProgressInterval(50);
    assert.throws(
      () =>
        bus.subscribe('t', () => {}, { resumeFromSeq: 0, progressInterval: 10 }),
      /progressInterval requires onReplayProgress/,
    );
  });

  it('setReplayProgressInterval still retunes a defaulted subscriber from the next pass', async () => {
    const bus = new EventBus({ durableLogDir: freshDir() });
    bus.setDefaultReplayProgressInterval(50);
    const events: ReplayProgressEvent[] = [];
    const { sub, received } = subscribeWithReplay(bus, 250, {
      onReplayProgress: (e) => void events.push({ ...e }),
    });
    await drainUntil(received, 250);
    assert.deepEqual(throttledCounts(events), [50, 100, 150, 200, 250]);
    events.length = 0;

    bus.setReplayProgressInterval(sub.id, 25);
    bus.seek(sub.id, 'earliest');
    await drainUntil(received, 500);
    assert.deepEqual(throttledCounts(events), [25, 50, 75, 100, 125, 150, 175, 200, 225, 250]);
    const done = events.filter((e) => e.done);
    assert.equal(done.length, 1);
    assert.equal(done[0].replayed, 250);
    // The per-subscriber revision did not touch the bus default.
    assert.equal(bus.getDefaultReplayProgressInterval(), 50);
    sub.unsubscribe();
  });

  it('an interval larger than the replay still fires the completion event exactly once', async () => {
    const bus = new EventBus({ durableLogDir: freshDir() });
    bus.setDefaultReplayProgressInterval(1000);
    const events: ReplayProgressEvent[] = [];
    const { sub, received } = subscribeWithReplay(bus, 25, {
      onReplayProgress: (e) => void events.push({ ...e }),
    });
    await drainUntil(received, 25);
    assert.deepEqual(throttledCounts(events), []);
    const done = events.filter((e) => e.done);
    assert.equal(done.length, 1, 'the completion event always fires exactly once');
    assert.equal(done[0].replayed, 25);
    assert.equal(done[0].total, 25);
    sub.unsubscribe();
  });

  it('the audit records previous and progressInterval exactly, and the getter follows the live value', () => {
    const bus = new EventBus({ durableLogDir: freshDir() });
    assert.equal(bus.getDefaultReplayProgressInterval(), undefined);
    bus.setDefaultReplayProgressInterval(50);
    assert.equal(bus.getDefaultReplayProgressInterval(), 50);
    bus.setDefaultReplayProgressInterval(25);
    assert.equal(bus.getDefaultReplayProgressInterval(), 25);
    bus.setDefaultReplayProgressInterval(null);
    assert.equal(bus.getDefaultReplayProgressInterval(), undefined);
    assert.deepEqual(
      defaultAudits(bus).map((a) => a.detail),
      [
        { previous: null, progressInterval: 50 },
        { previous: 50, progressInterval: 25 },
        { previous: 25, progressInterval: null },
      ],
    );
  });
});
