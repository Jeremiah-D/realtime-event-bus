import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type BusMessage } from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-resumetime-'));

/** Independent reimplementation of the bus's key->partition mapping. */
function keyPartition(key: string, n: number): number {
  const digest = createHash('sha256').update(`k\0${key}`, 'utf8').digest();
  return Number(digest.readBigUInt64BE(0) % BigInt(n));
}

describe('DurableTopicLog.readSinceTime (EB-42)', () => {
  it('returns only records strictly after the timestamp, in file order', () => {
    const log = DurableTopicLog.open({ dir: freshDir() });
    log.append({ seq: 1, topic: 'a', at: 1000, payload: 'one' });
    log.append({ seq: 2, topic: 'a', at: 2000, payload: 'two' });
    log.append({ seq: 3, topic: 'a', at: 2000, payload: 'two-b' });
    log.append({ seq: 4, topic: 'a', at: 3000, payload: 'three' });
    assert.deepEqual(
      log.readSinceTime('a', 2000).map((r) => r.payload),
      ['three'],
    );
    assert.deepEqual(
      log.readSinceTime('a', 0).map((r) => r.payload),
      ['one', 'two', 'two-b', 'three'],
    );
    assert.deepEqual(log.readSinceTime('a', 3000), []);
    assert.deepEqual(log.readSinceTime('unknown', 0), []);
  });

  it('with keyed compaction, only the latest record per key replays by time', () => {
    const log = DurableTopicLog.open({ dir: freshDir(), keyCompaction: true });
    log.append({ seq: 1, topic: 'a', at: 1000, payload: 'v1', key: 'k', keySeq: 1 });
    log.append({ seq: 2, topic: 'a', at: 2000, payload: 'v2', key: 'k', keySeq: 2 });
    log.append({ seq: 3, topic: 'a', at: 2500, payload: 'plain' });
    // v1 is superseded even though it is older than the window start.
    assert.deepEqual(
      log.readSinceTime('a', 0).map((r) => r.payload),
      ['v2', 'plain'],
    );
    assert.deepEqual(
      log.readSinceTime('a', 1500).map((r) => r.payload),
      ['v2', 'plain'],
    );
  });
});

describe('subscribe with resumeFromTime (EB-42)', () => {
  it('replays messages published strictly after the timestamp, in publish-time order', async () => {
    const clock = controllableClock(1_000);
    const bus = new EventBus({ durableLogDir: freshDir(), now: clock.now });
    bus.publish('a', 'old'); // at=1000
    clock.nowMs = 2000;
    bus.publish('b', 'mid'); // at=2000 — exactly at the boundary: excluded
    clock.nowMs = 3000;
    bus.publish('a', 'new'); // at=3000
    clock.nowMs = 2500;
    bus.publish('b', 'between'); // at=2500
    const got: BusMessage[] = [];
    bus.subscribe('**', (m) => got.push(m), { resumeFromTime: 2000 });
    await flush();
    // Cross-topic order follows publish time (at), then seq.
    assert.deepEqual(
      got.map((m) => m.payload),
      ['between', 'new'],
    );
    assert.deepEqual(
      got.map((m) => m.topic),
      ['b', 'a'],
    );
  });

  it('resumeFromTime: 0 replays everything logged', async () => {
    const clock = controllableClock(1_000);
    const bus = new EventBus({ durableLogDir: freshDir(), now: clock.now });
    bus.publish('a', 1);
    bus.publish('a', 2);
    const got: BusMessage[] = [];
    bus.subscribe('a', (m) => got.push(m), { resumeFromTime: 0 });
    await flush();
    assert.deepEqual(got.map((m) => m.payload), [1, 2]);
  });

  it('only replays topics matching the pattern', async () => {
    const clock = controllableClock(1_000);
    const bus = new EventBus({ durableLogDir: freshDir(), now: clock.now });
    clock.nowMs = 2000;
    bus.publish('a', 'hit');
    bus.publish('b', 'miss');
    const got: BusMessage[] = [];
    bus.subscribe('a', (m) => got.push(m), { resumeFromTime: 1000 });
    await flush();
    assert.deepEqual(got.map((m) => m.payload), ['hit']);
  });

  it('replayed messages keep their original seq and TTL deadline; expired ones drop, not resurrect', async () => {
    const clock = controllableClock(1_000);
    const bus = new EventBus({ durableLogDir: freshDir(), now: clock.now });
    bus.setTopicTtl('a', 100); // expiresAt = at + 100
    bus.publish('a', 'doomed'); // at=1000, expires 1100
    clock.nowMs = 2000; // past the deadline
    const got: BusMessage[] = [];
    bus.subscribe('a', (m) => got.push(m), { resumeFromTime: 0 });
    await flush();
    assert.equal(got.length, 0);
    const stats = bus.getStats();
    assert.ok(stats.expiredMessages >= 1);
  });

  it('the content filter applies to time-based replay', async () => {
    const clock = controllableClock(1_000);
    const bus = new EventBus({ durableLogDir: freshDir(), now: clock.now });
    clock.nowMs = 2000;
    bus.publish('a', { sym: 'BTC' });
    bus.publish('a', { sym: 'ETH' });
    const got: BusMessage[] = [];
    bus.subscribe(
      'a',
      (m) => got.push(m),
      { resumeFromTime: 1000, filter: (p) => (p as { sym: string }).sym === 'BTC' },
    );
    await flush();
    assert.deepEqual(got.map((m) => m.payload), [{ sym: 'BTC' }]);
  });

  it('works through subscribeReliable', async () => {
    const clock = controllableClock(1_000);
    const bus = new EventBus({ durableLogDir: freshDir(), now: clock.now });
    bus.publish('a', 'before'); // at=1000
    clock.nowMs = 2000;
    bus.publish('a', 'after');
    const got: string[] = [];
    bus.subscribeReliable(
      'a',
      (d) => {
        got.push(d.msg.payload as string);
        d.ack();
      },
      { resumeFromTime: 1500 },
    );
    await flush();
    assert.deepEqual(got, ['after']);
  });

  it('requires durableLogDir', () => {
    const bus = new EventBus();
    assert.throws(
      () => bus.subscribe('a', () => {}, { resumeFromTime: 0 }),
      RangeError,
    );
  });

  it('is mutually exclusive with resumeFromSeq', () => {
    const bus = new EventBus({ durableLogDir: freshDir() });
    assert.throws(
      () => bus.subscribe('a', () => {}, { resumeFromSeq: 0, resumeFromTime: 0 }),
      /mutually exclusive/,
    );
    assert.throws(
      () => bus.subscribeToGroup('g', 'a', () => {}, { resumeFromSeq: 0, resumeFromTime: 0 }),
      /mutually exclusive/,
    );
  });

  it('rejects non-finite or negative timestamps', () => {
    const bus = new EventBus({ durableLogDir: freshDir() });
    for (const bad of [NaN, -1, -100, Infinity, -Infinity]) {
      assert.throws(
        () => bus.subscribe('a', () => {}, { resumeFromTime: bad }),
        RangeError,
        `expected RangeError for ${String(bad)}`,
      );
    }
  });
});

describe('subscribeToGroup with resumeFromTime (EB-42)', () => {
  it('replays into the member queue like resumeFromSeq', async () => {
    const clock = controllableClock(1_000);
    const bus = new EventBus({ durableLogDir: freshDir(), now: clock.now });
    bus.publish('t', 'old'); // at=1000
    clock.nowMs = 2000;
    bus.publish('t', 'new');
    const got: BusMessage[] = [];
    bus.subscribeToGroup('g', 't', (m) => got.push(m), { resumeFromTime: 1500 });
    await flush();
    assert.deepEqual(got.map((m) => m.payload), ['new']);
  });

  it('partitioned member replays only its assigned partitions', async () => {
    const clock = controllableClock(1_000);
    const bus = new EventBus({ durableLogDir: freshDir(), now: clock.now });
    const keys = ['k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7', 'k8'];
    for (const k of keys) bus.publish('t', k, { key: k });
    // A single member owns every partition on join, so it replays all keys.
    const got: BusMessage[] = [];
    const m = bus.subscribeToGroup('g', 't', (msg) => got.push(msg), {
      partitions: 4,
      resumeFromTime: 0,
    });
    await flush();
    assert.deepEqual(
      got.map((x) => x.payload).sort(),
      keys.slice().sort(),
    );
    m.unsubscribe();
  });

  it('second member joins with resumeFromTime and gets only its own partitions', async () => {
    const bus = new EventBus({ durableLogDir: freshDir() });
    const keys = ['k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7', 'k8'];
    for (const k of keys) bus.publish('t', k, { key: k });
    const gotA: BusMessage[] = [];
    bus.subscribeToGroup('g', 't', (m) => gotA.push(m), { partitions: 4 });
    const gotB: BusMessage[] = [];
    const mB = bus.subscribeToGroup('g', 't', (m) => gotB.push(m), {
      partitions: 4,
      resumeFromTime: 0,
    });
    await flush();
    // mB's replay is filtered to the partitions the rendezvous assignment
    // gave it; every replayed key maps back to a partition mB owns.
    assert.ok(gotB.length > 0, 'expected the joiner to replay its partitions');
    const assignment = bus.getPartitionAssignment('g', 't');
    const owned = new Set<number>();
    for (const [p, owner] of Object.entries(assignment)) {
      if (owner === mB.id) owned.add(Number(p));
    }
    for (const msg of gotB) {
      const p = keyPartition(msg.payload as string, 4);
      assert.ok(owned.has(p), `replayed key ${msg.payload} maps to unowned partition ${p}`);
    }
    mB.unsubscribe();
  });
});
