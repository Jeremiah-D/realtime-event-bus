import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';
import { DEFAULT_GROUP_LAG_THRESHOLD, type GroupLagEvent } from '../src/grouplag.ts';

describe('consumer-group lag rows (getStats().groupLag)', () => {
  test('lag is assigned minus committed, per (group, topic)', () => {
    const bus = new EventBus();
    bus.subscribeToGroup('g', 't', () => {});
    for (let i = 0; i < 5; i++) bus.publish('t', i);
    bus.commitOffset('g', 't', 3);
    assert.deepEqual(bus.getStats().groupLag, [
      {
        groupId: 'g',
        pattern: 't',
        topic: 't',
        assignedSeq: 5,
        committedSeq: 3,
        lingerHeldToSeq: 0,
        lag: 2,
      },
    ]);
  });

  test('uncommitted groups read committedSeq 0 and lag = assigned', () => {
    const bus = new EventBus();
    bus.subscribeToGroup('g', 't', () => {});
    bus.publish('t', 'x');
    bus.publish('t', 'y');
    const [row] = bus.getStats().groupLag;
    assert.equal(row.assignedSeq, 2);
    assert.equal(row.committedSeq, 0);
    assert.equal(row.lag, 2);
  });

  test('commits past assigned clamp lag at 0 (no negative lag)', () => {
    const bus = new EventBus();
    bus.subscribeToGroup('g', 't', () => {});
    bus.publish('t', 'x');
    bus.commitOffset('g', 't', 99);
    assert.equal(bus.getStats().groupLag[0].lag, 0);
  });

  test('rows cover every topic and sort deterministically', () => {
    const bus = new EventBus();
    bus.subscribeToGroup('b', 'z', () => {});
    bus.subscribeToGroup('a', 'a', () => {});
    bus.publish('z', 1);
    bus.publish('a', 1);
    bus.publish('a', 2);
    assert.deepEqual(
      bus.getStats().groupLag.map((r) => [r.groupId, r.topic, r.lag]),
      [
        ['a', 'a', 2],
        ['b', 'z', 1],
      ],
    );
  });

  test('lag survives the last member leaving (watermarks are history)', () => {
    const bus = new EventBus();
    const sub = bus.subscribeToGroup('g', 't', () => {});
    for (let i = 0; i < 4; i++) bus.publish('t', i);
    sub.unsubscribe();
    // Committing for a group with no live members is the
    // restore-before-rejoin case; lag must still move.
    bus.commitOffset('g', 't', 2);
    const [row] = bus.getStats().groupLag;
    assert.equal(row.assignedSeq, 4);
    assert.equal(row.committedSeq, 2);
    assert.equal(row.lag, 2);
  });

  test('pattern is unattributable when one group runs several competing sets', () => {
    const bus = new EventBus();
    bus.subscribeToGroup('g', 't1', () => {});
    bus.subscribeToGroup('g', 't2', () => {});
    bus.publish('t1', 1);
    const [row] = bus.getStats().groupLag;
    assert.equal(row.groupId, 'g');
    assert.equal(row.topic, 't1');
    assert.equal(row.pattern, '');
  });
});

describe('group-lag alerting (onGroupLag)', () => {
  test('fires once per excursion with latch semantics', () => {
    const events: GroupLagEvent[] = [];
    const bus = new EventBus({ groupLag: { thresholdMessages: 2, onGroupLag: (e) => events.push(e) } });
    bus.subscribeToGroup('g', 't', () => {});
    for (let i = 0; i < 3; i++) bus.publish('t', i); // lag 1, 2, 3: fires on the third
    assert.equal(events.length, 1);
    assert.equal(events[0].lag, 3);
    assert.equal(events[0].groupId, 'g');
    assert.equal(events[0].topic, 't');
    for (let i = 0; i < 5; i++) bus.publish('t', i); // stays above: no refire
    assert.equal(events.length, 1);
    bus.commitOffset('g', 't', 8); // lag 0: rearms
    for (let i = 0; i < 3; i++) bus.publish('t', i); // lag 1, 2, 3: fires again
    assert.equal(events.length, 2);
    assert.equal(events[1].lag, 3);
  });

  test('threshold 0 alerts on any positive lag', () => {
    const events: GroupLagEvent[] = [];
    const bus = new EventBus({ groupLag: { thresholdMessages: 0, onGroupLag: (e) => events.push(e) } });
    bus.subscribeToGroup('g', 't', () => {});
    bus.publish('t', 1);
    assert.equal(events.length, 1);
    assert.equal(events[0].lag, 1);
  });

  test('per-group thresholds override the default', () => {
    const events: GroupLagEvent[] = [];
    const bus = new EventBus({
      groupLag: {
        thresholdMessages: 100,
        thresholds: { fast: 1 },
        onGroupLag: (e) => events.push(e),
      },
    });
    bus.subscribeToGroup('fast', 't', () => {});
    bus.subscribeToGroup('slow', 't', () => {});
    bus.publish('t', 1);
    bus.publish('t', 2);
    assert.deepEqual(events.map((e) => e.groupId), ['fast']);
  });

  test('setGroupLagThreshold / clearGroupLagThreshold adjust at runtime and re-evaluate', () => {
    const events: GroupLagEvent[] = [];
    const bus = new EventBus({ groupLag: { thresholdMessages: 100, onGroupLag: (e) => events.push(e) } });
    bus.subscribeToGroup('g', 't', () => {});
    for (let i = 0; i < 10; i++) bus.publish('t', i);
    assert.equal(events.length, 0);
    bus.setGroupLagThreshold('g', 5); // lag 10 already above: alerts now
    assert.equal(events.length, 1);
    bus.clearGroupLagThreshold('g'); // back to 100: rearms silently
    assert.equal(events.length, 1);
    bus.setGroupLagThreshold('g', 5); // lag 10 above again: refires
    assert.equal(events.length, 2);
  });

  test('a throwing onGroupLag is isolated from the publish path', () => {
    const bus = new EventBus({
      groupLag: {
        thresholdMessages: 0,
        onGroupLag: () => {
          throw new Error('alert sink down');
        },
      },
    });
    bus.subscribeToGroup('g', 't', () => {});
    assert.doesNotThrow(() => bus.publish('t', 1));
    assert.equal(bus.getStats().groupLag[0].lag, 1);
  });

  test('joining a member does not refire a latched alarm', () => {
    const events: GroupLagEvent[] = [];
    const bus = new EventBus({ groupLag: { thresholdMessages: 2, onGroupLag: (e) => events.push(e) } });
    bus.subscribeToGroup('g', 't', () => {});
    for (let i = 0; i < 5; i++) bus.publish('t', i);
    assert.equal(events.length, 1);
    bus.subscribeToGroup('g', 't', () => {}); // rebalance re-evaluates: still latched
    assert.equal(events.length, 1);
  });
});

describe('group-lag option validation', () => {
  test('bad constructor options throw RangeError', () => {
    assert.throws(() => new EventBus({ groupLag: { thresholdMessages: -1 } }), RangeError);
    assert.throws(() => new EventBus({ groupLag: { thresholdMessages: Number.NaN } }), RangeError);
    assert.throws(() => new EventBus({ groupLag: { thresholds: { g: -2 } } }), RangeError);
    assert.throws(() => new EventBus({ groupLag: { thresholds: { '': 1 } } }), RangeError);
    assert.throws(() => new EventBus({ groupLag: { onGroupLag: 42 as never } }), RangeError);
    assert.throws(() => new EventBus({ groupLag: 'x' as never }), RangeError);
  });

  test('bad runtime threshold calls throw RangeError', () => {
    const bus = new EventBus();
    bus.subscribeToGroup('g', 't', () => {});
    assert.throws(() => bus.setGroupLagThreshold('', 5), RangeError);
    assert.throws(() => bus.setGroupLagThreshold('g', -1), RangeError);
    assert.throws(() => bus.clearGroupLagThreshold(''), RangeError);
  });

  test('default threshold is documented and sane', () => {
    assert.equal(DEFAULT_GROUP_LAG_THRESHOLD, 100);
  });

  test('runtime thresholds work without a constructor option (monitoring stays off)', () => {
    const bus = new EventBus();
    bus.subscribeToGroup('g', 't', () => {});
    bus.publish('t', 1);
    bus.setGroupLagThreshold('g', 5); // no callback configured: must not throw
    assert.equal(bus.getStats().groupLag[0].lag, 1);
  });
});

describe('group lag with handoff linger (EB-32)', () => {
  test('a live linger window excludes the held backlog from lag (no false alarm)', () => {
    const dir = mkdtempSync(join(tmpdir(), 'grouplag-linger-'));
    let now = 1_000_000;
    const events: GroupLagEvent[] = [];
    const bus = new EventBus({
      durableLogDir: dir,
      now: () => now,
      groupLag: { thresholdMessages: 100, onGroupLag: (e) => events.push(e) },
    });
    const sub = bus.subscribeToGroup('g', 't', () => {}, { handoffLingerMs: 1000 });
    for (let i = 0; i < 10; i++) bus.publish('t', i);
    sub.unsubscribe(); // opens a linger window over (0, 10] until now + 1000
    assert.equal(events.length, 0);
    bus.setGroupLagThreshold('g', 5); // re-evaluates: lingered backlog is not lag
    assert.equal(events.length, 0);
    const [row] = bus.getStats().groupLag;
    assert.equal(row.assignedSeq, 10);
    assert.equal(row.committedSeq, 0);
    assert.equal(row.lingerHeldToSeq, 10);
    assert.equal(row.lag, 0);
    now += 2000; // linger expired: the backlog is at-least-once replayable again
    bus.commitOffset('g', 't', 1); // re-evaluates: linger gone, lag 9 > 5
    assert.equal(events.length, 1);
    assert.equal(events[0].lag, 9);
    assert.equal(bus.getStats().groupLag[0].lingerHeldToSeq, 0);
  });
});

describe('group lag for partitioned groups (EB-38)', () => {
  test('one row per (partition, topic) plus the group-level row; per-partition commits drive lag', () => {
    const events: GroupLagEvent[] = [];
    const bus = new EventBus({ groupLag: { thresholdMessages: 0, onGroupLag: (e) => events.push(e) } });
    bus.subscribeToGroup('g', 't', () => {}, { partitions: 2 });
    for (let i = 0; i < 6; i++) bus.publish('t', i);
    const rows = bus.getStats().groupLag;
    // Group-level row (assignedSeq = highest seq handed to the group) plus
    // one row per partition: the group total and the drill-down.
    assert.equal(rows.length, 3);
    const classic = rows.filter((r) => r.partition === undefined);
    const partitioned = rows.filter((r) => r.partition !== undefined);
    assert.equal(classic.length, 1);
    assert.equal(classic[0].assignedSeq, 6);
    assert.equal(partitioned.length, 2);
    assert.ok(partitioned.every((r) => r.pattern === 't'));
    assert.deepEqual(
      partitioned.map((r) => r.partition).sort(),
      [0, 1],
    );
    const totalAssigned = partitioned.reduce((n, r) => n + r.assignedSeq, 0);
    assert.ok(totalAssigned >= 6, 'every publish lands on exactly one partition');
    // Commit only partition 0 fully: its lag clears, partition 1 keeps lagging.
    const p0 = partitioned.find((r) => r.partition === 0)!;
    bus.commitOffset('g', 't', p0.assignedSeq, { partition: 0 });
    const after = bus.getStats().groupLag;
    assert.equal(after.find((r) => r.partition === 0)!.lag, 0);
    assert.ok(after.find((r) => r.partition === 1)!.lag > 0);
  });

  test('uncommitted partitions fall back to the group-level checkpoint', () => {
    const bus = new EventBus();
    bus.subscribeToGroup('g', 't', () => {}, { partitions: 2 });
    for (let i = 0; i < 4; i++) bus.publish('t', i);
    bus.commitOffset('g', 't', 2); // group-level only
    for (const row of bus.getStats().groupLag.filter((r) => r.partition !== undefined)) {
      assert.equal(row.committedSeq, 2);
      assert.equal(row.lag, Math.max(0, row.assignedSeq - 2));
    }
  });
});

describe('group lag in prometheus exposition', () => {
  test('classic and partition rows render as separate gauge families', () => {
    const bus = new EventBus();
    bus.subscribeToGroup('g', 't', () => {}, { partitions: 2 });
    for (let i = 0; i < 4; i++) bus.publish('t', i);
    bus.commitOffset('g', 't', 1);
    const text = renderPrometheus(bus.getStats());
    assert.ok(text.includes('eventbus_group_lag_messages'), 'missing classic family');
    const partitionLines = text
      .split('\n')
      .filter((l) => l.startsWith('eventbus_group_partition_lag_messages{'));
    assert.equal(partitionLines.length, 2);
    for (const line of partitionLines) {
      assert.match(line, /group="g",topic="t",partition="[01]"/);
    }
  });

  test('a stats object predating groupLag renders without lag series', () => {
    const bus = new EventBus();
    bus.publish('t', 1);
    const { groupLag: _omitted, ...legacyStats } = bus.getStats();
    const text = renderPrometheus(legacyStats as never);
    assert.ok(!text.includes('eventbus_group_lag_messages{'));
    assert.ok(text.includes('eventbus_published_messages_total 1'));
  });
});
