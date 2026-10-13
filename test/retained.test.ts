import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type BusMessage } from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

function tempDir(prefix: string): string {
  return mkdtempSync(join(tmpdir(), prefix));
}

// ---------------------------------------------------------------------------
// rule registration and validation
// ---------------------------------------------------------------------------

test('setTopicRetained rejects empty patterns and non-boolean flags', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setTopicRetained('', true), RangeError);
  assert.throws(() => bus.setTopicRetained('t', 'yes' as unknown as boolean), RangeError);
  assert.throws(() => bus.setTopicRetained('t', 1 as unknown as boolean), RangeError);
  // No partial state: nothing was registered.
  assert.equal(bus.getStats().retainedTopics.length, 0);
});

test('clearTopicRetained reports whether a rule existed', () => {
  const bus = new EventBus();
  assert.equal(bus.clearTopicRetained('nope'), false);
  bus.setTopicRetained('t', true);
  assert.equal(bus.clearTopicRetained('t'), true);
  assert.equal(bus.clearTopicRetained('t'), false);
});

test('an exact-topic rule wins over any pattern', () => {
  const bus = new EventBus();
  bus.setTopicRetained('a.*', true);
  bus.setTopicRetained('a.b', false);
  bus.publish('a.b', { x: 1 }, { retain: true });
  bus.publish('a.c', { x: 2 }, { retain: true });
  assert.deepEqual(
    bus.getStats().retainedTopics.map((r) => r.topic),
    ['a.c'],
  );
});

test('between patterns the earliest-registered rule wins', () => {
  const first = new EventBus();
  first.setTopicRetained('x.*', false);
  first.setTopicRetained('x.**', true);
  first.publish('x.y', { x: 1 }, { retain: true });
  assert.equal(first.getStats().retainedTopics.length, 0);

  const second = new EventBus();
  second.setTopicRetained('x.**', true);
  second.setTopicRetained('x.*', false);
  second.publish('x.y', { x: 1 }, { retain: true });
  assert.deepEqual(
    second.getStats().retainedTopics.map((r) => r.topic),
    ['x.y'],
  );
});

test('without a matching rule the retain flag is ignored (ordinary publish)', () => {
  const bus = new EventBus();
  const seen: BusMessage[] = [];
  bus.subscribe('t', (m) => seen.push(m));
  const accepted = bus.publish('t', { x: 1 }, { retain: true });
  assert.equal(accepted, 1);
  assert.equal(bus.getStats().retainedTopics.length, 0);
});

test('retained rule matchers never inflate patternCacheSize', () => {
  const bus = new EventBus();
  bus.setTopicRetained('a.*', true);
  bus.setTopicRetained('b.**', true);
  bus.publish('a.1', { x: 1 }, { retain: true });
  assert.equal(bus.getStats().patternCacheSize, 0);
});

// ---------------------------------------------------------------------------
// store semantics: latest wins, clear via empty payload
// ---------------------------------------------------------------------------

test('the latest retained publish wins per exact topic', () => {
  const bus = new EventBus();
  bus.setTopicRetained('sensor.*', true);
  bus.publish('sensor.temp', { v: 21 }, { retain: true });
  bus.publish('sensor.temp', { v: 22 }, { retain: true });
  const rows = bus.getStats().retainedTopics;
  assert.equal(rows.length, 1);
  assert.equal(rows[0].topic, 'sensor.temp');
  assert.equal(rows[0].seq, 2);
  assert.ok(rows[0].at > 0);
});

test('an empty payload with retain clears the retained message', () => {
  const bus = new EventBus();
  bus.setTopicRetained('t', true);
  bus.publish('t', { x: 1 }, { retain: true });
  assert.equal(bus.getStats().retainedTopics.length, 1);
  // `undefined` serializes to zero JSON bytes: the MQTT clear convention.
  bus.publish('t', undefined, { retain: true });
  assert.equal(bus.getStats().retainedTopics.length, 0);
  const seen: BusMessage[] = [];
  bus.subscribe('t', (m) => seen.push(m));
  assert.equal(seen.length, 0);
});

test('null is not an empty payload (it serializes to four bytes)', () => {
  const bus = new EventBus();
  bus.setTopicRetained('t', true);
  bus.publish('t', null, { retain: true });
  assert.equal(bus.getStats().retainedTopics.length, 1);
});

test('clearing a topic with no retained message is a silent no-op', () => {
  const bus = new EventBus();
  bus.setTopicRetained('t', true);
  bus.publish('t', undefined, { retain: true });
  assert.equal(bus.getStats().retainedTopics.length, 0);
  assert.deepEqual(
    bus.getSubscriptionAudit().map((e) => e.action),
    ['retained_rule_set'],
  );
});

test('a rejected publish never touches the retained store', () => {
  const bus = new EventBus();
  bus.setTopicRetained('t', true);
  bus.setTopicSchema('t', (p): p is unknown => typeof p === 'object' && p !== null);
  bus.publish('t', 'not-an-object', { retain: true });
  assert.equal(bus.getStats().retainedTopics.length, 0);
});

// ---------------------------------------------------------------------------
// late-joiner snapshot: order, backpressure bypass, flags
// ---------------------------------------------------------------------------

test('a late subscriber receives the retained snapshot before live messages', () => {
  const bus = new EventBus();
  bus.setTopicRetained('s.*', true);
  bus.publish('s.a', 'first', { retain: true });
  bus.publish('s.b', 'plain');
  bus.publish('s.a', 'second', { retain: true });

  const seen: BusMessage[] = [];
  bus.subscribe('s.*', (m) => seen.push(m));
  // Synchronous: the snapshot landed before subscribe returned — no flush ran.
  // 's.b' was never retained: no snapshot entry for it. Only the latest
  // retained value per topic is snapshotted.
  assert.equal(seen.length, 1);
  assert.deepEqual(
    seen.map((m) => ({ topic: m.topic, payload: m.payload, seq: m.seq, retained: m.retained })),
    // seq is per-topic: 'second' is the 2nd publish on s.a.
    [{ topic: 's.a', payload: 'second', seq: 2, retained: true }],
  );
});

test('live messages published after subscribe arrive after the snapshot', async () => {
  const bus = new EventBus();
  bus.setTopicRetained('t', true);
  bus.publish('t', 'retained', { retain: true });
  const seen: Array<{ payload: unknown; retained: boolean | undefined }> = [];
  bus.subscribe('t', (m) => seen.push({ payload: m.payload, retained: m.retained }));
  bus.publish('t', 'live-1');
  bus.publish('t', 'live-2');
  await flush();
  await flush();
  assert.deepEqual(
    seen.map((s) => s.payload),
    ['retained', 'live-1', 'live-2'],
  );
  assert.equal(seen[0].retained, true);
  assert.equal(seen[1].retained, undefined);
  assert.equal(seen[2].retained, undefined);
});

test('the snapshot bypasses the backpressure queue (no budget, no capacity)', () => {
  const bus = new EventBus();
  bus.setTopicRetained('t', true);
  bus.publish('t', 'r1', { retain: true });
  const seen: BusMessage[] = [];
  // A queue of capacity 1 with a byte budget: if the snapshot went
  // through the queue it would occupy the single slot and register
  // buffered bytes.
  bus.subscribe('t', (m) => seen.push(m), { queueSize: 1, queueMaxBytes: 1024 });
  assert.equal(seen.length, 1);
  assert.equal(bus.getStats().queueBytes, 0);
  assert.equal(bus.getStats().droppedMessages, 0);
  // A live message queued afterwards still sees a completely free queue.
  bus.publish('t', 'live');
  assert.equal(bus.getStats().queueBytes > 0, true);
});

test('the snapshot establishes the gap baseline (no phantom gaps)', async () => {
  const bus = new EventBus();
  bus.setTopicRetained('t', true);
  bus.publish('t', 'old-1');
  bus.publish('t', 'old-2', { retain: true });
  const seen: BusMessage[] = [];
  bus.subscribe('t', (m) => seen.push(m));
  bus.publish('t', 'new-1');
  await flush();
  await flush();
  assert.deepEqual(
    seen.map((m) => m.payload),
    ['old-2', 'new-1'],
  );
  assert.equal(bus.getStats().sequenceGaps, 0);
});

test('a wildcard subscription receives one retained message per matching topic', () => {
  const bus = new EventBus();
  bus.setTopicRetained('m.*', true);
  bus.publish('m.a', 'va', { retain: true });
  bus.publish('m.b', 'vb', { retain: true });
  bus.publish('other', 'vo', { retain: true });
  const seen: BusMessage[] = [];
  bus.subscribe('m.*', (m) => seen.push(m));
  assert.deepEqual(
    seen.map((m) => m.topic).sort(),
    ['m.a', 'm.b'],
  );
  assert.ok(seen.every((m) => m.retained === true));
});

test('content filters and composite patterns apply to the snapshot', () => {
  const bus = new EventBus();
  bus.setTopicRetained('f.*', true);
  bus.publish('f.a', { keep: true }, { retain: true });
  bus.publish('f.b', { keep: false }, { retain: true });
  const seen: BusMessage[] = [];
  bus.subscribe('f.*', (m) => seen.push(m), {
    filter: (p) => (p as { keep: boolean }).keep === true,
  });
  assert.deepEqual(
    seen.map((m) => m.topic),
    ['f.a'],
  );
});

test('shadow mirrors never receive the retained snapshot', async () => {
  const bus = new EventBus();
  bus.setTopicRetained('t', true);
  bus.publish('t', 'r', { retain: true });
  const seen: BusMessage[] = [];
  bus.subscribe('t', (m) => seen.push(m), { shadow: true });
  bus.publish('t', 'live');
  await flush();
  await flush();
  // Only the live message mirrors; the snapshot is history, not traffic.
  assert.deepEqual(
    seen.map((m) => m.payload),
    ['live'],
  );
});

test('a consumer-group member receives its own snapshot', () => {
  const bus = new EventBus();
  bus.setTopicRetained('g.*', true);
  bus.publish('g.a', 'v', { retain: true });
  const seen: BusMessage[] = [];
  bus.subscribeToGroup('grp', 'g.*', (m) => seen.push(m));
  assert.equal(seen.length, 1);
  assert.equal(seen[0].retained, true);
});

test('a batch subscriber receives the snapshot as one batch call', () => {
  const bus = new EventBus();
  bus.setTopicRetained('b.*', true);
  bus.publish('b.1', 'x', { retain: true });
  bus.publish('b.2', 'y', { retain: true });
  const batches: BusMessage[][] = [];
  bus.subscribe('b.*', (msgs) => batches.push(msgs), { batch: true });
  assert.equal(batches.length, 1);
  assert.equal(batches[0].length, 2);
  assert.ok(batches[0].every((m) => m.retained === true));
});

test('a reliable subscriber receives the snapshot as tracked deliveries', async () => {
  const bus = new EventBus();
  bus.setTopicRetained('r', true);
  bus.publish('r', 'v1', { retain: true });
  const seen: Array<{ payload: unknown; retained: boolean | undefined }> = [];
  bus.subscribeReliable('r', (d) => {
    seen.push({ payload: d.msg.payload, retained: d.msg.retained });
    d.ack();
  });
  bus.publish('r', 'v2');
  await flush();
  await flush();
  assert.deepEqual(
    seen.map((s) => s.payload),
    ['v1', 'v2'],
  );
  assert.equal(seen[0].retained, true);
  assert.equal(seen[1].retained, undefined);
  assert.equal(bus.getStats().unackedDeliveries, 0);
});

// ---------------------------------------------------------------------------
// replay interplay: no double delivery
// ---------------------------------------------------------------------------

test('resumeFromSeq never redelivers the snapshotted retained message', async () => {
  const bus = new EventBus({ durableLogDir: tempDir('retained-resume-') });
  bus.setTopicRetained('t', true);
  bus.publish('t', 'a');
  bus.publish('t', 'b', { retain: true }); // seq 2, retained
  bus.publish('t', 'c'); // seq 3
  const seen: Array<{ seq: number; retained: boolean | undefined }> = [];
  bus.subscribe('t', (m) => seen.push({ seq: m.seq, retained: m.retained }), {
    resumeFromSeq: 1,
  });
  await flush();
  await flush();
  // seq 2 arrives once — as the retained snapshot — then the replayed seq 3.
  assert.deepEqual(seen, [
    { seq: 2, retained: true },
    { seq: 3, retained: undefined },
  ]);
});

test('resumeFromTime never redelivers the snapshotted retained message', async () => {
  const clock = controllableClock(10_000);
  const bus = new EventBus({ durableLogDir: tempDir('retained-resumetime-'), now: clock.now });
  bus.setTopicRetained('t', true);
  bus.publish('t', 'b', { retain: true }); // at 10_000
  clock.nowMs = 11_000;
  bus.publish('t', 'c'); // at 11_000
  const seen: Array<{ seq: number; retained: boolean | undefined }> = [];
  bus.subscribe('t', (m) => seen.push({ seq: m.seq, retained: m.retained }), {
    resumeFromTime: 9_000,
  });
  await flush();
  await flush();
  assert.deepEqual(seen, [
    { seq: 1, retained: true },
    { seq: 2, retained: undefined },
  ]);
});

// ---------------------------------------------------------------------------
// durable recovery
// ---------------------------------------------------------------------------

test('retained state survives a restart via the durable log', () => {
  const dir = tempDir('retained-recover-');
  {
    const bus = new EventBus({ durableLogDir: dir });
    bus.setTopicRetained('cfg.*', true);
    bus.publish('cfg.app', { theme: 'dark' }, { retain: true });
    bus.publish('cfg.db', { url: 'x' }, { retain: true });
  }
  const bus = new EventBus({ durableLogDir: dir });
  assert.deepEqual(
    bus.getStats().retainedTopics.map((r) => ({ topic: r.topic, seq: r.seq })),
    [
      { topic: 'cfg.app', seq: 1 },
      { topic: 'cfg.db', seq: 1 },
    ],
  );
  assert.equal(bus.getStats().durableLog?.corruptLines ?? 0, 0);
  const seen: BusMessage[] = [];
  bus.subscribe('cfg.*', (m) => seen.push(m));
  assert.deepEqual(
    seen.map((m) => ({ topic: m.topic, retained: m.retained })),
    [
      { topic: 'cfg.app', retained: true },
      { topic: 'cfg.db', retained: true },
    ],
  );
  assert.deepEqual(seen[0].payload, { theme: 'dark' });
});

test('a retained tombstone survives a restart (no resurrection)', () => {
  const dir = tempDir('retained-tombstone-');
  {
    const bus = new EventBus({ durableLogDir: dir });
    bus.setTopicRetained('t', true);
    bus.publish('t', { x: 1 }, { retain: true });
    bus.publish('t', undefined, { retain: true });
  }
  const bus = new EventBus({ durableLogDir: dir });
  assert.equal(bus.getStats().retainedTopics.length, 0);
  const seen: BusMessage[] = [];
  bus.subscribe('t', (m) => seen.push(m));
  assert.equal(seen.length, 0);
});

test('a restarted clear still tombstones the on-disk retained record', () => {
  const dir = tempDir('retained-clearafer-');
  {
    const bus = new EventBus({ durableLogDir: dir });
    bus.setTopicRetained('t', true);
    bus.publish('t', { x: 1 }, { retain: true });
  }
  {
    // New process: the table rebuilds from disk, then the clear closes it.
    const bus = new EventBus({ durableLogDir: dir });
    assert.equal(bus.getStats().retainedTopics.length, 1);
    bus.setTopicRetained('t', true);
    bus.publish('t', undefined, { retain: true });
    assert.equal(bus.getStats().retainedTopics.length, 0);
  }
  const bus = new EventBus({ durableLogDir: dir });
  assert.equal(bus.getStats().retainedTopics.length, 0);
});

// ---------------------------------------------------------------------------
// TTL interplay
// ---------------------------------------------------------------------------

test('a retained message past its TTL deadline is never snapshotted', () => {
  const clock = controllableClock(1_000);
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 100);
  bus.setTopicRetained('t', true);
  bus.publish('t', 'v', { retain: true });
  clock.nowMs = 1_200; // past the deadline
  const seen: BusMessage[] = [];
  bus.subscribe('t', (m) => seen.push(m));
  assert.equal(seen.length, 0);
  assert.equal(bus.getStats().retainedTopics.length, 0);
});

test('a retained message inside its TTL deadline is snapshotted', () => {
  const clock = controllableClock(1_000);
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 10_000);
  bus.setTopicRetained('t', true);
  bus.publish('t', 'v', { retain: true });
  clock.nowMs = 2_000;
  const seen: BusMessage[] = [];
  bus.subscribe('t', (m) => seen.push(m));
  assert.equal(seen.length, 1);
  assert.equal(seen[0].retained, true);
});

// ---------------------------------------------------------------------------
// compression + signing interplay
// ---------------------------------------------------------------------------

test('compressed and signed retained messages verify on the snapshot', async () => {
  const dir = tempDir('retained-sign-');
  const payload = { data: 'x'.repeat(500), n: 42 };
  {
    const bus = new EventBus({ durableLogDir: dir });
    bus.setTopicRetained('big.*', true);
    bus.setTopicCompression('big.*', { thresholdBytes: 10 });
    bus.setTopicSigningKey('big.*', { keys: [{ kid: 'k1', secret: 'snapshot-signing-secret' }] });
    bus.publish('big.topic', payload, { retain: true });
  }
  // Restart: the snapshot must still inflate and verify from disk.
  const bus = new EventBus({ durableLogDir: dir });
  bus.setTopicCompression('big.*', { thresholdBytes: 10 });
  bus.setTopicSigningKey('big.*', { keys: [{ kid: 'k1', secret: 'snapshot-signing-secret' }] });
  const seen: BusMessage[] = [];
  bus.subscribe('big.topic', (m) => seen.push(m), { verifySignature: true });
  await flush();
  assert.equal(seen.length, 1);
  assert.deepEqual(seen[0].payload, payload);
  assert.equal(seen[0].retained, true);
  assert.equal(seen[0].signature?.kid, 'k1');
  // Delivery itself proves verification: a failed check drops the
  // message before the handler. (Per-topic compression counters are
  // rebuilt by recovery; the snapshot re-registered the wire envelope
  // for transparent inflation exactly like replay.)
});

test('a snapshot with a bad signature is dropped like a live message', () => {
  const clock = controllableClock(1_000);
  const bus = new EventBus({ now: clock.now });
  bus.setTopicRetained('s.*', true);
  bus.setTopicSigningKey('s.*', { keys: [{ kid: 'k1', secret: 'good-secret' }] });
  bus.publish('s.a', { x: 1 }, { retain: true });
  clock.nowMs = 2_000;
  // Rotate to an unrelated key: k1 retires as of now with grace 0.
  bus.setTopicSigningKey('s.*', { keys: [{ kid: 'k2', secret: 'other-secret' }] });
  clock.nowMs = 2_001; // past the inclusive grace boundary
  const seen: BusMessage[] = [];
  bus.subscribe('s.a', (m) => seen.push(m), { verifySignature: true });
  assert.equal(seen.length, 0);
  const topicStats = bus.getStats().topics.find((t) => t.topic === 's.a');
  assert.equal(topicStats?.signatureFailures, 1);
});

// ---------------------------------------------------------------------------
// observability: stats, audit, metrics
// ---------------------------------------------------------------------------

test('retained rule changes are audited', () => {
  const bus = new EventBus();
  bus.setTopicRetained('a.*', true);
  bus.setTopicRetained('b', false);
  bus.clearTopicRetained('a.*');
  bus.setTopicRetained('c', true);
  bus.publish('c', { x: 1 }, { retain: true });
  bus.publish('c', undefined, { retain: true });
  const events = bus.getSubscriptionAudit();
  assert.deepEqual(
    events.map((e) => e.action),
    [
      'retained_rule_set',
      'retained_rule_set',
      'retained_rule_cleared',
      'retained_rule_set',
      'retained_cleared',
    ],
  );
  assert.equal(events[0].pattern, 'a.*');
  assert.deepEqual(events[0].detail, { enabled: true });
  assert.equal(events[1].pattern, 'b');
  assert.deepEqual(events[1].detail, { enabled: false });
  assert.equal(events[2].pattern, 'a.*');
  assert.equal(events[4].topic, 'c');
  assert.deepEqual(events[4].detail, { via: 'publish' });
});

test('renderPrometheus exposes the retained-topics gauge', () => {
  const bus = new EventBus();
  bus.setTopicRetained('t', true);
  bus.publish('t', { x: 1 }, { retain: true });
  const text = renderPrometheus(bus.getStats());
  assert.ok(text.includes('eventbus_retained_topics 1'), text);
});

test('deliveredMessages counts snapshot deliveries like replays', () => {
  const bus = new EventBus();
  bus.setTopicRetained('t', true);
  bus.publish('t', 'r', { retain: true });
  const before = bus.getStats().deliveredMessages;
  bus.subscribe('t', () => {});
  assert.equal(bus.getStats().deliveredMessages, before + 1);
});
