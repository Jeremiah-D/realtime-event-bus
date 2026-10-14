import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { diagnoseBusHealth, type DoctorInput } from '../src/doctor.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

function check(report: { checks: Array<{ name: string }> }, name: string) {
  const found = report.checks.find((c) => c.name === name);
  assert.ok(found, `expected a "${name}" check`);
  return found as { name: string; status: string; detail: string; suggestion: string };
}

/** Minimal valid DoctorInput for direct fault injection into diagnoseBusHealth. */
function baseInput(overrides: Partial<DoctorInput> = {}): DoctorInput {
  return {
    at: 1_000,
    aliases: [],
    slowestSubscribers: [],
    lagWatermarks: [],
    breachingSubscribers: [],
    lagThresholdMs: new Map(),
    dlqBacklog: [],
    groupLag: [],
    groupLagThresholdFor: () => 100,
    consumerGroups: [],
    namespaces: undefined,
    durableLog: undefined,
    ...overrides,
  };
}

test('healthy bus: all checks pass, status healthy', () => {
  const bus = new EventBus();
  const report = bus.exportBusHealth();
  assert.equal(report.status, 'healthy');
  assert.equal(report.checks.length, 8);
  const names = report.checks.map((c) => c.name);
  assert.deepEqual(names, [
    'slow-subscribers',
    'subscriber-lag',
    'dead-letter-backlog',
    'consumer-group-lag',
    'topic-aliases',
    'namespaces',
    'durable-log',
    'partition-coverage',
  ]);
  for (const c of report.checks) {
    assert.equal(c.status, 'pass');
    assert.ok(c.detail.length > 0);
    assert.equal(c.suggestion, 'none');
  }
});

test('slow subscriber by p99 queue dwell warns (EB-31)', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {}, { deliveryLatency: true });
  bus.publish('t', 'hello'); // enqueued at t=1000
  clock.nowMs += 1500; // delivered at t=2500: 1500ms queue dwell
  await flush();
  const report = bus.exportBusHealth();
  assert.equal(report.status, 'degraded');
  const c = check(report, 'slow-subscribers');
  assert.equal(c.status, 'warn');
  assert.match(c.detail, /p99 queue dwell 1500ms/);
  assert.match(c.suggestion, /handler throughput/);
});

test('latency-SLO breach warns using the subscriber-configured threshold (EB-53)', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const duration = 25;
  bus.subscribe('t', () => { clock.nowMs += duration; }, {
    latencySlo: { windowSize: 10, p99ThresholdMs: 15 },
  });
  for (let i = 0; i < 10; i++) bus.publish('t', i);
  await flush();
  assert.equal(bus.getStats().subscriberLatencyP99[0].breaching, true);
  const c = check(bus.exportBusHealth(), 'slow-subscribers');
  assert.equal(c.status, 'warn');
  assert.match(c.detail, /breaching its latency-SLO threshold 15ms/);
});

test('lag over the configured threshold warns (EB-35)', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {}, { lagMonitor: { thresholdMs: 100 } });
  bus.publish('t', 'a'); // enqueued at t=1000, never flushed
  clock.nowMs += 500; // watermark 500ms >= threshold 100ms
  const report = bus.exportBusHealth();
  assert.equal(report.status, 'degraded');
  const c = check(report, 'subscriber-lag');
  assert.equal(c.status, 'warn');
  assert.match(c.detail, /watermark 500ms >= threshold 100ms/);
});

test('lag without a configured threshold falls back to the doctor default', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {}, { lagMonitor: true });
  bus.publish('t', 'a');
  clock.nowMs += 1000;
  // Default fallback is 30000ms: 1000ms is fine.
  assert.equal(check(bus.exportBusHealth(), 'subscriber-lag').status, 'pass');
  // Explicit doctor option tightens it.
  assert.equal(check(bus.exportBusHealth({ lagWatermarkDefaultMs: 500 }), 'subscriber-lag').status, 'warn');
});

test('DLQ backlog warns; a full DLQ fails (EB-28)', async () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable('t', (d) => { d.nack(); }, {
    ackTimeoutMs: 60_000,
    deadLetter: { maxRedeliveries: 0, maxEntries: 2 },
  });
  bus.publish('t', 'poison-1');
  await flush();
  await flush();
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  let report = bus.exportBusHealth();
  assert.equal(report.status, 'degraded');
  let c = check(report, 'dead-letter-backlog');
  assert.equal(c.status, 'warn');
  assert.match(c.detail, /1\/2 entries/);
  bus.publish('t', 'poison-2');
  await flush();
  await flush();
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 2);
  report = bus.exportBusHealth();
  assert.equal(report.status, 'critical');
  c = check(report, 'dead-letter-backlog');
  assert.equal(c.status, 'fail');
  assert.match(c.detail, /DLQ FULL/);
  assert.match(c.suggestion, /replayDeadLetter/);
});

test('consumer-group lag over threshold warns (EB-48)', async () => {
  const bus = new EventBus({ groupLag: { thresholdMessages: 2 } });
  bus.subscribeToGroup('g', 't', () => {});
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  const report = bus.exportBusHealth();
  assert.equal(report.status, 'degraded');
  const c = check(report, 'consumer-group-lag');
  assert.equal(c.status, 'warn');
  assert.match(c.detail, /group "g".*lag 5 > threshold 2/);
  bus.commitOffset('g', 't', 5);
  assert.equal(check(bus.exportBusHealth(), 'consumer-group-lag').status, 'pass');
});

test('alias chains re-verify clean (EB-44 prevents cycles at registration)', () => {
  const bus = new EventBus();
  bus.setTopicAlias('orders.v1', 'orders.v2');
  bus.setTopicAlias('orders.v2', 'orders.v3');
  const c = check(bus.exportBusHealth(), 'topic-aliases');
  assert.equal(c.status, 'pass');
  assert.match(c.detail, /2 alias\(es\) verified, longest chain 2, no cycles/);
});

test('alias cycle fails via direct diagnosis (unreachable through the public API)', () => {
  const report = diagnoseBusHealth(baseInput({
    aliases: [
      { oldTopic: 'a', newTopic: 'b', expired: false },
      { oldTopic: 'b', newTopic: 'a', expired: false },
    ],
  }));
  assert.equal(report.status, 'critical');
  const c = check(report, 'topic-aliases');
  assert.equal(c.status, 'fail');
  assert.match(c.detail, /alias cycle detected/);
});

test('empty namespace warns (EB-52)', () => {
  const bus = new EventBus();
  bus.createNamespace('t1');
  const report = bus.exportBusHealth();
  assert.equal(report.status, 'degraded');
  const c = check(report, 'namespaces');
  assert.equal(c.status, 'warn');
  assert.match(c.detail, /"t1"/);
});

test('namespace with traffic passes', () => {
  const bus = new EventBus();
  const ns = bus.createNamespace('t1');
  ns.subscribe('x', () => {});
  ns.publish('x', 1);
  assert.equal(check(bus.exportBusHealth(), 'namespaces').status, 'pass');
});

test('partition under-coverage warns with the honest quorum mapping (EB-90)', () => {
  const bus = new EventBus();
  bus.subscribeToGroup('g', 't', () => {}, { partitions: 4 });
  const report = bus.exportBusHealth();
  assert.equal(report.status, 'degraded');
  const c = check(report, 'partition-coverage');
  assert.equal(c.status, 'warn');
  assert.match(c.detail, /4 partitions but only 1 live member/);
  // The backlog's "quorum" item is mapped, not invented: the report says so.
  assert.match(c.detail, /no quorum concept exists on this bus/);
  assert.match(c.detail, /EB-90 partition coverage/);
});

test('fully covered partitioned group passes', () => {
  const bus = new EventBus();
  bus.subscribeToGroup('g', 't', () => {}, { partitions: 1 });
  assert.equal(check(bus.exportBusHealth(), 'partition-coverage').status, 'pass');
});

test('durable log: disabled passes, corrupt segments fail, corrupt lines warn (EB-94)', () => {
  const bus = new EventBus();
  assert.equal(check(bus.exportBusHealth(), 'durable-log').status, 'pass');

  const corrupt = diagnoseBusHealth(baseInput({
    durableLog: {
      dir: '/tmp/dl', topics: 2, entries: 100, corruptLines: 0,
      keyCompaction: false, encrypted: false, retentionEvicted: 0,
      corruptArchives: 0, corruptSegments: 1, archived: 50,
      segments: {},
    },
  }));
  assert.equal(corrupt.status, 'critical');
  assert.equal(check(corrupt, 'durable-log').status, 'fail');
  assert.match(check(corrupt, 'durable-log').detail, /1 corrupt segment\(s\) isolated by checksum/);

  const lines = diagnoseBusHealth(baseInput({
    durableLog: {
      dir: '/tmp/dl', topics: 2, entries: 100, corruptLines: 3,
      keyCompaction: false, encrypted: false, retentionEvicted: 0,
      corruptArchives: 0, corruptSegments: 0, archived: 0,
      segments: {},
    },
  }));
  assert.equal(lines.status, 'degraded');
  assert.equal(check(lines, 'durable-log').status, 'warn');
});

test('exportBusHealth is read-only: stats are unchanged by the call', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {}, { deliveryLatency: true, lagMonitor: true });
  bus.subscribeReliable('u', (d) => { d.ack(); }, { deadLetter: true });
  bus.subscribeToGroup('g', 'v', () => {});
  bus.setTopicAlias('old.t', 'new.t');
  for (let i = 0; i < 3; i++) bus.publish('t', i);
  bus.publish('u', 'x');
  await flush();
  const before = JSON.stringify(bus.getStats());
  const report = bus.exportBusHealth();
  const after = JSON.stringify(bus.getStats());
  assert.equal(after, before);
  // And again: the report itself is a fresh object each time.
  const report2 = bus.exportBusHealth();
  assert.deepEqual(report2, report);
  assert.notEqual(report2, report);
  assert.equal(report.at, clock.nowMs);
});

test('report at carries the bus clock', () => {
  const clock = controllableClock(9_000);
  const bus = new EventBus({ now: clock.now });
  assert.equal(bus.exportBusHealth().at, 9000);
});

test('grading: any fail -> critical, any warn -> degraded', () => {
  // warn only
  const warnOnly = diagnoseBusHealth(baseInput({
    namespaces: [{ namespace: 't1', topics: 0, subscribers: 0, publishedMessages: 0 }],
  }));
  assert.equal(warnOnly.status, 'degraded');
  // fail dominates warn
  const both = diagnoseBusHealth(baseInput({
    namespaces: [{ namespace: 't1', topics: 0, subscribers: 0, publishedMessages: 0 }],
    dlqBacklog: [{ subscriberId: 'sub-1', pattern: 't', entries: 5, maxEntries: 5 }],
  }));
  assert.equal(both.status, 'critical');
});
