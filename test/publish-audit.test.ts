import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, readdirSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import {
  PublishAuditLog,
  parsePublishAuditLine,
  PUBLISH_AUDIT_LIVE_NAME,
} from '../src/publishaudit.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const tempDir = (prefix: string): string => mkdtempSync(join(tmpdir(), prefix));

const liveLines = (dir: string): string[] =>
  readFileSync(join(dir, PUBLISH_AUDIT_LIVE_NAME), 'utf8')
    .split('\n')
    .filter((l) => l.trim() !== '');

test('publish audit: admitted publishes are journaled with seq and identities, no payload', async () => {
  const dir = tempDir('pubaudit-');
  const clock = controllableClock(5_000);
  const bus = new EventBus({ durableLogDir: dir, publishAudit: true, now: clock.now });
  bus.subscribe('orders.*', () => {});
  const traceId = 'a'.repeat(32);
  bus.publish(
    'orders.created',
    { secret: 'payload-body-must-not-appear' },
    {
      publisherId: 'svc-orders',
      messageId: 'm-1',
      key: 'k-9',
      traceparent: `00-${traceId}-${'b'.repeat(16)}-01`,
    },
  );
  bus.publish('orders.created', { n: 2 });
  bus.publish('orders.paid', { n: 3 });
  await flush();

  const entries = bus.getPublishAudit();
  assert.equal(entries.length, 3);
  assert.deepEqual(entries[0], {
    v: 1,
    outcome: 'admitted',
    topic: 'orders.created',
    seq: 1,
    publisherId: 'svc-orders',
    traceId,
    messageId: 'm-1',
    key: 'k-9',
    admittedAtMs: 5_000,
  });
  assert.equal(entries[1].seq, 2);
  assert.equal(entries[1].publisherId, undefined);
  assert.equal(entries[2].topic, 'orders.paid');
  assert.equal(entries[2].seq, 1); // per-topic sequence space
  // The payload body never lands in the journal.
  const raw = readFileSync(join(dir, PUBLISH_AUDIT_LIVE_NAME), 'utf8');
  assert.ok(!raw.includes('payload-body-must-not-appear'));
  bus.destroy?.();
});

test('publish audit: rejections cite the EB-39 reason', async () => {
  const dir = tempDir('pubaudit-');
  const bus = new EventBus({
    durableLogDir: dir,
    publishAudit: true,
    acl: { rules: [{ pattern: 'admin.**', publish: 'deny' }] },
  });
  bus.subscribe('**', () => {});
  bus.publish('admin.cmd', { x: 1 }, { publisherId: 'intruder' });
  await flush();

  const entries = bus.getPublishAudit();
  assert.equal(entries.length, 1);
  assert.equal(entries[0].outcome, 'rejected');
  assert.equal(entries[0].topic, 'admin.cmd');
  assert.equal(entries[0].reason, 'acl');
  assert.equal(entries[0].seq, undefined);
  assert.equal(entries[0].publisherId, 'intruder');
  bus.destroy?.();
});

test('publish audit: idempotent duplicates journal as rejected/duplicate', () => {
  const dir = tempDir('pubaudit-');
  const bus = new EventBus({ durableLogDir: dir, publishAudit: true });
  bus.subscribe('t', () => {});
  const r1 = bus.publishIdempotent('t', { n: 1 }, { messageId: 'dup-1' });
  const r2 = bus.publishIdempotent('t', { n: 1 }, { messageId: 'dup-1' });
  assert.equal(r1.duplicate, false);
  assert.equal(r2.duplicate, true);

  const entries = bus.getPublishAudit();
  assert.equal(entries.length, 2);
  assert.equal(entries[0].outcome, 'admitted');
  assert.equal(entries[1].outcome, 'rejected');
  assert.equal(entries[1].reason, 'duplicate');
  assert.equal(entries[1].messageId, 'dup-1');
  bus.destroy?.();
});

test('publish audit: opt-in — disabled by default, requires durableLogDir', () => {
  const plain = new EventBus();
  plain.publish('t', {});
  assert.deepEqual(plain.getPublishAudit(), []);
  assert.equal(plain.getStats().publishAuditWriteErrors, 0);
  plain.destroy?.();

  assert.throws(
    () => new EventBus({ publishAudit: true }),
    (err: unknown) => {
      assert.ok(err instanceof RangeError);
      assert.match((err as Error).message, /publishAudit requires.*durableLogDir/);
      return true;
    },
  );
  assert.throws(() => new EventBus({ publishAudit: 42 as unknown as boolean }), RangeError);
});

test('publish audit: publishBatch journals every entry', () => {
  const dir = tempDir('pubaudit-');
  const bus = new EventBus({ durableLogDir: dir, publishAudit: true });
  bus.subscribe('**', () => {});
  bus.publishBatch([
    { topic: 'b.a', payload: { n: 1 }, publisherId: 'batcher' },
    { topic: 'b.b', payload: { n: 2 } },
  ]);
  const entries = bus.getPublishAudit();
  assert.equal(entries.length, 2);
  assert.deepEqual(
    entries.map((e) => [e.topic, e.seq, e.outcome]),
    [
      ['b.a', 1, 'admitted'],
      ['b.b', 1, 'admitted'],
    ],
  );
  assert.equal(entries[0].publisherId, 'batcher');
  bus.destroy?.();
});

test('publish audit: query filters by topic, publisherId, time window and limit', () => {
  const dir = tempDir('pubaudit-');
  const clock = controllableClock(1_000);
  const bus = new EventBus({ durableLogDir: dir, publishAudit: true, now: clock.now });
  bus.subscribe('**', () => {});
  bus.publish('q.a', {}, { publisherId: 'p1' }); // t=1000
  clock.nowMs = 2_000;
  bus.publish('q.b', {}, { publisherId: 'p2' }); // t=2000
  clock.nowMs = 3_000;
  bus.publish('q.a', {}, { publisherId: 'p1' }); // t=3000

  assert.equal(bus.getPublishAudit({ topic: 'q.a' }).length, 2);
  assert.equal(bus.getPublishAudit({ publisherId: 'p2' }).length, 1);
  assert.equal(bus.getPublishAudit({ since: 2_000 }).length, 2);
  assert.equal(bus.getPublishAudit({ until: 3_000 }).length, 2);
  assert.equal(bus.getPublishAudit({ since: 2_000, until: 3_000 }).length, 1);
  // limit keeps the most recent matches, oldest-first.
  const limited = bus.getPublishAudit({ limit: 2 });
  assert.equal(limited.length, 2);
  assert.deepEqual(
    limited.map((e) => e.admittedAtMs),
    [2_000, 3_000],
  );
  // Illegal queries fail fast.
  assert.throws(() => bus.getPublishAudit({ limit: 0 }), RangeError);
  assert.throws(() => bus.getPublishAudit({ since: -1 }), RangeError);
  bus.destroy?.();
});

test('publish audit: UTC-day rollover rotates the live file (archived, never deleted)', () => {
  const dir = tempDir('pubaudit-');
  // 2026-10-16 23:59 UTC, then past midnight.
  const clock = controllableClock(Date.UTC(2026, 9, 16, 23, 59, 0));
  const bus = new EventBus({ durableLogDir: dir, publishAudit: true, now: clock.now });
  bus.subscribe('**', () => {});
  bus.publish('d.a', {});
  clock.nowMs = Date.UTC(2026, 9, 17, 0, 1, 0);
  bus.publish('d.b', {});

  const files = readdirSync(dir).sort();
  assert.ok(files.includes('__publish_audit.2026-10-16.0.jsonl'), `files: ${files}`);
  assert.ok(files.includes(PUBLISH_AUDIT_LIVE_NAME));
  // The live file holds only the new day's line; the archive holds the old one.
  assert.equal(liveLines(dir).length, 1);
  const archived = readFileSync(join(dir, '__publish_audit.2026-10-16.0.jsonl'), 'utf8');
  assert.ok(archived.includes('"topic":"d.a"'));
  // The read view still sees both.
  assert.equal(bus.getPublishAudit().length, 2);
  bus.destroy?.();
});

test('publish audit: live-file byte budget rotates (EB-61 archive convention)', () => {
  const dir = tempDir('pubaudit-');
  const bus = new EventBus({
    durableLogDir: dir,
    publishAudit: { maxBytes: 200 },
  });
  bus.subscribe('**', () => {});
  bus.publish('s.a', {});
  bus.publish('s.b', {});
  bus.publish('s.c', {});

  const files = readdirSync(dir);
  const archived = files.filter(
    (f) => f !== PUBLISH_AUDIT_LIVE_NAME && f.startsWith('__publish_audit.'),
  );
  assert.ok(archived.length >= 1, `files: ${files}`);
  // Nothing is ever deleted by rotation: every publish is on disk somewhere.
  let totalLines = liveLines(dir).length;
  for (const f of archived) {
    totalLines += readFileSync(join(dir, f), 'utf8').split('\n').filter((l) => l.trim() !== '').length;
  }
  assert.equal(totalLines, 3);
  assert.equal(bus.getPublishAudit().length, 3);
  bus.destroy?.();
});

test('publish audit: a restarted bus recovers the read view from disk', async () => {
  const dir = tempDir('pubaudit-');
  {
    const bus = new EventBus({ durableLogDir: dir, publishAudit: true });
    bus.subscribe('**', () => {});
    bus.publish('r.a', {}, { publisherId: 'p1' });
    bus.publish('r.b', {});
    await flush();
    bus.destroy?.();
  }
  const bus2 = new EventBus({ durableLogDir: dir, publishAudit: true });
  const entries = bus2.getPublishAudit();
  assert.equal(entries.length, 2);
  assert.deepEqual(
    entries.map((e) => [e.topic, e.outcome]),
    [
      ['r.a', 'admitted'],
      ['r.b', 'admitted'],
    ],
  );
  assert.equal(entries[0].publisherId, 'p1');
  // New publishes continue the journal.
  bus2.subscribe('**', () => {});
  bus2.publish('r.c', {});
  assert.equal(bus2.getPublishAudit().length, 3);
  bus2.destroy?.();
});

test('publish audit: corrupt lines are skipped on recovery, never fatal', () => {
  const dir = tempDir('pubaudit-');
  const bus = new EventBus({ durableLogDir: dir, publishAudit: true });
  bus.subscribe('**', () => {});
  bus.publish('c.a', {});
  bus.destroy?.();
  // Corrupt the journal: garbage lines around the good one.
  const live = join(dir, PUBLISH_AUDIT_LIVE_NAME);
  const good = readFileSync(live, 'utf8');
  writeFileSync(live, `not json\n${good}{"v":999,"outcome":"admitted"}\n`, 'utf8');

  const bus2 = new EventBus({ durableLogDir: dir, publishAudit: true });
  const entries = bus2.getPublishAudit();
  assert.equal(entries.length, 1);
  assert.equal(entries[0].topic, 'c.a');
  bus2.destroy?.();
});

test('publish audit: best-effort — disk failures never throw into the publish path', () => {
  // A journal rooted at an existing *file*: every disk op fails.
  const file = join(tempDir('pubaudit-'), 'not-a-dir');
  writeFileSync(file, 'x', 'utf8');
  const log = new PublishAuditLog(file);
  // record() must not throw; the failure is counted.
  log.record({ outcome: 'admitted', topic: 't', seq: 1 });
  log.record({ outcome: 'rejected', topic: 't', reason: 'acl' });
  assert.equal(log.writeErrors(), 2);
  // The in-memory entries still land.
  assert.equal(log.size(), 2);
  assert.equal(log.query().length, 2);
});

test('publish audit: malformed traceparent omits traceId instead of failing', () => {
  const dir = tempDir('pubaudit-');
  const bus = new EventBus({ durableLogDir: dir, publishAudit: { maxEntries: 100 } });
  bus.subscribe('**', () => {});
  // 'not-a-traceparent' mints a fresh trace at sample time (tolerated),
  // but the journal cannot name a trace id for it — omitted, not fatal.
  bus.publish('m.a', {}, { traceparent: 'not-a-traceparent' });
  const [entry] = bus.getPublishAudit();
  assert.equal(entry.outcome, 'admitted');
  assert.equal(entry.traceId, undefined);
  bus.destroy?.();
});

test('publish audit: in-memory read view is bounded (oldest evicted)', () => {
  const dir = tempDir('pubaudit-');
  const bus = new EventBus({ durableLogDir: dir, publishAudit: { maxEntries: 5 } });
  bus.subscribe('**', () => {});
  for (let i = 0; i < 8; i++) bus.publish('w', { n: i });
  const entries = bus.getPublishAudit();
  assert.equal(entries.length, 5);
  // The newest 5 survive, oldest-first.
  assert.deepEqual(
    entries.map((e) => e.seq),
    [4, 5, 6, 7, 8],
  );
  // Disk still holds everything.
  assert.equal(liveLines(dir).length, 8);
  bus.destroy?.();
});

test('parsePublishAuditLine: rejects malformed lines', () => {
  assert.equal(parsePublishAuditLine('nope'), undefined);
  assert.equal(parsePublishAuditLine('{"v":2,"outcome":"admitted"}'), undefined);
  assert.equal(parsePublishAuditLine('{"v":1,"outcome":"admitted","topic":"t"}'), undefined); // no seq
  assert.equal(
    parsePublishAuditLine('{"v":1,"outcome":"admitted","topic":"t","seq":0,"admittedAtMs":1}'),
    undefined,
  ); // seq < 1
  assert.equal(
    parsePublishAuditLine('{"v":1,"outcome":"rejected","topic":"t","admittedAtMs":1}'),
    undefined,
  ); // no reason
  assert.equal(
    parsePublishAuditLine(
      '{"v":1,"outcome":"admitted","topic":"t","seq":1,"reason":"acl","admittedAtMs":1}',
    ),
    undefined,
  ); // admitted with reason
  const ok = parsePublishAuditLine(
    '{"v":1,"outcome":"admitted","topic":"t","seq":3,"admittedAtMs":42}',
  );
  assert.deepEqual(ok, { v: 1, outcome: 'admitted', topic: 't', seq: 3, admittedAtMs: 42 });
});
