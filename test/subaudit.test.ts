import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, readdirSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import {
  SubscriptionAuditLog,
  parseAuditLine,
  SUBSCRIPTION_AUDIT_DEFAULT_MAX_ENTRIES,
} from '../src/subaudit.ts';

const flush = () => new Promise((resolve) => setImmediate(resolve));

test('subscribe and unsubscribe are audited with the default system actor', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('a.*', () => {});
  sub.unsubscribe();
  const events = bus.getSubscriptionAudit();
  assert.equal(events.length, 2);
  assert.equal(events[0].action, 'subscribe');
  assert.equal(events[0].subId, sub.id);
  assert.equal(events[0].pattern, 'a.*');
  assert.equal(events[0].actor, 'system');
  assert.equal(events[1].action, 'unsubscribe');
  assert.equal(events[1].subId, sub.id);
  assert.equal(events[1].pattern, 'a.*');
  assert.ok(events[0].at <= events[1].at);
});

test('a custom actor is recorded on the subscribe audit event', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('a.*', () => {}, { actor: 'ops-console' });
  const events = bus.getSubscriptionAudit();
  assert.equal(events.length, 1);
  assert.equal(events[0].action, 'subscribe');
  assert.equal(events[0].actor, 'ops-console');
  sub.unsubscribe();
});

test('an invalid actor fails fast and leaves no audit entry', () => {
  const bus = new EventBus();
  assert.throws(() => bus.subscribe('a.*', () => {}, { actor: '' }), RangeError);
  assert.throws(
    () => bus.subscribe('a.*', () => {}, { actor: 42 as unknown as string }),
    RangeError,
  );
  assert.equal(bus.getSubscriptionAudit().length, 0);
  assert.equal(bus.subscriberCount(), 0);
});

test('updateSubscriptionPattern audits the change with the old pattern', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('a.*', () => {});
  assert.equal(bus.updateSubscriptionPattern(sub.id, 'b.*'), true);
  const events = bus.getSubscriptionAudit({ action: 'update_pattern' });
  assert.equal(events.length, 1);
  assert.equal(events[0].subId, sub.id);
  assert.equal(events[0].pattern, 'b.*');
  assert.deepEqual(events[0].detail, { from: 'a.*' });
  sub.unsubscribe();
});

test('a failed pattern update audits nothing', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('a.*', () => {});
  assert.equal(bus.updateSubscriptionPattern('sub-nope', 'b.*'), false);
  assert.throws(() => bus.updateSubscriptionPattern(sub.id, ''), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'update_pattern' }).length, 0);
  sub.unsubscribe();
});

test('group join, leave and rebalance are audited', async () => {
  const bus = new EventBus();
  const a = bus.subscribeToGroup('g', 't.*', () => {});
  const b = bus.subscribeToGroup('g', 't.*', () => {});
  await flush();
  const events = bus.getSubscriptionAudit();
  const actions = events.map((e) => e.action);
  // Each join: subscribe + group_join + one rebalance entry.
  assert.deepEqual(actions, [
    'subscribe',
    'group_join',
    'rebalance',
    'subscribe',
    'group_join',
    'rebalance',
  ]);
  const join = events[1];
  assert.equal(join.groupId, 'g');
  assert.equal(join.subId, a.id);
  assert.equal(join.pattern, 't.*');
  const rebalance = events[2];
  assert.equal(rebalance.groupId, 'g');
  assert.deepEqual(rebalance.detail, { trigger: 'join', memberId: a.id, members: 1 });
  b.unsubscribe();
  await flush();
  const after = bus.getSubscriptionAudit();
  const tail = after.slice(-3).map((e) => e.action);
  assert.deepEqual(tail, ['unsubscribe', 'group_leave', 'rebalance']);
  a.unsubscribe();
});

test('getSubscriptionAudit filters by time window, action and limit', () => {
  let nowMs = 1_000_000;
  const bus = new EventBus({ now: () => nowMs });
  const sub = bus.subscribe('a.*', () => {});
  nowMs += 100;
  bus.updateSubscriptionPattern(sub.id, 'b.*');
  nowMs += 100;
  sub.unsubscribe();

  assert.equal(bus.getSubscriptionAudit({ since: 1_000_050 }).length, 2);
  assert.equal(bus.getSubscriptionAudit({ until: 1_000_100 }).length, 1);
  assert.equal(bus.getSubscriptionAudit({ action: 'subscribe' }).length, 1);
  assert.equal(
    bus.getSubscriptionAudit({ action: ['subscribe', 'unsubscribe'] }).length,
    2,
  );
  const limited = bus.getSubscriptionAudit({ limit: 2 });
  assert.equal(limited.length, 2);
  // Oldest-first among the most recent matches.
  assert.equal(limited[0].action, 'update_pattern');
  assert.equal(limited[1].action, 'unsubscribe');
});

test('getSubscriptionAudit rejects illegal queries', () => {
  const bus = new EventBus();
  assert.throws(() => bus.getSubscriptionAudit({ since: -1 }), RangeError);
  assert.throws(() => bus.getSubscriptionAudit({ limit: 0 }), RangeError);
  assert.throws(
    () => bus.getSubscriptionAudit({ action: 'nonsense' as never }),
    RangeError,
  );
});

test('audit log writes daily JSONL files and back-fills on restart', () => {
  const dir = mkdtempSync(join(tmpdir(), 'subaudit-'));
  // 2026-10-11 23:59 UTC and one minute later (next UTC day).
  let nowMs = Date.UTC(2026, 9, 11, 23, 59, 0);
  const bus = new EventBus({ subscriptionAuditDir: dir, now: () => nowMs });
  const sub = bus.subscribe('a.*', () => {});
  nowMs += 60_000;
  sub.unsubscribe();
  const files = readdirSync(dir).sort();
  assert.deepEqual(files, ['audit-2026-10-11.jsonl', 'audit-2026-10-12.jsonl']);
  const day1 = readFileSync(join(dir, files[0]), 'utf8').trim().split('\n');
  assert.equal(day1.length, 1);
  assert.equal(JSON.parse(day1[0]).action, 'subscribe');

  const reopened = new EventBus({ subscriptionAuditDir: dir, now: () => nowMs });
  const events = reopened.getSubscriptionAudit();
  assert.equal(events.length, 2);
  assert.equal(events[0].action, 'subscribe');
  assert.equal(events[1].action, 'unsubscribe');
});

test('corrupt audit lines are skipped on recovery', () => {
  const dir = mkdtempSync(join(tmpdir(), 'subaudit-'));
  writeFileSync(
    join(dir, 'audit-2026-10-11.jsonl'),
    '{"v":1,"at":1,"action":"subscribe","actor":"x","subId":"s1","pattern":"a.*"}\n' +
      'not json at all\n' +
      '{"v":1,"at":2,"action":"bogus-action","actor":"x"}\n' +
      '{"v":1,"at":3,"action":"unsubscribe","actor":"x","subId":"s1","pattern":"a.*"}\n',
    'utf8',
  );
  const bus = new EventBus({ subscriptionAuditDir: dir });
  const events = bus.getSubscriptionAudit();
  assert.equal(events.length, 2);
  assert.equal(events[0].action, 'subscribe');
  assert.equal(events[1].action, 'unsubscribe');
});

test('disk failures are counted, never thrown into the subscribe path', () => {
  const dir = mkdtempSync(join(tmpdir(), 'subaudit-'));
  const blocker = join(dir, 'blocker');
  writeFileSync(blocker, 'i am a file, not a directory', 'utf8');
  // Point the audit dir at a file: mkdir fails, appends fail — every
  // subscribe must still succeed.
  const bus = new EventBus({ subscriptionAuditDir: blocker });
  const sub = bus.subscribe('a.*', () => {});
  assert.equal(bus.subscriberCount(), 1);
  const events = bus.getSubscriptionAudit();
  assert.equal(events.length, 1);
  assert.equal(events[0].action, 'subscribe');
  sub.unsubscribe();
});

test('the in-memory index is bounded and evicts the oldest entries', () => {
  const log = new SubscriptionAuditLog({ maxEntries: 5 });
  for (let i = 0; i < 8; i++) {
    log.append({ action: 'subscribe', actor: 'system', subId: `s${i}` });
  }
  assert.equal(log.size(), 5);
  const events = log.query();
  assert.equal(events[0].subId, 's3');
  assert.equal(events[4].subId, 's7');
});

test('constructor validates its options', () => {
  assert.throws(
    () => new EventBus({ subscriptionAuditDir: '' }),
    RangeError,
  );
  assert.throws(
    () => new EventBus({ subscriptionAuditMaxEntries: 0 }),
    RangeError,
  );
  assert.throws(
    () => new SubscriptionAuditLog({ maxEntries: 1.5 }),
    RangeError,
  );
  assert.equal(SUBSCRIPTION_AUDIT_DEFAULT_MAX_ENTRIES, 1000);
});

test('parseAuditLine rejects malformed entries', () => {
  assert.equal(parseAuditLine('garbage'), undefined);
  assert.equal(parseAuditLine('{"v":2,"at":1,"action":"subscribe","actor":"x"}'), undefined);
  assert.equal(parseAuditLine('{"v":1,"at":"x","action":"subscribe","actor":"x"}'), undefined);
  const good = parseAuditLine(
    '{"v":1,"at":7,"action":"subscribe","actor":"ops","subId":"s","detail":{"from":"a"}}',
  );
  assert.ok(good !== undefined);
  assert.equal(good.at, 7);
  assert.deepEqual(good.detail, { from: 'a' });
});

test('lease expiry unsubscribe is audited as a system unsubscribe', async () => {
  const bus = new EventBus();
  bus.subscribe('a.*', () => {}, { leaseMs: 20 });
  await new Promise((resolve) => setTimeout(resolve, 60));
  const events = bus.getSubscriptionAudit();
  assert.deepEqual(
    events.map((e) => e.action),
    ['subscribe', 'unsubscribe'],
  );
  assert.equal(events[1].actor, 'system');
});
