import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import {
  AUDIT_CHAIN_GENESIS,
  attachChainLink,
  canonicalJson,
  chainHash,
  hashChainedRecord,
  verifyChainLinks,
} from '../src/auditchain.ts';
import {
  PublishAuditLog,
  parsePublishAuditLine,
  PUBLISH_AUDIT_LIVE_NAME,
} from '../src/publishaudit.ts';
import { SubscriptionAuditLog, parseAuditLine } from '../src/subaudit.ts';

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

test('auditchain: canonical JSON is key-order stable', () => {
  const a = { z: 1, a: [3, 2], m: { y: 'x', b: 1 } };
  const b = { a: [3, 2], m: { b: 1, y: 'x' }, z: 1 };
  assert.equal(canonicalJson(a), canonicalJson(b));
  assert.equal(
    canonicalJson(a),
    '{"a":[3,2],"m":{"b":1,"y":"x"},"z":1}',
  );
});

test('auditchain: attachChainLink chains prevHash/hash deterministically', () => {
  const first = attachChainLink(AUDIT_CHAIN_GENESIS, { v: 1, topic: 't' });
  assert.equal(first.prevHash, AUDIT_CHAIN_GENESIS);
  assert.ok(/^[0-9a-f]{64}$/.test(first.hash));
  assert.equal(first.hash, hashChainedRecord(AUDIT_CHAIN_GENESIS, { v: 1, topic: 't' }));
  const second = attachChainLink(first.hash, { v: 1, topic: 'u' });
  assert.equal(second.prevHash, first.hash);
  // The chain fields themselves are not part of the hashed body.
  assert.equal(second.hash, chainHash(first.hash, canonicalJson({ v: 1, topic: 'u' })));
});

test('auditchain: verifyChainLinks detects tampering, deletion, and splice', () => {
  const r1 = attachChainLink(AUDIT_CHAIN_GENESIS, { v: 1, n: 1 });
  const r2 = attachChainLink(r1.hash, { v: 1, n: 2 });
  const r3 = attachChainLink(r2.hash, { v: 1, n: 3 });

  assert.deepEqual(verifyChainLinks([r1, r2, r3]), { ok: true, checked: 3 });
  assert.deepEqual(verifyChainLinks([]), { ok: true, checked: 0 });

  // Tampered content: the stored hash no longer matches.
  const tampered = [
    r1,
    { ...r2, n: 999 },
    r3,
  ];
  const t = verifyChainLinks(tampered);
  assert.equal(t.ok, false);
  assert.equal(t.brokenAt, 1);
  assert.equal(t.reason, 'content-tampered');
  assert.equal(t.expected, r2.hash);

  // Deleted record: r3's prevHash no longer links to its predecessor.
  const deleted = [r1, r3];
  const d = verifyChainLinks(deleted);
  assert.equal(d.ok, false);
  assert.equal(d.brokenAt, 1);
  assert.equal(d.reason, 'link-broken');
  assert.equal(d.expected, r1.hash);
  assert.equal(d.actual, r3.prevHash);

  // Spliced record from another chain: link breaks at the splice point.
  const other = attachChainLink(AUDIT_CHAIN_GENESIS, { v: 1, n: 'evil' });
  const s = verifyChainLinks([r1, other, r3]);
  assert.equal(s.ok, false);
  assert.equal(s.brokenAt, 1);
  assert.equal(s.reason, 'link-broken');
});

test('publish audit journal: records are chained and verify', async () => {
  const dir = tempDir('chain-pubaudit-');
  const clock = controllableClock(5_000);
  const bus = new EventBus({ durableLogDir: dir, publishAudit: true, now: clock.now });
  bus.subscribe('orders.*', () => {});
  bus.publish('orders.created', { n: 1 });
  bus.publish('orders.paid', { n: 2 });
  bus.publish('orders.created', { n: 3 });
  await flush();

  const entries = bus.getPublishAudit();
  assert.equal(entries.length, 3);
  // Genesis-anchored, pairwise linked.
  assert.equal(entries[0].prevHash, AUDIT_CHAIN_GENESIS);
  for (const e of entries) {
    assert.ok(/^[0-9a-f]{64}$/.test(e.prevHash));
    assert.ok(/^[0-9a-f]{64}$/.test(e.hash));
  }
  assert.equal(entries[1].prevHash, entries[0].hash);
  assert.equal(entries[2].prevHash, entries[1].hash);

  const verification = bus.verifyPublishAuditChain();
  assert.deepEqual(verification, { ok: true, checked: 3 });

  // Disk and memory carry the same links.
  const lines = liveLines(dir).map((l) => JSON.parse(l));
  assert.equal(lines.length, 3);
  assert.equal(lines[0].prevHash, AUDIT_CHAIN_GENESIS);
  assert.equal(lines[2].hash, entries[2].hash);
  bus.destroy?.();
});

test('publish audit journal: tampering with a journaled record is detected', async () => {
  const dir = tempDir('chain-pubaudit-tamper-');
  const bus = new EventBus({ durableLogDir: dir, publishAudit: true });
  bus.subscribe('t', () => {});
  bus.publish('t', { n: 1 });
  bus.publish('t', { n: 2 });
  await flush();

  const entries = bus.getPublishAudit();
  assert.equal(entries.length, 2);
  entries[0].topic = 'forged.topic'; // rewrite a journaled field
  const verification = bus.verifyPublishAuditChain();
  assert.equal(verification.ok, false);
  assert.equal(verification.brokenAt, 0);
  assert.equal(verification.reason, 'content-tampered');
  bus.destroy?.();
});

test('publish audit journal: a deleted disk line breaks the recovered chain', async () => {
  const dir = tempDir('chain-pubaudit-del-');
  const bus = new EventBus({ durableLogDir: dir, publishAudit: true });
  bus.subscribe('t', () => {});
  bus.publish('t', { n: 1 });
  bus.publish('t', { n: 2 });
  bus.publish('t', { n: 3 });
  await flush();
  bus.destroy?.();

  // Delete the middle line from the on-disk journal, simulating tampering.
  const lines = liveLines(dir);
  assert.equal(lines.length, 3);
  writeFileSync(join(dir, PUBLISH_AUDIT_LIVE_NAME), `${lines[0]}\n${lines[2]}\n`, 'utf8');

  const bus2 = new EventBus({ durableLogDir: dir, publishAudit: true });
  const recovered = bus2.getPublishAudit();
  assert.equal(recovered.length, 2);
  const verification = bus2.verifyPublishAuditChain();
  assert.equal(verification.ok, false);
  assert.equal(verification.brokenAt, 1);
  assert.equal(verification.reason, 'link-broken');

  // The chain continues across the restart: new records link to the
  // recovered head.
  bus2.subscribe('t', () => {});
  bus2.publish('t', { n: 4 });
  await flush();
  const after = bus2.getPublishAudit();
  assert.equal(after.length, 3);
  assert.equal(after[2].prevHash, after[1].hash);
  bus2.destroy?.();
});

test('publish audit journal: pre-chain lines parse and anchor the chain', () => {
  const dir = tempDir('chain-pubaudit-legacy-');
  // A journal file written before chaining existed: no chain fields.
  writeFileSync(
    join(dir, PUBLISH_AUDIT_LIVE_NAME),
    '{"v":1,"outcome":"admitted","topic":"t","seq":1,"admittedAtMs":1}\n',
    'utf8',
  );
  const log = new PublishAuditLog(dir);
  assert.equal(log.size(), 1);
  const [entry] = log.query();
  assert.equal(entry.prevHash, undefined);
  assert.equal(entry.hash, undefined);
  // Verifies as an anchor (nothing to check against), and new records
  // chain onto it.
  assert.deepEqual(log.verifyAuditChain(), { ok: true, checked: 1 });
  log.record({ outcome: 'admitted', topic: 't', seq: 2 });
  const entries = log.query();
  assert.equal(entries.length, 2);
  assert.ok(/^[0-9a-f]{64}$/.test(entries[1].prevHash));
  assert.ok(/^[0-9a-f]{64}$/.test(entries[1].hash));
  assert.deepEqual(log.verifyAuditChain(), { ok: true, checked: 2 });
});

test('publish audit journal: malformed chain fields are corrupt lines', () => {
  assert.equal(
    parsePublishAuditLine(
      '{"v":1,"outcome":"admitted","topic":"t","seq":1,"admittedAtMs":1,"prevHash":"xyz","hash":"abc"}',
    ),
    undefined,
  );
  assert.equal(
    parsePublishAuditLine(
      '{"v":1,"outcome":"admitted","topic":"t","seq":1,"admittedAtMs":1,"prevHash":"not-hex","hash":"00"}',
    ),
    undefined,
  );
  const ok = parsePublishAuditLine(
    `{"v":1,"outcome":"admitted","topic":"t","seq":1,"admittedAtMs":1,"prevHash":"${'a'.repeat(64)}","hash":"${'b'.repeat(64)}"}`,
  );
  assert.equal(ok?.prevHash, 'a'.repeat(64));
  assert.equal(ok?.hash, 'b'.repeat(64));
});

test('subscription audit log: events are chained and verify', () => {
  const log = new SubscriptionAuditLog({});
  log.append({ action: 'subscribe', actor: 'op', subId: 's1', pattern: 't' });
  log.append({ action: 'topic_paused', actor: 'op', pattern: 't', detail: { mode: 'shed' } });
  log.append({ action: 'unsubscribe', actor: 'op', subId: 's1', pattern: 't' });

  const events = log.query();
  assert.equal(events.length, 3);
  assert.equal(events[0].prevHash, AUDIT_CHAIN_GENESIS);
  assert.equal(events[1].prevHash, events[0].hash);
  assert.equal(events[2].prevHash, events[1].hash);
  assert.deepEqual(log.verifyAuditChain(), { ok: true, checked: 3 });
});

test('subscription audit log: tampered event detail is detected', () => {
  const log = new SubscriptionAuditLog({});
  log.append({ action: 'subscribe', actor: 'op', subId: 's1', pattern: 't' });
  log.append({ action: 'subscriber_quarantined', actor: 'system', subId: 's1', pattern: 't' });
  const events = log.query();
  events[1].subId = 'forged';
  const verification = log.verifyAuditChain();
  assert.equal(verification.ok, false);
  assert.equal(verification.brokenAt, 1);
  assert.equal(verification.reason, 'content-tampered');
});

test('subscription audit log: disk persistence carries the chain across restarts', () => {
  const dir = tempDir('chain-subaudit-');
  const clock = controllableClock(9_000);
  const log = new SubscriptionAuditLog({ dir, now: clock.now });
  log.append({ action: 'subscribe', actor: 'op', subId: 's1', pattern: 't' });
  clock.nowMs += 1_000;
  log.append({ action: 'unsubscribe', actor: 'op', subId: 's1', pattern: 't' });

  const log2 = new SubscriptionAuditLog({ dir, now: clock.now });
  const events = log2.query();
  assert.equal(events.length, 2);
  assert.equal(events[0].prevHash, AUDIT_CHAIN_GENESIS);
  assert.equal(events[1].prevHash, events[0].hash);
  assert.deepEqual(log2.verifyAuditChain(), { ok: true, checked: 2 });

  // New events link to the recovered head.
  log2.append({ action: 'subscribe', actor: 'op', subId: 's2', pattern: 'u' });
  const after = log2.query();
  assert.equal(after[2].prevHash, after[1].hash);
  assert.deepEqual(log2.verifyAuditChain(), { ok: true, checked: 3 });
});

test('subscription audit log: malformed chain fields are corrupt lines', () => {
  assert.equal(
    parseAuditLine('{"v":1,"at":1,"action":"subscribe","actor":"op","hash":"short"}'),
    undefined,
  );
  const ok = parseAuditLine(
    `{"v":1,"at":1,"action":"subscribe","actor":"op","prevHash":"${'c'.repeat(64)}","hash":"${'d'.repeat(64)}"}`,
  );
  assert.equal(ok?.prevHash, 'c'.repeat(64));
  assert.equal(ok?.hash, 'd'.repeat(64));
});

test('EventBus exposes both chain verifiers', async () => {
  const dir = tempDir('chain-bus-');
  const bus = new EventBus({ durableLogDir: dir, publishAudit: true });
  const sub = bus.subscribe('t', () => {});
  bus.publish('t', { n: 1 });
  await flush();

  assert.deepEqual(bus.verifySubscriptionAuditChain(), { ok: true, checked: 1 });
  assert.deepEqual(bus.verifyPublishAuditChain(), { ok: true, checked: 1 });

  // Quarantine + resume audit events join the same chain.
  sub.unsubscribe();
  const verification = bus.verifySubscriptionAuditChain();
  assert.equal(verification.ok, true);
  assert.equal(verification.checked, 2);
  bus.destroy?.();
});

test('EventBus.verifyPublishAuditChain on a bus without the journal', () => {
  const bus = new EventBus();
  assert.deepEqual(bus.verifyPublishAuditChain(), { ok: true, checked: 0 });
  assert.deepEqual(bus.verifySubscriptionAuditChain(), { ok: true, checked: 0 });
  bus.destroy?.();
});
