/**
 * Admission-rejection hook live redirection (EB-428).
 *
 * `setAdmissionRejectedCallback` swaps the `onAdmissionRejected`
 * observer every publish-side admission rejection is handed to without
 * restarting the bus. There is no enable gate — any open bus can install
 * one — and `null` clears back to no observer. The new hook governs
 * from the next rejection onward, the rejection counters move regardless
 * of whether a hook is installed, and a throwing hook is swallowed by
 * `emitAdmissionRejected`. The callback function itself is never written
 * to the audit log: only whether one is installed is recorded
 * (`{ hasCallback, oldHasCallback }`). Re-installing the live callback
 * is a no-op with no audit.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn, type ChildProcess } from 'node:child_process';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  EventBus,
  type AdmissionRejectionEvent,
  type AdmissionRejectReason,
} from '../src/bus.ts';

const flush = () => new Promise((resolve) => setImmediate(resolve));

/**
 * A webhook stub in a child process (the publish pipeline calls the
 * webhook synchronously, so an in-process server would deadlock): always
 * answers `{"allowed": false}`.
 */
async function startWebhook(): Promise<{ proc: ChildProcess; url: string }> {
  const script = `
const http = require('node:http');
const server = http.createServer((req, res) => {
  let data = '';
  req.on('data', (c) => { data += c; });
  req.on('end', () => {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ allowed: false }));
  });
});
server.listen(0, '127.0.0.1', () => {
  console.log('READY ' + server.address().port);
});
`;
  const dir = mkdtempSync(join(tmpdir(), 'webhook-stub-'));
  const scriptPath = join(dir, 'stub.cjs');
  writeFileSync(scriptPath, script);
  const proc = spawn(process.execPath, [scriptPath], {
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  const url = await new Promise<string>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('stub did not start')), 10000);
    proc.stdout!.on('data', (chunk: Buffer) => {
      const text = chunk.toString();
      const m = text.match(/READY (\d+)/);
      if (m) {
        clearTimeout(timer);
        resolve(`http://127.0.0.1:${m[1]}/admit`);
      }
    });
    proc.on('error', (err) => {
      clearTimeout(timer);
      reject(err);
    });
  });
  return { proc, url };
}

async function stopWebhook(proc: ChildProcess): Promise<void> {
  proc.kill('SIGTERM');
  await new Promise<void>((resolve) => {
    const timer = setTimeout(resolve, 2000);
    proc.on('exit', () => {
      clearTimeout(timer);
      resolve();
    });
  });
}

/** A bus whose `orders` topic rejects non-object payloads by schema. */
function schemaBus(extra: Record<string, unknown> = {}): EventBus {
  const bus = new EventBus(extra);
  bus.setTopicSchema('orders', (p) => typeof p === 'object' && p !== null);
  return bus;
}

const audit = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'admission_rejected_callback_revised' });

test('installing a hook routes later rejections to it', () => {
  const seen: AdmissionRejectionEvent[] = [];
  const bus = schemaBus();
  // No hook at construction: the rejection is counted, not observed.
  assert.equal(bus.publish('orders', 'bad'), 0);
  assert.equal(seen.length, 0);
  assert.equal(bus.getStats().rejectedMessages, 1);
  bus.setAdmissionRejectedCallback((e) => seen.push(e));
  assert.equal(bus.publish('orders', 'also-bad'), 0);
  assert.equal(seen.length, 1);
  assert.equal(seen[0].topic, 'orders');
  assert.equal(seen[0].reason, 'schema');
  assert.equal(seen[0].payloadBytes, Buffer.byteLength('"also-bad"', 'utf8'));
  assert.equal(bus.getStats().rejectedMessages, 2);
  bus.destroy?.();
});

test('ACL denials and webhook denials both reach the new hook', async () => {
  const { proc, url } = await startWebhook();
  try {
    const seen: Array<{ topic: string; reason: AdmissionRejectReason }> = [];
    // The webhook gate runs before the ACL gate, so the two channels
    // need separate buses: a deny-all webhook would mask the ACL denial.
    const aclBus = new EventBus({
      acl: { rules: [{ pattern: 'admin.**', publish: 'deny', subscribe: 'deny' }] },
    });
    aclBus.setAdmissionRejectedCallback((e) => seen.push({ topic: e.topic, reason: e.reason }));
    assert.equal(aclBus.publish('admin.cmd', 1), 0);
    const webhookBus = new EventBus({ admissionWebhook: { url } });
    webhookBus.setAdmissionRejectedCallback((e) => seen.push({ topic: e.topic, reason: e.reason }));
    assert.equal(webhookBus.publish('orders', { id: 1 }), 0);
    assert.deepEqual(seen, [
      { topic: 'admin.cmd', reason: 'acl' },
      { topic: 'orders', reason: 'webhook-deny' },
    ]);
    aclBus.destroy?.();
    webhookBus.destroy?.();
  } finally {
    await stopWebhook(proc);
  }
});

test('clearing with null stops delivery and the counters keep moving', () => {
  const seen: AdmissionRejectionEvent[] = [];
  const bus = schemaBus({ onAdmissionRejected: (e: AdmissionRejectionEvent) => seen.push(e) });
  assert.equal(bus.publish('orders', 'bad'), 0);
  assert.equal(seen.length, 1);
  bus.setAdmissionRejectedCallback(null);
  assert.equal(bus.publish('orders', 'bad'), 0);
  assert.equal(seen.length, 1);
  assert.equal(bus.getStats().rejectedMessages, 2);
  // Clearing a bus with no callback is a no-op with no audit.
  const before = audit(bus).length;
  bus.setAdmissionRejectedCallback(null);
  assert.equal(audit(bus).length, before);
  bus.destroy?.();
});

test('non-functions are rejected and the live hook stays put', () => {
  const seen: AdmissionRejectionEvent[] = [];
  const bus = schemaBus({ onAdmissionRejected: (e: AdmissionRejectionEvent) => seen.push(e) });
  const bad: Array<unknown> = ['nope', 42, {}, [], undefined];
  for (const handler of bad) {
    assert.throws(
      () => bus.setAdmissionRejectedCallback(handler as never),
      RangeError,
      `expected RangeError for ${String(handler)}`,
    );
  }
  // The failed calls changed nothing: the live hook still observes.
  assert.equal(bus.publish('orders', 'bad'), 0);
  assert.equal(seen.length, 1);
  assert.equal(audit(bus).length, 0);
  bus.destroy?.();
});

test('re-installing the live callback is a no-op with no audit', () => {
  const handler = (_e: AdmissionRejectionEvent): void => {};
  const bus = schemaBus({ onAdmissionRejected: handler });
  bus.setAdmissionRejectedCallback(handler);
  assert.equal(audit(bus).length, 0);
  bus.destroy?.();
});

test('a closed bus refuses the swap', async () => {
  const bus = schemaBus();
  await bus.close();
  const handler = (_e: AdmissionRejectionEvent): void => {};
  assert.throws(() => bus.setAdmissionRejectedCallback(handler), RangeError);
  assert.throws(() => bus.setAdmissionRejectedCallback(null), RangeError);
  assert.equal(audit(bus).length, 0);
});

test('a throwing hook is swallowed and the publish path is unaffected', async () => {
  const bus = schemaBus();
  bus.setAdmissionRejectedCallback(() => {
    throw new Error('broken observer');
  });
  // The rejection still returns 0 (not a throw) and the counter moves.
  assert.equal(bus.publish('orders', 'bad'), 0);
  assert.equal(bus.getStats().rejectedMessages, 1);
  // An admitted publish still flows to subscribers.
  const received: unknown[] = [];
  bus.subscribe('market.btc', (msg) => received.push(msg.payload));
  assert.equal(bus.publish('market.btc', 2), 1);
  await flush();
  assert.deepEqual(received, [2]);
  bus.destroy?.();
});

test('the swap audits exact presence flags with the actor', () => {
  const first = (_e: AdmissionRejectionEvent): void => {};
  const second = (_e: AdmissionRejectionEvent): void => {};
  const bus = schemaBus();
  bus.setAdmissionRejectedCallback(first, 'ops');
  bus.setAdmissionRejectedCallback(second, 'ops');
  bus.setAdmissionRejectedCallback(null, 'ops');
  const entries = audit(bus);
  assert.equal(entries.length, 3);
  assert.deepEqual(entries[0].detail, { hasCallback: true, oldHasCallback: false });
  assert.equal(entries[0].actor, 'ops');
  assert.deepEqual(entries[1].detail, { hasCallback: true, oldHasCallback: true });
  assert.deepEqual(entries[2].detail, { hasCallback: false, oldHasCallback: true });
  // The function itself is never recorded — only its presence.
  assert.ok(!JSON.stringify(entries).includes('first'));
  bus.destroy?.();
});

test('setAdmissionRejectedCallback rejects an illegal actor', () => {
  const bus = schemaBus();
  const handler = (_e: AdmissionRejectionEvent): void => {};
  assert.throws(() => bus.setAdmissionRejectedCallback(handler, ''), RangeError);
  assert.throws(() => bus.setAdmissionRejectedCallback(handler, 42 as never), RangeError);
  assert.equal(audit(bus).length, 0);
  bus.destroy?.();
});

test('a restart restores the construction-time callback', () => {
  const constructionSeen: AdmissionRejectionEvent[] = [];
  const liveSeen: AdmissionRejectionEvent[] = [];
  const construction = (e: AdmissionRejectionEvent) => constructionSeen.push(e);
  const live = (e: AdmissionRejectionEvent) => liveSeen.push(e);
  // First bus: the live hook replaces the construction-time one.
  const bus = schemaBus({ onAdmissionRejected: construction });
  bus.setAdmissionRejectedCallback(live);
  assert.equal(bus.publish('orders', 'bad'), 0);
  assert.equal(constructionSeen.length, 0);
  assert.equal(liveSeen.length, 1);
  bus.destroy?.();
  // A fresh bus with the same construction options (a restart) fires
  // the construction-time hook again.
  const restarted = schemaBus({ onAdmissionRejected: construction });
  assert.equal(restarted.publish('orders', 'bad'), 0);
  assert.equal(constructionSeen.length, 1);
  assert.equal(liveSeen.length, 1);
  restarted.destroy?.();
});

test('two consecutive redirects both take effect', () => {
  const firstSeen: AdmissionRejectionEvent[] = [];
  const secondSeen: AdmissionRejectionEvent[] = [];
  const bus = schemaBus();
  bus.setAdmissionRejectedCallback((e) => firstSeen.push(e));
  assert.equal(bus.publish('orders', 'bad'), 0);
  assert.equal(firstSeen.length, 1);
  bus.setAdmissionRejectedCallback((e) => secondSeen.push(e));
  assert.equal(bus.publish('orders', 'bad'), 0);
  assert.equal(firstSeen.length, 1);
  assert.equal(secondSeen.length, 1);
  assert.equal(audit(bus).length, 2);
  bus.destroy?.();
});
