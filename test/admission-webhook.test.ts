/**
 * External admission webhook (EB-157): K8s-admission-style allow/deny
 * for publishes.
 *
 * - A webhook answering `{"allowed": true}` admits the publish.
 * - A webhook answering `{"allowed": false}` sheds it with admission
 *   reason `'webhook-deny'` — no sequence number, no durable-log write,
 *   no rate-limit budget — surfaced on `onAdmissionRejected`.
 * - A timeout/error applies `failureMode` (`'fail-closed'` denies,
 *   `'fail-open'` admits).
 * - The decision is journaled in the publish audit and counted in stats.
 *
 * NOTE: The webhook server MUST run in a separate process. The
 * admission check blocks the main thread via `Atomics.wait` (the
 * publish pipeline is synchronous), which would also block an
 * in-process HTTP server's event loop.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn, type ChildProcess } from 'node:child_process';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';

/**
 * Starts a webhook stub in a separate process; `responseJson` is the
 * JSON string the stub returns for every request. Returns the child
 * process and the URL. The stub prints "READY <port>" to stdout when
 * listening.
 */
async function startWebhook(
  responseJson: string,
  opts?: { status?: number; delayMs?: number },
): Promise<{ proc: ChildProcess; url: string }> {
  const status = opts?.status ?? 200;
  const delayMs = opts?.delayMs ?? 0;
  // The stub server script: listens on 127.0.0.1:0, prints READY <port>.
  const script = `
const http = require('node:http');
const server = http.createServer((req, res) => {
  let data = '';
  req.on('data', (c) => { data += c; });
  req.on('end', () => {
    const send = () => {
      res.writeHead(${status}, { 'content-type': 'application/json' });
      res.end(${JSON.stringify(responseJson)});
    };
    if (${delayMs} > 0) setTimeout(send, ${delayMs});
    else send();
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

test('webhook allow admits the publish', async (t) => {
  const { proc, url } = await startWebhook(JSON.stringify({ allowed: true }));
  t.after(() => stopWebhook(proc));
  const bus = new EventBus({ admissionWebhook: { url } });
  t.after(() => bus.close());

  let received = 0;
  bus.subscribe('orders.**', () => {
    received += 1;
  });
  const accepted = bus.publish('orders.created', { id: 1 });
  assert.equal(accepted, 1);
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(received, 1);
  const stats = bus.getStats();
  assert.equal(stats.webhookDeniedMessages, 0);
});

test('webhook deny sheds with webhook-deny and zero side effects', async (t) => {
  const { proc, url } = await startWebhook(
    JSON.stringify({ allowed: false, reason: 'policy says no' }),
  );
  t.after(() => stopWebhook(proc));
  const rejections: { reason: string }[] = [];
  const bus = new EventBus({
    admissionWebhook: { url },
    onAdmissionRejected: (e) => {
      rejections.push({ reason: e.reason });
    },
  });
  t.after(() => bus.close());

  let received = 0;
  bus.subscribe('orders.**', () => {
    received += 1;
  });
  const accepted = bus.publish('orders.created', { id: 1 });
  assert.equal(accepted, 0);
  assert.equal(received, 0);
  assert.equal(rejections.length, 1);
  assert.equal(rejections[0].reason, 'webhook-deny');
  const stats = bus.getStats();
  assert.equal(stats.webhookDeniedMessages, 1);
  assert.equal(stats.rejectedMessages, 1);
});

test('webhook timeout with fail-closed denies', async (t) => {
  // Delay longer than the timeout: the bus must apply the timeout, not hang.
  const { proc, url } = await startWebhook(JSON.stringify({ allowed: true }), {
    delayMs: 5000,
  });
  t.after(() => stopWebhook(proc));
  const bus = new EventBus({
    admissionWebhook: { url, timeoutMs: 200, failureMode: 'fail-closed' },
  });
  t.after(() => bus.close());

  const accepted = bus.publish('orders.created', { id: 1 });
  assert.equal(accepted, 0);
  const stats = bus.getStats();
  assert.equal(stats.webhookDeniedMessages, 1);
  assert.equal(stats.webhookErrorMessages, 1);
});

test('webhook timeout with fail-open admits', async (t) => {
  const { proc, url } = await startWebhook(JSON.stringify({ allowed: true }), {
    delayMs: 5000,
  });
  t.after(() => stopWebhook(proc));
  const bus = new EventBus({
    admissionWebhook: { url, timeoutMs: 200, failureMode: 'fail-open' },
  });
  t.after(() => bus.close());

  let received = 0;
  bus.subscribe('orders.**', () => {
    received += 1;
  });
  const accepted = bus.publish('orders.created', { id: 1 });
  assert.equal(accepted, 1);
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(received, 1);
  const stats = bus.getStats();
  assert.equal(stats.webhookDeniedMessages, 0);
});

test('webhook is skipped without configuration', async (t) => {
  const bus = new EventBus();
  t.after(() => bus.close());
  let received = 0;
  bus.subscribe('orders.**', () => {
    received += 1;
  });
  const accepted = bus.publish('orders.created', { id: 1 });
  assert.equal(accepted, 1);
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(received, 1);
  const stats = bus.getStats();
  assert.equal(stats.webhookDeniedMessages, 0);
});

test('invalid webhook options throw at construction', () => {
  assert.throws(() => new EventBus({ admissionWebhook: { url: '' } as never }), TypeError);
  assert.throws(
    () => new EventBus({ admissionWebhook: { url: 'ftp://x/admit' } }),
    RangeError,
  );
  assert.throws(
    () => new EventBus({ admissionWebhook: { url: 'http://x/admit', timeoutMs: -1 } }),
    RangeError,
  );
  assert.throws(
    () =>
      new EventBus({
        admissionWebhook: { url: 'http://x/admit', failureMode: 'sometimes' as never },
      }),
    RangeError,
  );
});

test('webhook deny is journaled in the publish audit', async (t) => {
  const { proc, url } = await startWebhook(JSON.stringify({ allowed: false }));
  t.after(() => stopWebhook(proc));
  const dir = mkdtempSync(join(tmpdir(), 'admission-webhook-audit-'));
  const bus = new EventBus({
    admissionWebhook: { url },
    durableLogDir: dir,
    publishAudit: true,
  });
  t.after(() => bus.close());

  bus.publish('orders.created', { id: 1 });
  const audit = bus.getPublishAudit({ topic: 'orders.created' });
  const denied = audit.find((e) => e.outcome === 'rejected' && e.reason === 'webhook-deny');
  assert.ok(denied, 'expected a webhook-deny audit entry');
});
