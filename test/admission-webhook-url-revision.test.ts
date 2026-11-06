/**
 * Bus-level admission webhook URL repoint (EB-408).
 *
 * `setAdmissionWebhookUrl` repoints the policy endpoint live — the
 * next admission decision POSTs to the new URL, with no restart and
 * no loss of live bus state. A decision already dispatched keeps the
 * URL it started with (the worker is spawned per call with the URL in
 * its worker data); the timeout, failure mode, and headers keep their
 * live values. Only a bus that already enabled `admissionWebhook`
 * can be repointed; a closed bus refuses; illegal URLs fail closed
 * with the live URL untouched; a repoint identical to the current URL
 * (string equality) is a no-op with no audit.
 *
 * NOTE: The webhook server MUST run in a separate process. The
 * admission check blocks the main thread via `Atomics.wait` (the
 * publish pipeline is synchronous), which would also block an
 * in-process HTTP server's event loop.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn, type ChildProcess } from 'node:child_process';
import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { decideAdmission } from '../src/admission-webhook.ts';

/**
 * Starts a webhook stub in a separate process; `responseJson` is the
 * JSON string the stub returns for every request. Returns the child
 * process and the URL. The stub prints "READY <port>" to stdout when
 * listening. When `headersLogPath` is given, every request's headers
 * are appended to that file as one JSON object per line (one line per
 * decision request, so the line count is the request count).
 */
async function startWebhook(
  responseJson: string,
  opts?: { status?: number; delayMs?: number; headersLogPath?: string },
): Promise<{ proc: ChildProcess; url: string }> {
  const status = opts?.status ?? 200;
  const delayMs = opts?.delayMs ?? 0;
  const logPath = opts?.headersLogPath;
  // The stub server script: listens on 127.0.0.1:0, prints READY <port>.
  const script = `
const http = require('node:http');
const fs = require('node:fs');
const logPath = ${JSON.stringify(logPath ?? null)};
const server = http.createServer((req, res) => {
  let data = '';
  req.on('data', (c) => { data += c; });
  req.on('end', () => {
    if (logPath) fs.appendFileSync(logPath, JSON.stringify(req.headers) + '\\n');
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

/**
 * A URL that deterministically refuses connections: listen on an
 * ephemeral port, then close it before returning.
 */
async function closedPortUrl(): Promise<string> {
  const server = createServer();
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const port = (server.address() as { port: number }).port;
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return `http://127.0.0.1:${port}/admit`;
}

/** A fresh empty headers-log file; returns its path. */
function freshLog(): string {
  const dir = mkdtempSync(join(tmpdir(), 'webhook-headers-'));
  const logPath = join(dir, 'headers.jsonl');
  writeFileSync(logPath, '');
  return logPath;
}

/** The request headers logged so far, one object per decision request. */
function readLog(logPath: string): Array<Record<string, string>> {
  return readFileSync(logPath, 'utf8')
    .split('\n')
    .filter((l) => l.trim() !== '')
    .map((l) => JSON.parse(l) as Record<string, string>);
}

const audit = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'admission_webhook_url_revised' });

test('repoint sends the next decision to the new endpoint and the old one stops receiving', async (t) => {
  const oldLog = freshLog();
  const newLog = freshLog();
  const oldStub = await startWebhook(JSON.stringify({ allowed: true }), {
    headersLogPath: oldLog,
  });
  t.after(() => stopWebhook(oldStub.proc));
  const newStub = await startWebhook(JSON.stringify({ allowed: false }), {
    headersLogPath: newLog,
  });
  t.after(() => stopWebhook(newStub.proc));
  const bus = new EventBus({ admissionWebhook: { url: oldStub.url } });
  t.after(() => bus.close());
  bus.subscribe('orders.**', () => {});

  // Admitted by the old policy endpoint.
  assert.equal(bus.publish('orders.created', { id: 1 }), 1);
  assert.equal(readLog(oldLog).length, 1);
  assert.equal(readLog(newLog).length, 0);

  bus.setAdmissionWebhookUrl(newStub.url);
  // Denied by the new policy endpoint — and the old one heard nothing.
  assert.equal(bus.publish('orders.created', { id: 2 }), 0);
  assert.equal(readLog(oldLog).length, 1, 'no further decision may go to the old endpoint');
  assert.equal(readLog(newLog).length, 1, 'the next decision must POST to the new endpoint');
});

test('a decision dispatched before the repoint keeps the old URL', async (t) => {
  const { proc, url } = await startWebhook(JSON.stringify({ allowed: true }));
  t.after(() => stopWebhook(proc));
  const bus = new EventBus({ admissionWebhook: { url } });
  t.after(() => bus.close());

  // The bus reads the live field per publish, so a decision's URL is
  // bound at dispatch: call decideAdmission with the pre-repoint
  // options after the bus has moved on, and it still consults the
  // old endpoint.
  const before = { url, timeoutMs: 1000, failureMode: 'fail-closed' as const };
  const dead = await closedPortUrl();
  bus.setAdmissionWebhookUrl(dead);
  const verdict = decideAdmission(before, {
    topic: 'orders.created',
    payloadSha256: '0'.repeat(64),
    payloadBytes: 2,
  });
  assert.equal(verdict.allowed, true, 'the dispatched decision must still consult the old URL');
  // The bus itself has moved: the next publish hits the dead endpoint.
  bus.subscribe('orders.**', () => {});
  assert.equal(bus.publish('orders.created', { id: 1 }), 0);
});

test('setAdmissionWebhookUrl fails closed when admissionWebhook is not enabled', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setAdmissionWebhookUrl('http://127.0.0.1:9/admit'), RangeError);
  assert.equal(audit(bus).length, 0);
});

test('setAdmissionWebhookUrl rejects illegal URLs and leaves the live URL untouched', async (t) => {
  const oldLog = freshLog();
  const { proc, url } = await startWebhook(JSON.stringify({ allowed: true }), {
    headersLogPath: oldLog,
  });
  t.after(() => stopWebhook(proc));
  const bus = new EventBus({ admissionWebhook: { url } });
  t.after(() => bus.close());
  bus.subscribe('orders.**', () => {});

  for (const bad of ['', 'not a url', 'ftp://example.com/admit', 'http://', 42]) {
    assert.throws(
      () => bus.setAdmissionWebhookUrl(bad as never),
      RangeError,
      `expected RangeError for ${JSON.stringify(bad)}`,
    );
  }
  assert.throws(() => bus.setAdmissionWebhookUrl(url, ''), RangeError, 'an illegal actor throws');
  assert.equal(audit(bus).length, 0);
  // The live URL is untouched: the old stub still admits.
  assert.equal(bus.publish('orders.created', { id: 1 }), 1);
  assert.equal(readLog(oldLog).length, 1);
  // And the bus still repoints afterwards.
  const newLog = freshLog();
  const other = await startWebhook(JSON.stringify({ allowed: true }), {
    headersLogPath: newLog,
  });
  t.after(() => stopWebhook(other.proc));
  bus.setAdmissionWebhookUrl(other.url);
  assert.equal(bus.publish('orders.created', { id: 2 }), 1);
  assert.equal(readLog(newLog).length, 1);
  assert.equal(readLog(oldLog).length, 1);
});

test('repointing to the current URL is a no-op with no audit', async (t) => {
  const log = freshLog();
  const { proc, url } = await startWebhook(JSON.stringify({ allowed: true }), {
    headersLogPath: log,
  });
  t.after(() => stopWebhook(proc));
  const bus = new EventBus({ admissionWebhook: { url } });
  t.after(() => bus.close());
  bus.subscribe('orders.**', () => {});

  bus.setAdmissionWebhookUrl(url);
  assert.equal(audit(bus).length, 0);
  // The untouched URL still governs the next decision.
  assert.equal(bus.publish('orders.created', { id: 1 }), 1);
  assert.equal(readLog(log).length, 1);
});

test('setAdmissionWebhookUrl audits the repoint with old and new URL', async (t) => {
  const oldStub = await startWebhook(JSON.stringify({ allowed: true }));
  t.after(() => stopWebhook(oldStub.proc));
  const newStub = await startWebhook(JSON.stringify({ allowed: true }));
  t.after(() => stopWebhook(newStub.proc));
  const bus = new EventBus({ admissionWebhook: { url: oldStub.url } });
  t.after(() => bus.close());

  bus.setAdmissionWebhookUrl(newStub.url, 'ops');
  const entries = audit(bus);
  assert.equal(entries.length, 1);
  assert.equal(entries[0].actor, 'ops');
  assert.deepEqual(entries[0].detail, { url: newStub.url, oldUrl: oldStub.url });
});

test('timeoutMs, failureMode, and headers survive a repoint', async (t) => {
  const oldLog = freshLog();
  const newLog = freshLog();
  const oldStub = await startWebhook(JSON.stringify({ allowed: true }), {
    delayMs: 2500,
    headersLogPath: oldLog,
  });
  t.after(() => stopWebhook(oldStub.proc));
  const newStub = await startWebhook(JSON.stringify({ allowed: true }), {
    delayMs: 2500,
    headersLogPath: newLog,
  });
  t.after(() => stopWebhook(newStub.proc));
  const bus = new EventBus({
    admissionWebhook: {
      url: oldStub.url,
      timeoutMs: 1200,
      failureMode: 'fail-closed',
      headers: { 'x-test-a': 'one' },
    },
  });
  t.after(() => bus.close());
  bus.subscribe('orders.**', () => {});

  // The stub answers after 2500ms; the 1200ms timeout denies. The
  // timeout is deliberately generous: the per-call worker needs time
  // to start up and dispatch the request before the wait expires,
  // or the stub would never see the decision at all.
  assert.equal(bus.publish('orders.created', { id: 1 }), 0);
  assert.equal(bus.getStats().webhookErrorMessages, 1);

  bus.setAdmissionWebhookUrl(newStub.url);
  // The timeout is still 1200ms: the new stub's 2500ms answer still
  // times out and denies — and the same headers arrive there.
  assert.equal(bus.publish('orders.created', { id: 2 }), 0);
  assert.equal(bus.getStats().webhookErrorMessages, 2);
  assert.equal(readLog(newLog).length, 1);
  assert.equal(readLog(newLog)[0]['x-test-a'], 'one');

  // The failure mode is still fail-closed: a dead endpoint still
  // denies instead of admitting.
  const dead = await closedPortUrl();
  bus.setAdmissionWebhookUrl(dead);
  assert.equal(bus.publish('orders.created', { id: 3 }), 0);
  assert.equal(bus.getStats().webhookErrorMessages, 3);
});

test('fail-closed: repoint to a dead endpoint still denies', async (t) => {
  const { proc, url } = await startWebhook(JSON.stringify({ allowed: true }));
  t.after(() => stopWebhook(proc));
  const bus = new EventBus({
    admissionWebhook: { url, timeoutMs: 500, failureMode: 'fail-closed' },
  });
  t.after(() => bus.close());
  bus.subscribe('orders.**', () => {});

  assert.equal(bus.publish('orders.created', { id: 1 }), 1);
  const dead = await closedPortUrl();
  bus.setAdmissionWebhookUrl(dead);
  const errorsBefore = bus.getStats().webhookErrorMessages;
  assert.equal(bus.publish('orders.created', { id: 2 }), 0);
  assert.equal(bus.getStats().webhookErrorMessages, errorsBefore + 1);
});

test('fail-open: repoint keeps fail-open admission on a dead endpoint', async (t) => {
  const { proc, url } = await startWebhook(JSON.stringify({ allowed: true }));
  t.after(() => stopWebhook(proc));
  const bus = new EventBus({
    admissionWebhook: { url, timeoutMs: 500, failureMode: 'fail-open' },
  });
  t.after(() => bus.close());
  bus.subscribe('orders.**', () => {});

  assert.equal(bus.publish('orders.created', { id: 1 }), 1);
  const dead = await closedPortUrl();
  bus.setAdmissionWebhookUrl(dead);
  assert.equal(bus.publish('orders.created', { id: 2 }), 1);
  // A fail-open admit is not a denial: neither counter moves.
  assert.equal(bus.getStats().webhookDeniedMessages, 0);
  assert.equal(bus.getStats().webhookErrorMessages, 0);
});

test('consecutive repoints both take effect', async (t) => {
  const firstLog = freshLog();
  const secondLog = freshLog();
  const firstStub = await startWebhook(JSON.stringify({ allowed: true }), {
    headersLogPath: firstLog,
  });
  t.after(() => stopWebhook(firstStub.proc));
  const secondStub = await startWebhook(JSON.stringify({ allowed: false }), {
    headersLogPath: secondLog,
  });
  t.after(() => stopWebhook(secondStub.proc));
  const bus = new EventBus({ admissionWebhook: { url: firstStub.url } });
  t.after(() => bus.close());
  bus.subscribe('orders.**', () => {});

  assert.equal(bus.publish('orders.created', { id: 1 }), 1);
  bus.setAdmissionWebhookUrl(secondStub.url);
  assert.equal(bus.publish('orders.created', { id: 2 }), 0);
  bus.setAdmissionWebhookUrl(firstStub.url);
  assert.equal(bus.publish('orders.created', { id: 3 }), 1);
  assert.equal(readLog(firstLog).length, 2, 'the first endpoint must decide twice');
  assert.equal(readLog(secondLog).length, 1);
});

test('a closed bus refuses the repoint', async (t) => {
  const { proc, url } = await startWebhook(JSON.stringify({ allowed: true }));
  t.after(() => stopWebhook(proc));
  const bus = new EventBus({ admissionWebhook: { url } });

  await bus.close();
  const other = await startWebhook(JSON.stringify({ allowed: true }));
  t.after(() => stopWebhook(other.proc));
  assert.throws(() => bus.setAdmissionWebhookUrl(other.url), RangeError);
  assert.equal(audit(bus).length, 0);
});

test('setAdmissionWebhook still revises only the tuning after the URL moves', async (t) => {
  const firstLog = freshLog();
  const secondLog = freshLog();
  const firstStub = await startWebhook(JSON.stringify({ allowed: true }), {
    headersLogPath: firstLog,
  });
  t.after(() => stopWebhook(firstStub.proc));
  const secondStub = await startWebhook(JSON.stringify({ allowed: true }), {
    headersLogPath: secondLog,
  });
  t.after(() => stopWebhook(secondStub.proc));
  const bus = new EventBus({ admissionWebhook: { url: firstStub.url, timeoutMs: 1000 } });
  t.after(() => bus.close());
  bus.subscribe('orders.**', () => {});

  bus.setAdmissionWebhookUrl(secondStub.url);
  // The EB-308 tuning revision still applies to the live URL. The
  // revised timeout is deliberately generous (5000ms): the per-call
  // worker needs time to start up under parallel load, and a tight
  // timeout would deny the publish for lack of worker time, not for
  // lack of a policy answer.
  bus.setAdmissionWebhook({ timeoutMs: 5000 });
  assert.equal(bus.publish('orders.created', { id: 1 }), 1);
  assert.equal(readLog(secondLog).length, 1);
  assert.equal(readLog(firstLog).length, 0);
  const entries = bus.getSubscriptionAudit({ action: 'admission_webhook_revised' });
  assert.equal(entries.length, 1);
  assert.deepEqual(entries[0].detail, {
    timeoutMs: 5000,
    oldTimeoutMs: 1000,
    failureMode: 'fail-closed',
    oldFailureMode: 'fail-closed',
    headersChanged: false,
  });
  assert.equal(audit(bus).length, 1);
});
