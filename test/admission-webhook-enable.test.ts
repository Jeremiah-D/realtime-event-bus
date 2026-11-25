/**
 * Bus-level admission webhook live enable (EB-555).
 *
 * `setAdmissionWebhook` on a bus that never enabled `admissionWebhook`
 * installs the hook live — no restart, no lost subscriptions — with the
 * call resolved through the construction-time rule. The `url` is the one
 * knob with no default; `timeoutMs` / `failureMode` / `headers` fall
 * back to the construction defaults (1000ms / `'fail-closed'` / none).
 * The enable is audited like any revision, with `oldTimeoutMs` /
 * `oldFailureMode` `null`. `null` is a silent no-op; a closed bus
 * refuses the enable; the enable is live-only — a restart restores the
 * unconfigured state.
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

/**
 * Starts a webhook stub in a separate process; `responseJson` is the
 * JSON string the stub returns for every request. Returns the child
 * process and the URL. The stub prints "READY <port>" to stdout when
 * listening. When `headersLogPath` is given, every request's headers
 * are appended to that file as one JSON object per line.
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

const audit = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'admission_webhook_revised' });

const loggedHeaders = (logPath: string): Array<Record<string, string>> =>
  readFileSync(logPath, 'utf8')
    .split('\n')
    .filter((l) => l.trim() !== '')
    .map((l) => JSON.parse(l) as Record<string, string>);

test('enabling with only a url takes the construction defaults and the next decision queries the new endpoint', async (t) => {
  const { proc, url } = await startWebhook(JSON.stringify({ allowed: true }));
  t.after(() => stopWebhook(proc));
  const bus = new EventBus();
  t.after(() => bus.close());
  bus.subscribe('orders.**', () => {});

  // Before the enable there is no hook: publishes are admitted unchecked.
  assert.equal(bus.publish('orders.created', { id: 1 }), 1);
  bus.setAdmissionWebhook({ url });
  const entries = audit(bus);
  assert.equal(entries.length, 1);
  assert.deepEqual(entries[0].detail, {
    timeoutMs: 1000,
    oldTimeoutMs: null,
    failureMode: 'fail-closed',
    oldFailureMode: null,
    headersChanged: false,
  });
  // The next decision queries the new endpoint: the stub allows.
  assert.equal(bus.publish('orders.created', { id: 2 }), 1);
});

test('a deny stub sheds the next publish after the enable', async (t) => {
  const { proc, url } = await startWebhook(JSON.stringify({ allowed: false }));
  t.after(() => stopWebhook(proc));
  const bus = new EventBus();
  t.after(() => bus.close());
  bus.subscribe('orders.**', () => {});

  // The publish from before the enable was never checked.
  assert.equal(bus.publish('orders.created', { id: 1 }), 1);
  bus.setAdmissionWebhook({ url });
  assert.equal(bus.publish('orders.created', { id: 2 }), 0);
  assert.equal(bus.getStats().webhookDeniedMessages, 1);
  assert.equal(audit(bus).length, 1);
});

test('enabling with explicit values is exact, and the headers ride the next decision', async (t) => {
  const dir = mkdtempSync(join(tmpdir(), 'webhook-enable-headers-'));
  const logPath = join(dir, 'headers.jsonl');
  writeFileSync(logPath, '');
  const { proc, url } = await startWebhook(JSON.stringify({ allowed: true }), {
    headersLogPath: logPath,
  });
  t.after(() => stopWebhook(proc));
  const bus = new EventBus();
  t.after(() => bus.close());
  bus.subscribe('orders.**', () => {});

  bus.setAdmissionWebhook({
    url,
    timeoutMs: 250,
    failureMode: 'fail-open',
    headers: { 'x-test-a': 'one' },
  });
  const entries = audit(bus);
  assert.equal(entries.length, 1);
  assert.deepEqual(entries[0].detail, {
    timeoutMs: 250,
    oldTimeoutMs: null,
    failureMode: 'fail-open',
    oldFailureMode: null,
    headersChanged: true,
  });
  assert.equal(bus.publish('orders.created', { id: 1 }), 1);
  const lines = loggedHeaders(logPath);
  assert.equal(lines.length, 1);
  assert.equal(lines[0]['x-test-a'], 'one');
});

test('null on a never-enabled bus is a silent no-op and the bus stays enableable', async (t) => {
  const { proc, url } = await startWebhook(JSON.stringify({ allowed: true }));
  t.after(() => stopWebhook(proc));
  const bus = new EventBus();
  t.after(() => bus.close());
  bus.subscribe('orders.**', () => {});

  bus.setAdmissionWebhook(null as never);
  assert.equal(audit(bus).length, 0);
  assert.equal(bus.publish('orders.created', { id: 1 }), 1);
  // Still unconfigured: a later valid call enables it.
  bus.setAdmissionWebhook({ url });
  assert.equal(audit(bus).length, 1);
  assert.equal(bus.publish('orders.created', { id: 2 }), 1);
});

test('an enable without a url, and other illegal values, fail closed and leave the bus unconfigured', async (t) => {
  const { proc, url } = await startWebhook(JSON.stringify({ allowed: true }));
  t.after(() => stopWebhook(proc));
  const bus = new EventBus();
  t.after(() => bus.close());
  bus.subscribe('orders.**', () => {});

  const bad: Array<unknown> = [
    {},
    { timeoutMs: 500 },
    { failureMode: 'fail-open' },
    { headers: { 'x-a': '1' } },
    { url: '' },
    { url: 'not-a-url' },
    { url: 'ftp://127.0.0.1/admit' },
    { url: 42 },
    { url, timeoutMs: 0 },
    { url, timeoutMs: -1 },
    { url, timeoutMs: Number.NaN },
    { url, timeoutMs: Infinity },
    { url, timeoutMs: '500' },
    { url, failureMode: 'bogus' },
    { url, failureMode: '' },
    { url, headers: 'x-a: 1' },
    { url, headers: 42 },
    { url, headers: { 'x-a': 1 } },
    { url, headers: { 'x-a': undefined } },
    'nope',
    42,
    [],
  ];
  for (const enable of bad) {
    assert.throws(
      () => bus.setAdmissionWebhook(enable as never),
      RangeError,
      `expected RangeError for ${JSON.stringify(enable)}`,
    );
  }
  // The failed calls changed nothing: still unconfigured — publishes are
  // admitted without any webhook call — and the audit log is empty.
  assert.equal(bus.publish('orders.created', { id: 1 }), 1);
  assert.equal(audit(bus).length, 0);
  // A later valid call still enables it.
  bus.setAdmissionWebhook({ url });
  assert.equal(audit(bus).length, 1);
  assert.equal(bus.publish('orders.created', { id: 2 }), 1);
});

test('headers are copied on enable: later mutation of the caller object does not move the live tuning', async (t) => {
  const dir = mkdtempSync(join(tmpdir(), 'webhook-enable-copy-'));
  const logPath = join(dir, 'headers.jsonl');
  writeFileSync(logPath, '');
  const { proc, url } = await startWebhook(JSON.stringify({ allowed: true }), {
    headersLogPath: logPath,
  });
  t.after(() => stopWebhook(proc));
  const bus = new EventBus();
  t.after(() => bus.close());
  bus.subscribe('orders.**', () => {});

  const headers: Record<string, string> = { 'x-test-a': 'one' };
  bus.setAdmissionWebhook({ url, headers });
  headers['x-test-a'] = 'two';
  headers['x-test-b'] = 'three';
  assert.equal(bus.publish('orders.created', { id: 1 }), 1);
  const lines = loggedHeaders(logPath);
  assert.equal(lines.length, 1);
  assert.equal(lines[0]['x-test-a'], 'one');
  assert.equal(lines[0]['x-test-b'], undefined);
  assert.equal(audit(bus).length, 1);
});

test('a revision after the enable merges the live values, and a same-value revision is a no-op', async (t) => {
  const { proc, url } = await startWebhook(JSON.stringify({ allowed: true }));
  t.after(() => stopWebhook(proc));
  const bus = new EventBus();
  t.after(() => bus.close());

  bus.setAdmissionWebhook({ url, timeoutMs: 1000, failureMode: 'fail-closed' });
  bus.setAdmissionWebhook({ timeoutMs: 250 });
  const entries = audit(bus);
  assert.equal(entries.length, 2);
  assert.deepEqual(entries[1].detail, {
    timeoutMs: 250,
    oldTimeoutMs: 1000,
    failureMode: 'fail-closed',
    oldFailureMode: 'fail-closed',
    headersChanged: false,
  });
  bus.setAdmissionWebhook({ timeoutMs: 250 });
  bus.setAdmissionWebhook({});
  assert.equal(audit(bus).length, 2);
});

test('setAdmissionWebhookUrl repoints the endpoint after the enable', async (t) => {
  const allow = await startWebhook(JSON.stringify({ allowed: true }));
  t.after(() => stopWebhook(allow.proc));
  const deny = await startWebhook(JSON.stringify({ allowed: false }));
  t.after(() => stopWebhook(deny.proc));
  const bus = new EventBus();
  t.after(() => bus.close());
  bus.subscribe('orders.**', () => {});

  bus.setAdmissionWebhook({ url: allow.url });
  assert.equal(bus.publish('orders.created', { id: 1 }), 1);
  bus.setAdmissionWebhookUrl(deny.url);
  assert.equal(bus.publish('orders.created', { id: 2 }), 0);
  assert.equal(bus.getStats().webhookDeniedMessages, 1);
});

test('the enabled timeout governs from the next decision', async (t) => {
  const { proc, url } = await startWebhook(JSON.stringify({ allowed: true }), {
    delayMs: 400,
  });
  t.after(() => stopWebhook(proc));
  const bus = new EventBus();
  t.after(() => bus.close());
  bus.subscribe('orders.**', () => {});

  bus.setAdmissionWebhook({ url, timeoutMs: 100, failureMode: 'fail-closed' });
  assert.equal(bus.publish('orders.created', { id: 1 }), 0);
  assert.equal(bus.getStats().webhookErrorMessages, 1);
  bus.setAdmissionWebhook({ timeoutMs: 2000 });
  assert.equal(bus.publish('orders.created', { id: 2 }), 1);
  assert.equal(bus.getStats().webhookErrorMessages, 1);
});

test('the enabled failureMode governs the next transport failure', async (t) => {
  const url = await closedPortUrl();
  const bus = new EventBus();
  t.after(() => bus.close());
  bus.subscribe('orders.**', () => {});

  bus.setAdmissionWebhook({ url, timeoutMs: 500, failureMode: 'fail-open' });
  assert.equal(bus.publish('orders.created', { id: 1 }), 1);
  bus.setAdmissionWebhook({ failureMode: 'fail-closed' });
  assert.equal(bus.publish('orders.created', { id: 2 }), 0);
});

test('a closed bus refuses the enable', async () => {
  const bus = new EventBus();
  await bus.close();

  assert.throws(
    () => bus.setAdmissionWebhook({ url: 'http://127.0.0.1:1/admit' }),
    /bus is closed/,
  );
  assert.equal(audit(bus).length, 0);
});

test('the enable is live-only: a fresh bus is unconfigured', async (t) => {
  const { proc, url } = await startWebhook(JSON.stringify({ allowed: false }));
  t.after(() => stopWebhook(proc));
  const bus = new EventBus();
  t.after(() => bus.close());
  bus.subscribe('orders.**', () => {});
  bus.setAdmissionWebhook({ url });
  assert.equal(bus.publish('orders.created', { id: 1 }), 0);

  // A restart restores the unconfigured state: a fresh bus built without
  // the option admits publishes unchecked and carries no audit.
  const fresh = new EventBus();
  t.after(() => fresh.close());
  fresh.subscribe('orders.**', () => {});
  assert.equal(fresh.publish('orders.created', { id: 2 }), 1);
  assert.equal(audit(fresh).length, 0);
});
