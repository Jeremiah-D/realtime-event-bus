/**
 * Bus-level admission webhook revision (EB-308).
 *
 * `setAdmissionWebhook` retunes the external admission webhook live —
 * the response timeout, the timeout/transport failure mode, and the
 * extra decision-request headers — without restarting the bus. Only a
 * bus that enabled `admissionWebhook` can be revised; illegal values
 * fail closed with the live tuning and the audit log untouched; an
 * identical (or empty) revision is a no-op with no audit. The `url`
 * is not revisable — it is the policy endpoint's identity.
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

test('setAdmissionWebhook fails closed when admissionWebhook is not enabled', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setAdmissionWebhook({ timeoutMs: 1000 }), RangeError);
  assert.throws(() => bus.setAdmissionWebhook({ failureMode: 'fail-open' }), RangeError);
  assert.throws(() => bus.setAdmissionWebhook({ headers: { 'x-a': '1' } }), RangeError);
  assert.equal(audit(bus).length, 0);
});

test('setAdmissionWebhook rejects illegal values and leaves the live tuning untouched', async (t) => {
  const { proc, url } = await startWebhook(JSON.stringify({ allowed: true }), {
    delayMs: 400,
  });
  t.after(() => stopWebhook(proc));
  // timeoutMs 100 against a 400ms-delayed stub: every publish times out.
  const bus = new EventBus({
    admissionWebhook: { url, timeoutMs: 100, failureMode: 'fail-closed' },
  });
  t.after(() => bus.close());

  const bad: Array<unknown> = [
    { timeoutMs: 0 },
    { timeoutMs: -1 },
    { timeoutMs: Number.NaN },
    { timeoutMs: Infinity },
    { timeoutMs: '500' },
    { failureMode: 'bogus' },
    { failureMode: '' },
    { failureMode: 'FAIL-CLOSED' },
    { headers: 'x-a: 1' },
    { headers: 42 },
    { headers: { 'x-a': 1 } },
    { headers: { 'x-a': undefined } },
    // NOTE: `{ headers: [] }` is NOT in this list — the construction
    // validator accepts an empty array as a header map (an object with
    // no entries), so the revision reuses that acceptance.
    'nope',
    42,
    [],
  ];
  for (const revision of bad) {
    assert.throws(
      () => bus.setAdmissionWebhook(revision as never),
      RangeError,
      `expected RangeError for ${JSON.stringify(revision)}`,
    );
  }
  // The failed calls changed nothing: the 100ms timeout and fail-closed
  // mode are still live — the delayed stub still times out and denies.
  assert.equal(bus.publish('orders.created', { id: 1 }), 0);
  assert.equal(bus.getStats().webhookErrorMessages, 1);
  assert.equal(audit(bus).length, 0);
});

test('setAdmissionWebhook with identical values or an empty revision is a no-op with no audit', async (t) => {
  const { proc, url } = await startWebhook(JSON.stringify({ allowed: true }));
  t.after(() => stopWebhook(proc));
  const bus = new EventBus({
    admissionWebhook: {
      url,
      timeoutMs: 1000,
      failureMode: 'fail-closed',
      headers: { 'x-a': '1', 'x-b': '2' },
    },
  });
  t.after(() => bus.close());

  bus.setAdmissionWebhook({ timeoutMs: 1000 });
  bus.setAdmissionWebhook({ failureMode: 'fail-closed' });
  bus.setAdmissionWebhook({ headers: { 'x-a': '1', 'x-b': '2' } });
  // A reordered copy of the same header set is the same tuning.
  bus.setAdmissionWebhook({ headers: { 'x-b': '2', 'x-a': '1' } });
  bus.setAdmissionWebhook({});
  bus.setAdmissionWebhook(null as never);
  assert.equal(audit(bus).length, 0);
});

test('partial revision moves only the provided field', async (t) => {
  const { proc, url } = await startWebhook(JSON.stringify({ allowed: true }));
  t.after(() => stopWebhook(proc));
  const bus = new EventBus({
    admissionWebhook: { url, timeoutMs: 1000, failureMode: 'fail-closed', headers: { 'x-a': '1' } },
  });
  t.after(() => bus.close());

  bus.setAdmissionWebhook({ timeoutMs: 250 });
  const entries = audit(bus);
  assert.equal(entries.length, 1);
  assert.deepEqual(entries[0].detail, {
    timeoutMs: 250,
    oldTimeoutMs: 1000,
    failureMode: 'fail-closed',
    oldFailureMode: 'fail-closed',
    headersChanged: false,
  });
});

test('timeout revision takes effect from the next decision', async (t) => {
  const { proc, url } = await startWebhook(JSON.stringify({ allowed: true }), {
    delayMs: 400,
  });
  t.after(() => stopWebhook(proc));
  const bus = new EventBus({
    admissionWebhook: { url, timeoutMs: 100, failureMode: 'fail-closed' },
  });
  t.after(() => bus.close());
  bus.subscribe('orders.**', () => {});

  // The stub answers after 400ms; the 100ms timeout denies.
  assert.equal(bus.publish('orders.created', { id: 1 }), 0);
  assert.equal(bus.getStats().webhookErrorMessages, 1);
  // A 2000ms timeout admits the very next publish.
  bus.setAdmissionWebhook({ timeoutMs: 2000 });
  assert.equal(bus.publish('orders.created', { id: 2 }), 1);
  assert.equal(bus.getStats().webhookErrorMessages, 1);
});

test('failureMode revision takes effect from the next failure', async (t) => {
  const url = await closedPortUrl();
  const bus = new EventBus({
    admissionWebhook: { url, timeoutMs: 500, failureMode: 'fail-closed' },
  });
  t.after(() => bus.close());
  bus.subscribe('orders.**', () => {});

  // Connection refused + fail-closed denies.
  assert.equal(bus.publish('orders.created', { id: 1 }), 0);
  assert.equal(bus.getStats().webhookErrorMessages, 1);
  // Flip to fail-open: the same refused connection now admits — and
  // the error counter only counts denials, so it stays at 1.
  bus.setAdmissionWebhook({ failureMode: 'fail-open' });
  assert.equal(bus.publish('orders.created', { id: 2 }), 1);
  assert.equal(bus.getStats().webhookDeniedMessages, 1);
  assert.equal(bus.getStats().webhookErrorMessages, 1);
});

test('headers revision is sent on the next decision request, values never audited', async (t) => {
  const dir = mkdtempSync(join(tmpdir(), 'webhook-headers-'));
  const logPath = join(dir, 'headers.jsonl');
  writeFileSync(logPath, '');
  const { proc, url } = await startWebhook(JSON.stringify({ allowed: true }), {
    headersLogPath: logPath,
  });
  t.after(() => stopWebhook(proc));
  const bus = new EventBus({
    admissionWebhook: { url, headers: { 'x-test-a': 'one' } },
  });
  t.after(() => bus.close());
  bus.subscribe('orders.**', () => {});

  assert.equal(bus.publish('orders.created', { id: 1 }), 1);
  bus.setAdmissionWebhook({
    headers: { 'x-test-a': 'two', authorization: 'Bearer tok-secret-123' },
  });
  assert.equal(bus.publish('orders.created', { id: 2 }), 1);

  const lines = readFileSync(logPath, 'utf8')
    .split('\n')
    .filter((l) => l.trim() !== '')
    .map((l) => JSON.parse(l) as Record<string, string>);
  assert.equal(lines.length, 2);
  assert.equal(lines[0]['x-test-a'], 'one');
  assert.equal(lines[0]['authorization'], undefined);
  assert.equal(lines[1]['x-test-a'], 'two');
  assert.equal(lines[1]['authorization'], 'Bearer tok-secret-123');
  // The audit records only that the header set moved — never the values.
  const entries = audit(bus);
  assert.equal(entries.length, 1);
  assert.deepEqual(entries[0].detail, {
    timeoutMs: 1000,
    oldTimeoutMs: 1000,
    failureMode: 'fail-closed',
    oldFailureMode: 'fail-closed',
    headersChanged: true,
  });
  assert.equal(JSON.stringify(entries[0].detail).includes('tok-secret-123'), false);
});

test('url is not revisable: the endpoint identity stays put', async (t) => {
  const { proc, url } = await startWebhook(JSON.stringify({ allowed: true }));
  t.after(() => stopWebhook(proc));
  const bus = new EventBus({ admissionWebhook: { url } });
  t.after(() => bus.close());
  bus.subscribe('orders.**', () => {});

  const closedUrl = await closedPortUrl();
  bus.setAdmissionWebhook({ url: closedUrl } as never);
  // Still admitted by the original stub — the url never moved — and the
  // url-only object carried no revisable field, so there is no audit.
  assert.equal(bus.publish('orders.created', { id: 1 }), 1);
  assert.equal(audit(bus).length, 0);
});
