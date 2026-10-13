import test from 'node:test';
import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { Buffer } from 'node:buffer';
import { mkdtempSync, readFileSync, readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type BridgeEnvelope, type BridgeTransport, type BusMessage } from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';
import {
  assertValidSigningKeys,
  canonicalWireBytes,
  isSigningKeyActive,
  signTopicMessage,
  signingBaseBytes,
  verifyTopicMessage,
  type StoredSigningKey,
} from '../src/signing.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

// NOTE: no ALL-CAPS secret constant names in test code — the runtime
// secret redaction rewrites them and breaks the syntax.
const keySecret1 = 'first-signing-secret';
const keySecret2 = 'second-signing-secret';

function storedKey(kid: string, secret: string, retiredAtMs?: number): StoredSigningKey {
  return {
    kid,
    secret: Buffer.from(secret, 'utf8'),
    ...(retiredAtMs === undefined ? {} : { retiredAtMs }),
  };
}

// ---------------------------------------------------------------------------
// signing.ts unit tests
// ---------------------------------------------------------------------------

test('assertValidSigningKeys rejects empty lists, bad kids, dup kids, empty secrets', () => {
  assert.throws(() => assertValidSigningKeys([], 'here'), RangeError);
  assert.throws(() => assertValidSigningKeys(undefined, 'here'), RangeError);
  assert.throws(
    () => assertValidSigningKeys([{ kid: '', secret: 'x' }], 'here'),
    RangeError,
  );
  assert.throws(
    () => assertValidSigningKeys([{ kid: 'bad kid!', secret: 'x' }], 'here'),
    RangeError,
  );
  assert.throws(
    () => assertValidSigningKeys([{ kid: 'k'.repeat(65), secret: 'x' }], 'here'),
    RangeError,
  );
  assert.throws(
    () =>
      assertValidSigningKeys(
        [
          { kid: 'k1', secret: 'x' },
          { kid: 'k1', secret: 'y' },
        ],
        'here',
      ),
    RangeError,
  );
  assert.throws(() => assertValidSigningKeys([{ kid: 'k1', secret: '' }], 'here'), RangeError);
  assert.throws(
    () => assertValidSigningKeys([{ kid: 'k1', secret: Buffer.alloc(0) }], 'here'),
    RangeError,
  );
  // Valid: 1-64 [A-Za-z0-9_-] kids, string or Buffer secrets.
  assertValidSigningKeys(
    [
      { kid: 'k1', secret: 'x' },
      { kid: 'k-2_3', secret: Buffer.from('y') },
    ],
    'here',
  );
});

test('sign/verify happy path over (topic, seq, wire bytes)', () => {
  const key = storedKey('k1', keySecret1);
  const wire = canonicalWireBytes({ id: 1 })!;
  const { kid, sig } = signTopicMessage(key, 'orders.created', 7, wire);
  assert.equal(kid, 'k1');
  assert.match(sig, /^[0-9a-f]{64}$/);
  assert.equal(verifyTopicMessage([key], 'orders.created', 7, wire, { kid, sig }, 1000, 0), true);
  // Independent recomputation matches: base = topic + NUL + seq + wire.
  const expected = createHmac('sha256', keySecret1)
    .update(signingBaseBytes('orders.created', 7, wire))
    .digest('hex');
  assert.equal(sig, expected);
  // Wrong topic / seq / wire all fail.
  assert.equal(verifyTopicMessage([key], 'orders.other', 7, wire, { kid, sig }, 1000, 0), false);
  assert.equal(verifyTopicMessage([key], 'orders.created', 8, wire, { kid, sig }, 1000, 0), false);
  assert.equal(
    verifyTopicMessage([key], 'orders.created', 7, canonicalWireBytes({ id: 2 })!, { kid, sig }, 1000, 0),
    false,
  );
  // Malformed or missing signatures never verify (never throw).
  assert.equal(verifyTopicMessage([key], 'orders.created', 7, wire, undefined, 1000, 0), false);
  assert.equal(
    verifyTopicMessage([key], 'orders.created', 7, wire, { kid, sig: 'not-hex' }, 1000, 0),
    false,
  );
  assert.equal(
    verifyTopicMessage([key], 'orders.created', 7, wire, { kid, sig: 'ab'.repeat(32) }, 1000, 0),
    false,
  );
});

test('kid hint verifies against that single key with no fallback', () => {
  const k1 = storedKey('k1', keySecret1);
  const k2 = storedKey('k2', keySecret2);
  const wire = canonicalWireBytes('hi')!;
  const signed = signTopicMessage(k1, 't', 1, wire);
  // Correct key via hint: ok.
  assert.equal(verifyTopicMessage([k1, k2], 't', 1, wire, signed, 1000, 0), true);
  // Hinting the other key fails even though it is a valid configured key.
  assert.equal(
    verifyTopicMessage([k1, k2], 't', 1, wire, { kid: 'k2', sig: signed.sig }, 1000, 0),
    false,
  );
  // Unknown kid fails.
  assert.equal(
    verifyTopicMessage([k1, k2], 't', 1, wire, { kid: 'nope', sig: signed.sig }, 1000, 0),
    false,
  );
});

test('no hint tries active keys newest-first; retired keys honor the grace window', () => {
  const wire = canonicalWireBytes('hi')!;
  const k2 = storedKey('k2', keySecret2); // newest
  const k1 = storedKey('k1', keySecret1, 1000); // retired at t=1000
  const signedOld = signTopicMessage(k1, 't', 1, wire);
  // Within grace (inclusive boundary): the old key still verifies.
  assert.equal(verifyTopicMessage([k2, k1], 't', 1, wire, signedOld, 6000, 5000), true);
  // Past grace: the old key is dead, the new key never signed this.
  assert.equal(verifyTopicMessage([k2, k1], 't', 1, wire, signedOld, 6001, 5000), false);
  // A hint at the retired-beyond-grace key fails outright.
  assert.equal(
    verifyTopicMessage([k2, k1], 't', 1, wire, { kid: 'k1', sig: signedOld.sig }, 6001, 5000),
    false,
  );
  // Newest-first: a message signed by the newest key verifies without a hint.
  const signedNew = signTopicMessage(k2, 't', 2, wire);
  assert.equal(verifyTopicMessage([k2, k1], 't', 2, wire, signedNew, 6001, 5000), true);
});

test('isSigningKeyActive follows the WR-20 inclusive boundary', () => {
  assert.equal(isSigningKeyActive({ retiredAtMs: undefined }, 99999, 0), true);
  assert.equal(isSigningKeyActive({ retiredAtMs: 1000 }, 6000, 5000), true);
  assert.equal(isSigningKeyActive({ retiredAtMs: 1000 }, 6001, 5000), false);
  assert.equal(isSigningKeyActive({ retiredAtMs: 1000 }, 1000, 0), true);
});

// ---------------------------------------------------------------------------
// bus integration: setTopicSigningKey / publish-side signing
// ---------------------------------------------------------------------------

test('publish signs when a rule matches; envelope carries {kid, sig}', async () => {
  const bus = new EventBus();
  bus.setTopicSigningKey('orders.*', { keys: [{ kid: 'k1', secret: keySecret1 }] });
  const received: BusMessage[] = [];
  bus.subscribe('orders.created', (msg) => received.push(msg));
  bus.publish('orders.created', { id: 1 });
  await flush();
  assert.equal(received.length, 1);
  const sig = received[0].signature;
  assert.ok(sig !== undefined);
  assert.equal(sig.kid, 'k1');
  assert.match(sig.sig, /^[0-9a-f]{64}$/);
  // Unsigned topics carry no signature.
  bus.publish('other.topic', { id: 2 });
  await flush();
  assert.equal(received.length, 1);
});

test('setTopicSigningKey validates fail-fast with RangeError', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setTopicSigningKey('', { keys: [{ kid: 'k1', secret: 'x' }] }), RangeError);
  assert.throws(() => bus.setTopicSigningKey('t', { keys: [] }), RangeError);
  assert.throws(
    () =>
      bus.setTopicSigningKey('t', {
        keys: [
          { kid: 'k1', secret: 'x' },
          { kid: 'k1', secret: 'y' },
        ],
      }),
    RangeError,
  );
  assert.throws(() => bus.setTopicSigningKey('t', { keys: [{ kid: 'k!', secret: 'x' }] }), RangeError);
  assert.throws(() => bus.setTopicSigningKey('t', { keys: [{ kid: 'k1', secret: '' }] }), RangeError);
  assert.throws(
    () => bus.setTopicSigningKey('t', { keys: [{ kid: 'k1', secret: 'x' }], graceMs: -1 }),
    RangeError,
  );
  assert.throws(() => bus.subscribe('t', () => {}, { verifySignature: 'yes' as never }), TypeError);
});

test('clearTopicSigningKey removes the rule; publish stops signing', async () => {
  const bus = new EventBus();
  bus.setTopicSigningKey('t', { keys: [{ kid: 'k1', secret: keySecret1 }] });
  assert.equal(bus.clearTopicSigningKey('t'), true);
  assert.equal(bus.clearTopicSigningKey('t'), false);
  const received: BusMessage[] = [];
  bus.subscribe('t', (msg) => received.push(msg));
  bus.publish('t', 1);
  await flush();
  assert.equal(received.length, 1);
  assert.equal(received[0].signature, undefined);
  assert.ok(!('signing' in bus.getStats().topics[0]));
});

test('rule matching: exact topic wins over patterns; earliest pattern wins', async () => {
  const bus = new EventBus();
  bus.setTopicSigningKey('evt.*', { keys: [{ kid: 'pattern', secret: keySecret1 }] });
  bus.setTopicSigningKey('evt.created', { keys: [{ kid: 'exact', secret: keySecret2 }] });
  // Two patterns both matching: the earliest-registered wins.
  bus.setTopicSigningKey('evt.**', { keys: [{ kid: 'first-pattern', secret: 'p1' }] });
  bus.setTopicSigningKey('**.deep', { keys: [{ kid: 'second-pattern', secret: 'p2' }] });
  const kids: Record<string, string | undefined> = {};
  const seen: BusMessage[] = [];
  bus.subscribe('**', (msg) => seen.push(msg));
  bus.publish('evt.created', 1); // exact rule
  bus.publish('evt.other', 2); // evt.* pattern
  bus.publish('evt.a.deep', 3); // both patterns: earliest (evt.**) wins
  await flush();
  for (const msg of seen) kids[msg.topic] = msg.signature?.kid;
  assert.equal(kids['evt.created'], 'exact');
  assert.equal(kids['evt.other'], 'pattern');
  assert.equal(kids['evt.a.deep'], 'first-pattern');
});

// ---------------------------------------------------------------------------
// subscriber-side verification: verifySignature
// ---------------------------------------------------------------------------

test('verifySignature happy path: valid messages deliver normally', async () => {
  const bus = new EventBus();
  bus.setTopicSigningKey('t', { keys: [{ kid: 'k1', secret: keySecret1 }] });
  const verified: unknown[] = [];
  const plain: unknown[] = [];
  bus.subscribe('t', (msg) => verified.push(msg.payload), { verifySignature: true });
  bus.subscribe('t', (msg) => plain.push(msg.payload));
  bus.publish('t', { n: 1 });
  bus.publish('t', { n: 2 });
  await flush();
  assert.deepEqual(verified, [{ n: 1 }, { n: 2 }]);
  assert.deepEqual(plain, [{ n: 1 }, { n: 2 }]);
  const stats = bus.getStats();
  assert.equal(stats.signatureFailures, 0);
  assert.equal(stats.topics[0].signatureFailures, 0);
});

test('verifySignature is fail-closed: no rule means every message drops, counted and audited', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload), { verifySignature: true });
  bus.setTopicSigningKey('t', { keys: [{ kid: 'k1', secret: keySecret1 }] });
  bus.publish('t', 'ok'); // seq 1: delivered, establishes the gap baseline
  await flush();
  bus.clearTopicSigningKey('t');
  bus.publish('t', 'drop1'); // seq 2: no rule — dropped
  bus.publish('t', 'drop2'); // seq 3: no rule — dropped
  await flush();
  assert.deepEqual(received, ['ok']);
  const stats = bus.getStats();
  assert.equal(stats.signatureFailures, 2);
  assert.equal(stats.topics[0].signatureFailures, 2);
  const audit = bus.getSubscriptionAudit({ action: 'signature_verification_failed' });
  assert.equal(audit.length, 2);
  assert.equal(audit[0].topic, 't');
  assert.equal(audit[0].detail?.seq, 2);
  assert.equal(audit[1].detail?.seq, 3);
  // A failed verification is a genuine loss (unlike a filter skip): the
  // next valid delivery surfaces the missing seqs as a sequence gap.
  bus.setTopicSigningKey('t', { keys: [{ kid: 'k1', secret: keySecret1 }] });
  bus.publish('t', 'ok2'); // seq 4
  await flush();
  assert.deepEqual(received, ['ok', 'ok2']);
  assert.equal(bus.getStats().topics[0].sequenceGaps, 2);
});

test('durable replay verifies: rotated-out key within grace replays, past grace drops', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'signing-replay-'));
  const clock = controllableClock();
  const bus = new EventBus({ durableLogDir: dir, now: clock.now });
  bus.setTopicSigningKey('t', { keys: [{ kid: 'k1', secret: keySecret1 }], graceMs: 5000 });
  bus.publish('t', 'v1'); // signed with k1 at t=1000
  await flush();

  clock.nowMs = 2000;
  // Rotate: k2 newest, k1 retired as of now (t=2000).
  bus.setTopicSigningKey('t', {
    keys: [
      { kid: 'k2', secret: keySecret2 },
      { kid: 'k1', secret: keySecret1 },
    ],
    graceMs: 5000,
  });

  // Replay within the grace window: the k1-signed message verifies.
  const within: unknown[] = [];
  bus.subscribe('t', (msg) => within.push(msg.payload), {
    verifySignature: true,
    resumeFromSeq: 0,
  });
  await flush();
  await flush();
  assert.deepEqual(within, ['v1']);
  assert.equal(bus.getStats().signatureFailures, 0);

  // Past the grace window (2000 + 5000): the same record now fails.
  clock.nowMs = 8000;
  const past: unknown[] = [];
  bus.subscribe('t', (msg) => past.push(msg.payload), {
    verifySignature: true,
    resumeFromSeq: 0,
  });
  await flush();
  await flush();
  assert.deepEqual(past, []);
  const stats = bus.getStats();
  assert.equal(stats.signatureFailures, 1);
  assert.equal(stats.topics[0].signatureFailures, 1);
  const audit = bus.getSubscriptionAudit({ action: 'signature_verification_failed' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].detail?.kid, 'k1');
  assert.equal(audit[0].detail?.seq, 1);
});

test('unsigned replayed message on a signed topic fails closed', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'signing-unsigned-replay-'));
  const bus = new EventBus({ durableLogDir: dir });
  // Logged before any signing rule existed: no signature on the record.
  bus.publish('t', 'early');
  await flush();
  bus.setTopicSigningKey('t', { keys: [{ kid: 'k1', secret: keySecret1 }] });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload), {
    verifySignature: true,
    resumeFromSeq: 0,
  });
  await flush();
  await flush();
  assert.deepEqual(received, []);
  assert.equal(bus.getStats().signatureFailures, 1);
});

// ---------------------------------------------------------------------------
// compression: the signature covers the ciphertext
// ---------------------------------------------------------------------------

test('compressed envelope: signature covers the ciphertext bytes', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'signing-compress-'));
  const bus = new EventBus({ durableLogDir: dir });
  bus.setTopicSigningKey('t', { keys: [{ kid: 'k1', secret: keySecret1 }] });
  bus.setTopicCompression('t', { thresholdBytes: 64 });
  const big = { text: 'lorem ipsum dolor sit amet '.repeat(200) };
  const received: BusMessage[] = [];
  bus.subscribe('t', (msg) => received.push(msg), { verifySignature: true });
  bus.publish('t', big);
  await flush();
  assert.equal(received.length, 1);
  // Transparent inflation still works after verification.
  assert.deepEqual(received[0].payload, big);
  assert.equal(bus.getStats().topics[0].compressedMessages, 1);

  // The durable log holds the wire (compressed) form: recompute the HMAC
  // over the logged envelope bytes and confirm it equals the signature —
  // proving the signature covered the ciphertext, not the plaintext.
  const files = readdirSync(dir).filter((f) => f.endsWith('.log'));
  assert.equal(files.length, 1);
  const lines = readFileSync(join(dir, files[0]), 'utf8').trim().split('\n');
  const record = lines.map((l) => JSON.parse(l)).find((r) => r.seq >= 1);
  assert.ok(record !== undefined);
  assert.ok(record.signature !== undefined);
  const wireBytes = Buffer.from(JSON.stringify(record.payload), 'utf8');
  const expected = createHmac('sha256', keySecret1)
    .update(signingBaseBytes(record.topic, record.seq, wireBytes))
    .digest('hex');
  assert.equal(record.signature.sig, expected);
  assert.equal(record.signature.kid, 'k1');
  // And the plaintext alone does NOT verify — the signature is over the
  // ciphertext.
  const plainBytes = Buffer.from(JSON.stringify(big), 'utf8');
  const plainSig = createHmac('sha256', keySecret1)
    .update(signingBaseBytes(record.topic, record.seq, plainBytes))
    .digest('hex');
  assert.notEqual(record.signature.sig, plainSig);
});

// ---------------------------------------------------------------------------
// bridge: source signature verified against destination rules
// ---------------------------------------------------------------------------

function loopback(
  deliver: (env: BridgeEnvelope) => void,
  seen: BridgeEnvelope[] = [],
): BridgeTransport {
  return {
    name: 'loopback',
    publish(envelope: BridgeEnvelope) {
      seen.push(envelope);
      deliver(envelope);
    },
  };
}
const sink: BridgeTransport = { name: 'sink', publish() {} };

test('bridge carries the signature; destination verifies against its own rules', async () => {
  const seen: BridgeEnvelope[] = [];
  let busB!: EventBus;
  const busA = new EventBus({
    bridge: { transport: loopback((env) => busB.receiveFromBridge(env), seen) },
  });
  busB = new EventBus({ bridge: { transport: sink } });
  for (const bus of [busA, busB]) {
    bus.setTopicSigningKey('t', { keys: [{ kid: 'k1', secret: keySecret1 }] });
  }
  const received: BusMessage[] = [];
  busB.subscribe('t', (msg) => received.push(msg), { verifySignature: true });
  busA.publish('t', { n: 1 });
  await flush();
  await flush();
  // The outbound envelope carried the source signature context.
  assert.equal(seen.length, 1);
  assert.ok(seen[0].signature !== undefined);
  assert.equal(seen[0].signature.kid, 'k1');
  assert.match(seen[0].signature.sig, /^[0-9a-f]{64}$/);
  assert.equal(seen[0].signature.seq, 1);
  // The destination re-admitted and re-signed under its own rules (new
  // seq), and the verifying subscriber accepted it.
  assert.equal(received.length, 1);
  assert.deepEqual(received[0].payload, { n: 1 });
  assert.equal(received[0].seq, 1);
  assert.ok(received[0].signature !== undefined);
  assert.equal(busB.getStats().signatureFailures, 0);
});

test('bridge rejects a tampered envelope whose signature no longer verifies', async () => {
  const seen: BridgeEnvelope[] = [];
  let busB!: EventBus;
  const results: Array<{ accepted: boolean; reason?: string }> = [];
  const busA = new EventBus({
    bridge: {
      transport: loopback((env) => {
        // Tamper with the payload in flight: the signature no longer
        // covers what the envelope carries.
        const tampered: BridgeEnvelope = { ...env, payload: { n: 999 } };
        results.push(busB.receiveFromBridge(tampered));
      }, seen),
    },
  });
  busB = new EventBus({ bridge: { transport: sink } });
  for (const bus of [busA, busB]) {
    bus.setTopicSigningKey('t', { keys: [{ kid: 'k1', secret: keySecret1 }] });
  }
  const received: BusMessage[] = [];
  busB.subscribe('t', (msg) => received.push(msg), { verifySignature: true });
  busA.publish('t', { n: 1 });
  await flush();
  await flush();
  assert.deepEqual(results, [{ accepted: false, reason: 'signature-verification-failed' }]);
  assert.deepEqual(received, []);
  assert.equal(busB.getStats().bridge.inbound, 0);
});

test('bridge skips the source-signature check when the destination has no rule', async () => {
  let busB!: EventBus;
  busB = new EventBus({ bridge: { transport: sink } });
  // busB has no signing rule: the signed envelope is still admitted.
  const received: BusMessage[] = [];
  busB.subscribe('t', (msg) => received.push(msg));
  const result = { accepted: false as boolean, reason: undefined as string | undefined };
  const busA = new EventBus({
    bridge: {
      transport: loopback((env) => {
        const r = busB.receiveFromBridge(env);
        result.accepted = r.accepted;
        result.reason = r.reason;
      }),
    },
  });
  busA.setTopicSigningKey('t', { keys: [{ kid: 'k1', secret: keySecret1 }] });
  busA.publish('t', { n: 1 });
  await flush();
  await flush();
  assert.equal(result.accepted, true);
  assert.equal(result.reason, undefined);
  assert.equal(received.length, 1);
});

// ---------------------------------------------------------------------------
// stats / metrics / default-off
// ---------------------------------------------------------------------------

test('getStats exposes per-topic signing config without secrets', async () => {
  const bus = new EventBus();
  bus.setTopicSigningKey('orders.*', {
    keys: [
      { kid: 'k2', secret: keySecret2 },
      { kid: 'k1', secret: keySecret1 },
    ],
    graceMs: 5000,
  });
  bus.subscribe('orders.created', () => {});
  bus.publish('orders.created', 1);
  await flush();
  const topic = bus.getStats().topics[0];
  assert.deepEqual(topic.signing, { keyIds: ['k2', 'k1'], graceMs: 5000 });
  assert.equal(topic.signatureFailures, 0);
  // No secret material leaks through the stats snapshot.
  assert.ok(!JSON.stringify(topic.signing).includes(keySecret1));
  assert.ok(!JSON.stringify(topic.signing).includes(keySecret2));
});

test('disabled by default: no signing configured means byte-identical stats shape', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.publish('t', { a: 1 });
  await flush();
  const stats = bus.getStats();
  assert.equal(stats.signatureFailures, 0);
  const topic = stats.topics[0];
  assert.equal(topic.signatureFailures, 0);
  // No `signing` key at all when no rule matches the topic.
  assert.ok(!('signing' in topic));
  // An explicit verifySignature:false behaves exactly like the default.
  const bus2 = new EventBus();
  bus2.subscribe('t', () => {}, { verifySignature: false });
  bus2.publish('t', { a: 1 });
  await flush();
  assert.deepEqual(
    JSON.stringify(bus2.getStats().topics[0]).replace(/"rates":\{[^}]*\}/, '"rates":{}'),
    JSON.stringify(stats.topics[0]).replace(/"rates":\{[^}]*\}/, '"rates":{}'),
  );
});

test('metrics exposition carries the signature-failure series', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {}, { verifySignature: true });
  bus.publish('t', 1);
  await flush();
  const text = renderPrometheus(bus.getStats());
  assert.ok(text.includes('eventbus_signature_failures_total 1'));
  assert.ok(text.includes('eventbus_signature_failures_total{topic="t"} 1'));
});
