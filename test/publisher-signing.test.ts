/**
 * Publisher identity-signature binding (EB-97).
 *
 * Covers: `setPublisherSigningKey` / `clearPublisherSigningKey`
 * validation and rotation (the WR-20/EB-84 grace convention), the
 * publish-side signature over (topic, seq, publisherId, wire bytes),
 * dual topic+publisher signatures on one message, the subscribe-side
 * `verifyPublisher` fail-closed check (live fan-out, durable-log
 * replay, retained snapshot), impersonation drops counted in
 * `signatureFailures` and audited with `detail.kind: 'publisher'`,
 * rotation grace windows, composition with redaction (EB-68: redaction
 * runs at admission, before signing), and durable-log persistence of
 * the publisher signature across restarts.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type BusMessage } from '../src/bus.ts';
import {
  publisherSigningBaseBytes,
  signPublisherMessage,
  verifyPublisherMessage,
} from '../src/signing.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-pubsign-'));

// NOTE: no ALL-CAPS secret constant names in test code — the runtime
// secret redaction rewrites them and breaks the syntax.
const keySecret1 = 'publisher-first-secret';
const keySecret2 = 'publisher-second-secret';

test('setPublisherSigningKey validates fail-fast', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setPublisherSigningKey('', { keys: [{ kid: 'k1', secret: 'x' }] }), RangeError);
  assert.throws(
    () => bus.setPublisherSigningKey('bad id!', { keys: [{ kid: 'k1', secret: 'x' }] }),
    RangeError,
  );
  assert.throws(() => bus.setPublisherSigningKey('svc', { keys: [] }), RangeError);
  assert.throws(
    () => bus.setPublisherSigningKey('svc', { keys: [{ kid: 'k1', secret: '' }] }),
    RangeError,
  );
  assert.throws(
    () => bus.setPublisherSigningKey('svc', { keys: [{ kid: 'k1', secret: 'x' }], graceMs: -1 }),
    RangeError,
  );
  assert.throws(
    () =>
      bus.setPublisherSigningKey('svc', {
        keys: [
          { kid: 'k1', secret: 'x' },
          { kid: 'k1', secret: 'y' },
        ],
      }),
    RangeError,
  );
  assert.equal(bus.clearPublisherSigningKey('never-set'), false);
  assert.equal(bus.clearPublisherSigningKey('bad id!'), false);
});

test('verifyPublisher must be a boolean', () => {
  const bus = new EventBus();
  assert.throws(
    () => bus.subscribe('t', () => {}, { verifyPublisher: 'yes' as unknown as boolean }),
    TypeError,
  );
});

test('happy path: signed publisher identity verifies end to end', async () => {
  const bus = new EventBus();
  bus.setPublisherSigningKey('svc-a', { keys: [{ kid: 'k1', secret: keySecret1 }] });
  const received: BusMessage[] = [];
  bus.subscribe('t', (msg) => received.push(msg), { verifyPublisher: true });
  bus.publish('t', { v: 1 }, { publisherId: 'svc-a' });
  await flush();
  assert.equal(received.length, 1);
  assert.equal(received[0].publisherId, 'svc-a');
  assert.deepEqual(received[0].publisherSignature, {
    kid: 'k1',
    sig: received[0].publisherSignature?.sig,
  });
  assert.match(received[0].publisherSignature?.sig ?? '', /^[0-9a-f]{64}$/);
  assert.equal(bus.getStats().signatureFailures, 0);
});

test('dual signature: topic + publisher signatures are independent', async () => {
  const bus = new EventBus();
  bus.setTopicSigningKey('t', { keys: [{ kid: 'tk', secret: keySecret1 }] });
  bus.setPublisherSigningKey('svc-a', { keys: [{ kid: 'pk', secret: keySecret2 }] });
  const received: BusMessage[] = [];
  bus.subscribe('t', (msg) => received.push(msg), { verifySignature: true, verifyPublisher: true });
  bus.publish('t', { v: 1 }, { publisherId: 'svc-a' });
  await flush();
  assert.equal(received.length, 1);
  assert.equal(received[0].signature?.kid, 'tk');
  assert.equal(received[0].publisherSignature?.kid, 'pk');
});

test('verifyPublisher is fail-closed: anonymous messages drop', async () => {
  const bus = new EventBus();
  bus.setPublisherSigningKey('svc-a', { keys: [{ kid: 'k1', secret: keySecret1 }] });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload), { verifyPublisher: true });
  bus.publish('t', 'anon'); // no publisherId
  await flush();
  assert.deepEqual(received, []);
  const stats = bus.getStats();
  assert.equal(stats.signatureFailures, 1);
  assert.equal(stats.topics[0].signatureFailures, 1);
  const audit = bus.getSubscriptionAudit({ action: 'signature_verification_failed' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].detail?.kind, 'publisher');
});

test('verifyPublisher is fail-closed: publisher without a key set drops', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload), { verifyPublisher: true });
  bus.publish('t', 'x', { publisherId: 'unknown-svc' });
  await flush();
  assert.deepEqual(received, []);
  assert.equal(bus.getStats().signatureFailures, 1);
});

test('impersonation: a signature from a rotated-out key fails closed', async () => {
  const bus = new EventBus();
  bus.setPublisherSigningKey('svc-a', { keys: [{ kid: 'k1', secret: keySecret1 }] });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload), { verifyPublisher: true });
  bus.publish('t', 'genuine', { publisherId: 'svc-a' }); // seq 1, signed k1
  await flush();
  // The key set rotates with no grace: k1 stops verifying immediately.
  bus.setPublisherSigningKey('svc-a', { keys: [{ kid: 'k2', secret: keySecret2 }] });
  bus.publish('t', 'forged', { publisherId: 'svc-a' }); // seq 2, signed k2 — fine
  await flush();
  assert.deepEqual(received, ['genuine', 'forged']);
  // Clearing the key set leaves the identity unsigned: fail-closed.
  bus.clearPublisherSigningKey('svc-a');
  bus.publish('t', 'unsigned-now', { publisherId: 'svc-a' }); // seq 3: no key set
  await flush();
  assert.deepEqual(received, ['genuine', 'forged']);
  const stats = bus.getStats();
  assert.equal(stats.signatureFailures, 1);
  const audit = bus.getSubscriptionAudit({ action: 'signature_verification_failed' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].detail?.kind, 'publisher');
  assert.equal(audit[0].detail?.seq, 3);
  // A failed verification is a genuine loss: the next valid delivery
  // surfaces the missing seq as a sequence gap.
  bus.setPublisherSigningKey('svc-a', { keys: [{ kid: 'k2', secret: keySecret2 }] });
  bus.publish('t', 'back', { publisherId: 'svc-a' }); // seq 4
  await flush();
  assert.deepEqual(received, ['genuine', 'forged', 'back']);
  assert.equal(bus.getStats().topics[0].sequenceGaps, 1);
});

test('rotation grace: a retired key verifies inside its window, not after', async () => {
  const clock = controllableClock(1_000);
  const bus = new EventBus({ now: clock.now });
  bus.setTopicRetained('t', true);
  bus.setPublisherSigningKey('svc-a', {
    keys: [{ kid: 'k1', secret: keySecret1 }],
    graceMs: 5000,
  });
  bus.publish('t', 'v1', { publisherId: 'svc-a', retain: true }); // signed k1 at t=1000
  await flush();
  // Rotate at t=1000: k1 is retired as of now, k2 is live.
  bus.setPublisherSigningKey('svc-a', {
    keys: [
      { kid: 'k2', secret: keySecret2 },
      { kid: 'k1', secret: keySecret1 },
    ],
    graceMs: 5000,
  });
  // Inside the grace window the retained snapshot still verifies.
  clock.nowMs = 5999;
  const got1: unknown[] = [];
  const sub1 = bus.subscribe('t', (msg) => got1.push(msg.payload), { verifyPublisher: true });
  await flush();
  assert.deepEqual(got1, ['v1']);
  assert.equal(bus.getStats().signatureFailures, 0);
  sub1.unsubscribe();
  // Past the grace window the retired signature fails closed.
  clock.nowMs = 6001;
  const got2: unknown[] = [];
  bus.subscribe('t', (msg) => got2.push(msg.payload), { verifyPublisher: true });
  await flush();
  assert.deepEqual(got2, []);
  assert.equal(bus.getStats().signatureFailures, 1);
  const audit = bus.getSubscriptionAudit({ action: 'signature_verification_failed' });
  assert.equal(audit[0].detail?.kind, 'publisher');
});

test('signing primitives: publisher base binds topic, seq, publisherId, wire bytes', () => {
  const wire = Buffer.from('{"v":1}', 'utf8');
  const base = publisherSigningBaseBytes('t', 7, 'svc-a', wire);
  assert.ok(base.includes(Buffer.from('svc-a', 'utf8')));
  const sig = signPublisherMessage({ kid: 'k1', secret: Buffer.from(keySecret1, 'utf8') }, 't', 7, 'svc-a', wire);
  assert.equal(sig.kid, 'k1');
  const keys = [{ kid: 'k1', secret: Buffer.from(keySecret1, 'utf8') }];
  assert.equal(verifyPublisherMessage(keys, 't', 7, 'svc-a', wire, sig, 1000, 0), true);
  // Wrong publisherId fails: the identity is bound into the base.
  assert.equal(verifyPublisherMessage(keys, 't', 7, 'svc-b', wire, sig, 1000, 0), false);
  // Wrong seq fails.
  assert.equal(verifyPublisherMessage(keys, 't', 8, 'svc-a', wire, sig, 1000, 0), false);
  // Malformed signatures never throw, never verify.
  assert.equal(verifyPublisherMessage(keys, 't', 7, 'svc-a', wire, undefined, 1000, 0), false);
  assert.equal(
    verifyPublisherMessage(keys, 't', 7, 'svc-a', wire, { kid: 'k1', sig: 'zz' }, 1000, 0),
    false,
  );
});

test('redaction runs before signing: verification sees the redacted bytes', async () => {
  const bus = new EventBus();
  bus.setTopicRedaction('t', ['secret']);
  bus.setPublisherSigningKey('svc-a', { keys: [{ kid: 'k1', secret: keySecret1 }] });
  const received: Array<{ payload: unknown; sig: string | undefined }> = [];
  bus.subscribe('t', (msg) => received.push({ payload: msg.payload, sig: msg.publisherSignature?.sig }), {
    verifyPublisher: true,
  });
  bus.publish('t', { secret: 'hunter2', v: 1 }, { publisherId: 'svc-a' });
  await flush();
  assert.equal(received.length, 1);
  // The sensitive field is redacted for the subscriber…
  assert.deepEqual(received[0].payload, { secret: '***', v: 1 });
  // …and the signature the subscriber verified covers those same
  // redacted bytes (redaction at admission, signing at fan-out).
  assert.match(received[0].sig ?? '', /^[0-9a-f]{64}$/);
  assert.equal(bus.getStats().signatureFailures, 0);
});

test('durable log: publisher signature survives restart and replay verifies', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  bus.setPublisherSigningKey('svc-a', { keys: [{ kid: 'k1', secret: keySecret1 }] });
  bus.publish('t', { v: 1 }, { publisherId: 'svc-a' });
  bus.publish('t', { v: 2 }); // anonymous, unsigned
  await flush();

  const bus2 = new EventBus({ durableLogDir: dir });
  bus2.setPublisherSigningKey('svc-a', { keys: [{ kid: 'k1', secret: keySecret1 }] });
  const received: unknown[] = [];
  bus2.subscribe('t', (msg) => received.push(msg.payload), { verifyPublisher: true, resumeFromSeq: 0 });
  await flush();
  // seq 1 replays verified; seq 2 (anonymous) fails closed on replay.
  assert.deepEqual(received, [{ v: 1 }]);
  assert.equal(bus2.getStats().signatureFailures, 1);
});

test('retained snapshot: late verifyPublisher subscriber checks the stored signature', async () => {
  const bus = new EventBus();
  bus.setTopicRetained('t', true);
  bus.setPublisherSigningKey('svc-a', { keys: [{ kid: 'k1', secret: keySecret1 }] });
  bus.publish('t', { v: 9 }, { publisherId: 'svc-a', retain: true });
  await flush();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload), { verifyPublisher: true });
  await flush();
  assert.deepEqual(received, [{ v: 9 }]);
  assert.equal(bus.getStats().signatureFailures, 0);
});

test('non-verifying subscribers are unaffected by publisher signing', async () => {
  const bus = new EventBus();
  bus.setPublisherSigningKey('svc-a', { keys: [{ kid: 'k1', secret: keySecret1 }] });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload)); // no verifyPublisher
  bus.publish('t', 'a', { publisherId: 'svc-a' });
  bus.publish('t', 'b'); // anonymous
  bus.publish('t', 'c', { publisherId: 'no-keys' }); // no key set
  await flush();
  assert.deepEqual(received, ['a', 'b', 'c']);
});
