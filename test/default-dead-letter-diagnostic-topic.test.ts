/**
 * Bus-level default dead-letter diagnostic topic live revision (EB-530).
 *
 * `setDefaultDeadLetterDiagnosticTopic` decides where the diagnostic
 * event of a new reliable subscriber that opts into a dead-letter
 * queue and omits `diagnosticTopic` is fanned out — `'ops.poison'`
 * to send every new subscriber's poison-message diagnostics to one
 * centralized alerting topic fleet-wide with one call, `null` to
 * clear the default back to no diagnostic. The priority chain is
 * explicit `diagnosticTopic` > bus default > no diagnostic; an
 * explicit topic always wins, while an explicit `null` at subscribe
 * time still throws `RangeError`, exactly as before. The default
 * never enables a dead-letter queue — a subscriber that does not
 * pass `deadLetter` still has none. A revision only affects
 * reliable subscribers created after it, and a revision identical
 * to the current default (re-setting the same topic, or clearing
 * an already-unset default) is a no-op with no audit. Audited as
 * `default_dead_letter_diagnostic_topic_revised`
 * (`{ previous, diagnosticTopic }`).
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const defaultAudits = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'default_dead_letter_diagnostic_topic_revised' });

/** Poison subscriber: always nacks, so the message dead-letters at once. */
function poisonBus(opts: {
  defaultDiagnosticTopic?: string | null;
  diagnosticTopic?: string | null;
  requireApproval?: boolean;
} = {}) {
  const bus = new EventBus();
  if (opts.defaultDiagnosticTopic !== undefined) {
    bus.setDefaultDeadLetterDiagnosticTopic(opts.defaultDiagnosticTopic);
  }
  const diagnostics: unknown[] = [];
  bus.subscribe('ops.poison', (msg) => diagnostics.push(msg.payload));
  const otherDiagnostics: unknown[] = [];
  bus.subscribe('ops.other', (msg) => otherDiagnostics.push(msg.payload));
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      d.nack();
    },
    {
      ackTimeoutMs: 10_000,
      deadLetter: {
        maxRedeliveries: 0,
        diagnosticTopic: opts.diagnosticTopic,
      },
    },
  );
  return { bus, sub, diagnostics, otherDiagnostics };
}

async function deadLetterOne(setup: ReturnType<typeof poisonBus>, payload: unknown) {
  setup.bus.publish('t', payload);
  await flush();
  await flush();
  await flush();
  const entries = setup.bus.getDeadLetterMessages(setup.sub.id);
  assert.equal(entries.length, 1);
  return entries[0];
}

test('EB-530: a bus default topic receives the diagnostic with exact metadata', async () => {
  const setup = poisonBus({ defaultDiagnosticTopic: 'ops.poison' });
  assert.equal(setup.bus.getDefaultDeadLetterDiagnosticTopic(), 'ops.poison');
  await deadLetterOne(setup, 'poison');
  assert.equal(setup.bus.getStats().deadLetteredMessages, 1);
  assert.equal(setup.bus.getStats().diagnosticEvents, 1);
  assert.equal(setup.diagnostics.length, 1);
  assert.equal(setup.otherDiagnostics.length, 0);
  const [diag] = setup.diagnostics as Array<Record<string, unknown>>;
  assert.equal(diag.payload, 'poison');
  assert.equal(diag.subscriberId, setup.sub.id);
  assert.equal(diag.pattern, 't');
  assert.equal(diag.seq, 1);
  assert.equal(diag.lastError, 'nack');
  assert.equal(diag.redeliveries, 0);
  assert.equal(diag.traceId, undefined);
  assert.ok(typeof diag.deadLetteredAt === 'number' && diag.deadLetteredAt > 0);
});

test('EB-530: an explicit diagnosticTopic always beats the bus default', async () => {
  const setup = poisonBus({ defaultDiagnosticTopic: 'ops.poison', diagnosticTopic: 'ops.other' });
  await deadLetterOne(setup, 'poison');
  assert.equal(setup.bus.getStats().diagnosticEvents, 1);
  assert.equal(setup.diagnostics.length, 0);
  assert.equal(setup.otherDiagnostics.length, 1);
  const [diag] = setup.otherDiagnostics as Array<Record<string, unknown>>;
  assert.equal(diag.payload, 'poison');
  assert.equal(diag.subscriberId, setup.sub.id);
});

test('EB-530: an explicit null at subscribe time still throws RangeError', () => {
  const bus = new EventBus();
  bus.setDefaultDeadLetterDiagnosticTopic('ops.poison');
  assert.throws(
    () =>
      bus.subscribeReliable('t', () => {}, {
        deadLetter: { diagnosticTopic: null as unknown as string },
      }),
    RangeError,
  );
  assert.equal(bus.getDefaultDeadLetterDiagnosticTopic(), 'ops.poison');
});

test('EB-530: the default never enables a dead-letter queue', async () => {
  const bus = new EventBus();
  bus.setDefaultDeadLetterDiagnosticTopic('ops.poison');
  const diagnostics: unknown[] = [];
  bus.subscribe('ops.poison', (msg) => diagnostics.push(msg.payload));
  let failures = 0;
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      failures += 1;
      if (failures <= 1) d.nack();
      else d.ack();
    },
    { ackTimeoutMs: 1000 },
  );
  bus.publish('t', 'hiccup');
  await flush();
  await flush();
  await flush();
  assert.equal(failures, 2, 'immediate redelivery only — no DLQ was created');
  assert.deepEqual(bus.getDeadLetterMessages(sub.id), []);
  assert.equal(diagnostics.length, 0);
  assert.equal(bus.getStats().diagnosticEvents, 0);
});

test('EB-530: clearing the default returns new subscribers to no diagnostic', async () => {
  const bus = new EventBus();
  bus.setDefaultDeadLetterDiagnosticTopic('ops.poison');
  bus.setDefaultDeadLetterDiagnosticTopic(null);
  assert.equal(bus.getDefaultDeadLetterDiagnosticTopic(), undefined);
  const diagnostics: unknown[] = [];
  bus.subscribe('ops.poison', (msg) => diagnostics.push(msg.payload));
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      d.nack();
    },
    { ackTimeoutMs: 10_000, deadLetter: { maxRedeliveries: 0 } },
  );
  bus.publish('t', 'poison');
  await flush();
  await flush();
  await flush();
  // The DLQ move still happens — only the diagnostic fan-out stops.
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  assert.equal(bus.getStats().diagnosticEvents, 0);
  assert.equal(diagnostics.length, 0);
  assert.deepEqual(defaultAudits(bus).map((a) => a.detail), [
    { previous: null, diagnosticTopic: 'ops.poison' },
    { previous: 'ops.poison', diagnosticTopic: null },
  ]);
});

test('EB-530: illegal values throw RangeError and leave the live default untouched', () => {
  const bus = new EventBus();
  bus.setDefaultDeadLetterDiagnosticTopic('ops.poison');
  const illegal: unknown[] = ['', 42, {}, [], true, 0];
  for (const value of illegal) {
    assert.throws(
      () => bus.setDefaultDeadLetterDiagnosticTopic(value as string),
      RangeError,
      `setDefaultDeadLetterDiagnosticTopic(${String(value)}) must throw`,
    );
    assert.equal(bus.getDefaultDeadLetterDiagnosticTopic(), 'ops.poison', 'live default untouched after a failed call');
  }
  assert.equal(defaultAudits(bus).length, 1, 'failed calls audit nothing');
  // An illegal actor also throws with the live default untouched.
  assert.throws(() => bus.setDefaultDeadLetterDiagnosticTopic('ops.other', ''), RangeError);
  assert.equal(bus.getDefaultDeadLetterDiagnosticTopic(), 'ops.poison');
});

test('EB-530: a same-value revision is a silent no-op with no audit', () => {
  const bus = new EventBus();
  bus.setDefaultDeadLetterDiagnosticTopic('ops.poison');
  assert.equal(defaultAudits(bus).length, 1);
  // Re-setting the same topic audits nothing.
  bus.setDefaultDeadLetterDiagnosticTopic('ops.poison');
  assert.equal(defaultAudits(bus).length, 1);
  assert.equal(bus.getDefaultDeadLetterDiagnosticTopic(), 'ops.poison');
  // Clearing an installed default audits; clearing again does not.
  bus.setDefaultDeadLetterDiagnosticTopic(null);
  assert.equal(defaultAudits(bus).length, 2);
  bus.setDefaultDeadLetterDiagnosticTopic(null);
  assert.equal(defaultAudits(bus).length, 2);
  // Clearing an already-unset default on a factory bus is a no-op.
  const fresh = new EventBus();
  fresh.setDefaultDeadLetterDiagnosticTopic(null);
  assert.equal(defaultAudits(fresh).length, 0);
});

test('EB-530: a revision does not move an already-subscribed subscriber diagnostic topic', async () => {
  const setup = poisonBus({ defaultDiagnosticTopic: 'ops.poison' });
  await deadLetterOne(setup, 'first');
  assert.equal(setup.diagnostics.length, 1);
  // Retarget the bus default: the existing subscriber keeps 'ops.poison'.
  setup.bus.setDefaultDeadLetterDiagnosticTopic('ops.other');
  setup.bus.publish('t', 'second');
  await flush();
  await flush();
  await flush();
  assert.equal(setup.bus.getDeadLetterMessages(setup.sub.id).length, 2);
  assert.equal(setup.diagnostics.length, 2, 'second diagnostic still on the baked topic');
  assert.equal(setup.otherDiagnostics.length, 0);
  // A subscriber created after the revision takes the new default.
  const later = setup.bus.subscribeReliable(
    't',
    (d) => {
      d.nack();
    },
    { ackTimeoutMs: 10_000, deadLetter: { maxRedeliveries: 0 } },
  );
  setup.bus.publish('t', 'third');
  await flush();
  await flush();
  await flush();
  assert.equal(setup.bus.getDeadLetterMessages(later.id).length, 1);
  // The original subscriber also dead-letters 'third' — still on its
  // baked topic — while the later subscriber's diagnostic takes the
  // new default.
  assert.equal(setup.diagnostics.length, 3);
  assert.equal(setup.otherDiagnostics.length, 1);
});

test('EB-530: a default-baked subscriber can still be revised per subscriber', async () => {
  const setup = poisonBus({ defaultDiagnosticTopic: 'ops.poison' });
  await deadLetterOne(setup, 'first');
  assert.equal(setup.diagnostics.length, 1);
  // Retarget this subscriber only: the next diagnostic follows it.
  setup.bus.setDeadLetterDiagnosticTopic(setup.sub.id, 'ops.other');
  setup.bus.publish('t', 'second');
  await flush();
  await flush();
  await flush();
  assert.equal(setup.bus.getDeadLetterMessages(setup.sub.id).length, 2);
  assert.equal(setup.diagnostics.length, 1);
  assert.equal(setup.otherDiagnostics.length, 1);
  // Clear it per subscriber: the DLQ move continues, fan-out stops.
  setup.bus.setDeadLetterDiagnosticTopic(setup.sub.id, null);
  setup.bus.publish('t', 'third');
  await flush();
  await flush();
  await flush();
  assert.equal(setup.bus.getDeadLetterMessages(setup.sub.id).length, 3);
  assert.equal(setup.diagnostics.length, 1);
  assert.equal(setup.otherDiagnostics.length, 1);
  // A later bus-default revision never re-applies to the already-revised
  // subscriber.
  setup.bus.setDefaultDeadLetterDiagnosticTopic('ops.third');
  setup.bus.publish('t', 'fourth');
  await flush();
  await flush();
  await flush();
  assert.equal(setup.bus.getDeadLetterMessages(setup.sub.id).length, 4);
  assert.equal(setup.diagnostics.length, 1);
  assert.equal(setup.otherDiagnostics.length, 1);
});

test('EB-530: audit previous/diagnosticTopic are exact and the getter follows the live value', () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultDeadLetterDiagnosticTopic(), undefined);
  bus.setDefaultDeadLetterDiagnosticTopic('ops.poison', 'ops');
  assert.equal(bus.getDefaultDeadLetterDiagnosticTopic(), 'ops.poison');
  bus.setDefaultDeadLetterDiagnosticTopic('ops.other');
  assert.equal(bus.getDefaultDeadLetterDiagnosticTopic(), 'ops.other');
  bus.setDefaultDeadLetterDiagnosticTopic(null);
  assert.equal(bus.getDefaultDeadLetterDiagnosticTopic(), undefined);
  const audits = defaultAudits(bus);
  assert.equal(audits.length, 3);
  assert.deepEqual(audits[0].detail, { previous: null, diagnosticTopic: 'ops.poison' });
  assert.equal(audits[0].actor, 'ops');
  assert.deepEqual(audits[1].detail, { previous: 'ops.poison', diagnosticTopic: 'ops.other' });
  assert.deepEqual(audits[2].detail, { previous: 'ops.other', diagnosticTopic: null });
});
