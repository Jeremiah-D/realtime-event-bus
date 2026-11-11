import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** Sleeps for wall-clock ms (auto-replay backoff uses real timers). */
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** Publishes one poison message and waits until it has dead-lettered. */
async function deadLetterOne(bus: InstanceType<typeof EventBus>, topic: string, payload: string): Promise<void> {
  bus.publish(topic, payload);
  await flush();
  await flush();
  await flush();
  await sleep(10);
}

test('setDeadLetterDiagnosticTopic rejects unknown, non-reliable, and DLQ-less subscribers', () => {
  const bus = new EventBus();
  const plain = bus.subscribe('plain', () => {});
  const noDlq = bus.subscribeReliable('nodlq', () => {}, { ackTimeoutMs: 1000 });
  const sub = bus.subscribeReliable(
    't',
    (d) => d.nack(),
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 1, diagnosticTopic: 'diag.a' } },
  );

  assert.throws(() => bus.setDeadLetterDiagnosticTopic('', 'diag.b'), RangeError);
  assert.throws(() => bus.setDeadLetterDiagnosticTopic('no-such-sub', 'diag.b'), RangeError);
  assert.throws(() => bus.setDeadLetterDiagnosticTopic(plain.id, 'diag.b'), RangeError);
  assert.throws(() => bus.setDeadLetterDiagnosticTopic(noDlq.id, 'diag.b'), RangeError);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'dead_letter_diagnostic_topic_revised' }).length,
    0,
    'no failed revision is audited',
  );
});

test('setDeadLetterDiagnosticTopic rejects illegal topic values and leaves the live topic untouched', async () => {
  const bus = new EventBus();
  const diagnostics: unknown[] = [];
  bus.subscribe('diag.a', (msg) => diagnostics.push(msg.payload));
  const sub = bus.subscribeReliable(
    't',
    (d) => d.nack(),
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 1, diagnosticTopic: 'diag.a' } },
  );

  for (const bad of ['', 42, {}, undefined]) {
    assert.throws(() => bus.setDeadLetterDiagnosticTopic(sub.id, bad as never), RangeError);
  }
  assert.equal(
    bus.getSubscriptionAudit({ action: 'dead_letter_diagnostic_topic_revised' }).length,
    0,
    'no failed revision is audited',
  );

  // The original topic is untouched: the next dead-lettering still fans
  // out to diag.a.
  await deadLetterOne(bus, 't', 'poison');
  assert.equal(bus.getStats().deadLetteredMessages, 1);
  assert.equal(diagnostics.length, 1);
  assert.equal((diagnostics[0] as Record<string, unknown>).payload, 'poison');
});

test('retargeting the diagnostic topic moves the next dead-lettering to the new topic', async () => {
  const bus = new EventBus();
  const oldDiagnostics: unknown[] = [];
  const newDiagnostics: unknown[] = [];
  bus.subscribe('diag.old', (msg) => oldDiagnostics.push(msg.payload));
  bus.subscribe('diag.new', (msg) => newDiagnostics.push(msg.payload));
  const sub = bus.subscribeReliable(
    't',
    (d) => d.nack(),
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 1, diagnosticTopic: 'diag.old' } },
  );

  await deadLetterOne(bus, 't', 'poison');
  assert.equal(oldDiagnostics.length, 1);
  assert.equal(newDiagnostics.length, 0);

  bus.setDeadLetterDiagnosticTopic(sub.id, 'diag.new');
  await deadLetterOne(bus, 't', 'poison2');
  assert.equal(bus.getStats().deadLetteredMessages, 2);
  // The old topic receives nothing further; the new topic gets the second
  // dead-lettering with exact poison-message metadata.
  assert.equal(oldDiagnostics.length, 1);
  assert.equal(newDiagnostics.length, 1);
  const [diag] = newDiagnostics as Array<Record<string, unknown>>;
  assert.equal(diag.payload, 'poison2');
  assert.equal(diag.subscriberId, sub.id);
  assert.equal(diag.pattern, 't');
  assert.equal(diag.seq, 2);
  assert.equal(diag.lastError, 'nack');
  assert.equal(diag.redeliveries, 1);
  assert.equal(diag.traceId, undefined);
  assert.ok(typeof diag.deadLetteredAt === 'number' && diag.deadLetteredAt > 0);
  assert.equal(bus.getStats().diagnosticEvents, 2);
});

test('a subscriber with no topic can opt in by setting one', async () => {
  const bus = new EventBus();
  const diagnostics: unknown[] = [];
  bus.subscribe('diag.a', (msg) => diagnostics.push(msg.payload));
  const sub = bus.subscribeReliable(
    't',
    (d) => d.nack(),
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 1 } },
  );

  await deadLetterOne(bus, 't', 'poison');
  assert.equal(bus.getStats().deadLetteredMessages, 1);
  assert.equal(diagnostics.length, 0);

  bus.setDeadLetterDiagnosticTopic(sub.id, 'diag.a');
  await deadLetterOne(bus, 't', 'poison2');
  assert.equal(bus.getStats().deadLetteredMessages, 2);
  assert.equal(diagnostics.length, 1);
  assert.equal((diagnostics[0] as Record<string, unknown>).payload, 'poison2');
});

test('clearing the diagnostic topic keeps the DLQ move and callbacks, stops fan-out', async () => {
  const bus = new EventBus();
  const diagnostics: unknown[] = [];
  const deadLettered: unknown[] = [];
  bus.subscribe('diag.a', (msg) => diagnostics.push(msg.payload));
  const sub = bus.subscribeReliable(
    't',
    (d) => d.nack(),
    {
      ackTimeoutMs: 1000,
      deadLetter: {
        maxRedeliveries: 1,
        diagnosticTopic: 'diag.a',
        onDeadLetter: (event) => deadLettered.push(event),
      },
    },
  );

  bus.setDeadLetterDiagnosticTopic(sub.id, null);
  await deadLetterOne(bus, 't', 'poison');
  assert.equal(bus.getStats().deadLetteredMessages, 1);
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  assert.equal(diagnostics.length, 0);
  assert.equal(bus.getStats().diagnosticEvents, 0);
  assert.equal(deadLettered.length, 1, 'onDeadLetter still fires after the clear');
});

test('setting the current topic (or clearing a topic-less subscriber) is a no-op with no audit', () => {
  const bus = new EventBus();
  const withTopic = bus.subscribeReliable(
    't',
    (d) => d.nack(),
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 1, diagnosticTopic: 'diag.a' } },
  );
  const withoutTopic = bus.subscribeReliable(
    'u',
    (d) => d.nack(),
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 1 } },
  );

  bus.setDeadLetterDiagnosticTopic(withTopic.id, 'diag.a');
  bus.setDeadLetterDiagnosticTopic(withoutTopic.id, null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'dead_letter_diagnostic_topic_revised' }).length,
    0,
    'no-op revisions are not audited',
  );
});

test('the revision is audited with the old and new topic', () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable(
    't',
    (d) => d.nack(),
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 1, diagnosticTopic: 'diag.a' } },
  );

  bus.setDeadLetterDiagnosticTopic(sub.id, 'diag.b');
  bus.setDeadLetterDiagnosticTopic(sub.id, null);
  const events = bus.getSubscriptionAudit({ action: 'dead_letter_diagnostic_topic_revised' });
  assert.equal(events.length, 2);
  assert.equal(events[0]?.action, 'dead_letter_diagnostic_topic_revised');
  assert.equal(events[0]?.actor, 'system');
  assert.equal(events[0]?.subId, sub.id);
  assert.equal(events[0]?.pattern, 't');
  assert.deepEqual(events[0]?.detail, { diagnosticTopic: 'diag.b', oldDiagnosticTopic: 'diag.a' });
  assert.deepEqual(events[1]?.detail, { diagnosticTopic: null, oldDiagnosticTopic: 'diag.b' });
});

test('entries already dead-lettered never re-emit a diagnostic under the new topic', async () => {
  const bus = new EventBus();
  const newDiagnostics: unknown[] = [];
  bus.subscribe('diag.new', (msg) => newDiagnostics.push(msg.payload));
  const sub = bus.subscribeReliable(
    't',
    (d) => d.nack(),
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 1, diagnosticTopic: 'diag.old' } },
  );

  await deadLetterOne(bus, 't', 'poison');
  assert.equal(bus.getStats().diagnosticEvents, 1);
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);

  bus.setDeadLetterDiagnosticTopic(sub.id, 'diag.new');
  await flush();
  await flush();
  await sleep(10);
  assert.equal(bus.getStats().diagnosticEvents, 1, 'no diagnostic is re-emitted for the old entry');
  assert.equal(newDiagnostics.length, 0);
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1, 'the DLQ entry is untouched');
});

test('retargeting leaves auto-replay and the approval gate untouched', async () => {
  const bus = new EventBus();
  let deliveries = 0;
  let poisoned = 0;
  const subAuto = bus.subscribeReliable(
    'a',
    (d) => {
      deliveries += 1;
      d.nack();
    },
    {
      ackTimeoutMs: 1000,
      deadLetter: {
        maxRedeliveries: 1,
        diagnosticTopic: 'diag.auto',
        autoReplay: { initialDelayMs: 20, maxDelayMs: 20, maxAutoReplayAttempts: 1 },
        onPoisoned: () => {
          poisoned += 1;
        },
      },
    },
  );
  const subGate = bus.subscribeReliable(
    'g',
    (d) => d.nack(),
    {
      ackTimeoutMs: 1000,
      deadLetter: {
        maxRedeliveries: 0,
        diagnosticTopic: 'diag.gate',
        requireApproval: true,
        maxPending: 5,
      },
    },
  );

  bus.setDeadLetterDiagnosticTopic(subAuto.id, 'diag.auto2');
  bus.setDeadLetterDiagnosticTopic(subGate.id, 'diag.gate2');

  // The approval gate still parks operator replays after the retarget.
  await deadLetterOne(bus, 'g', 'gated');
  const stats = bus.getDeadLetterStats(subGate.id);
  assert.equal(stats.requireApproval, true);
  assert.equal(stats.maxPending, 5);
  assert.equal(stats.entries, 1);
  assert.equal(bus.replayDeadLetter(subGate.id, 1), true, 'replay request is accepted');
  assert.equal(bus.getDeadLetterStats(subGate.id).pendingApprovals, 1, 'request is parked, not executed');

  // Auto-replay still arms and exhausts exactly as configured: initial
  // delivery + redelivery, one auto-replay + redelivery, then poisoned.
  // (The auto-replay lifts the first entry out of the DLQ, so the second
  // dead-lettering leaves exactly one — poisoned — entry behind.)
  await deadLetterOne(bus, 'a', 'replayed');
  await sleep(300);
  assert.equal(deliveries, 4);
  assert.equal(poisoned, 1);
  const autoEntries = bus.getDeadLetterMessages(subAuto.id);
  assert.equal(autoEntries.length, 1);
  assert.equal(autoEntries[0]?.poisoned, true);
  assert.equal(autoEntries[0]?.autoReplayAttempts, 1);
});

test('a diagnostic rejected at admission still leaves the DLQ move unaffected', async () => {
  const rejected: unknown[] = [];
  const bus = new EventBus({
    onAdmissionRejected: (event) => rejected.push(event),
  });
  bus.setAclRules([{ pattern: 'diag.**', publish: 'deny' }]);
  const sub = bus.subscribeReliable(
    't',
    (d) => d.nack(),
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 1, diagnosticTopic: 'diag.old' } },
  );

  bus.setDeadLetterDiagnosticTopic(sub.id, 'diag.new');
  await deadLetterOne(bus, 't', 'poison');
  assert.equal(bus.getStats().deadLetteredMessages, 1);
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  assert.equal(bus.getStats().diagnosticEvents, 0);
  assert.equal(bus.getStats().authzDenied, 1);
  assert.equal(rejected.length, 1);
  assert.equal((rejected[0] as Record<string, unknown>).reason, 'acl');
});
