import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

test('dead-lettering emits a diagnostic event with poison-message metadata', async () => {
  const bus = new EventBus();
  const diagnostics: unknown[] = [];
  bus.subscribe('diag.dlq', (msg) => diagnostics.push(msg.payload));
  const sub = bus.subscribeReliable(
    't',
    (d) => d.nack(), // always fail
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 1, diagnosticTopic: 'diag.dlq' } },
  );
  bus.publish('t', 'poison');
  await flush();
  await flush();
  await flush();
  // Initial delivery + 1 redelivery, then DLQ + diagnostic.
  assert.equal(bus.getStats().deadLetteredMessages, 1);
  assert.equal(diagnostics.length, 1);
  const [diag] = diagnostics as Array<Record<string, unknown>>;
  assert.equal(diag.payload, 'poison');
  assert.equal(diag.subscriberId, sub.id);
  assert.equal(diag.pattern, 't');
  assert.equal(diag.seq, 1);
  assert.equal(diag.lastError, 'nack');
  assert.equal(diag.redeliveries, 1);
  assert.equal(diag.traceId, undefined);
  assert.ok(typeof diag.deadLetteredAt === 'number' && diag.deadLetteredAt > 0);
  assert.equal(bus.getStats().diagnosticEvents, 1);
  // The diagnostic event is a normal publish: it consumed a topic seq.
  assert.equal(bus.getStats().totalPublished, 2);
});

test('a dead-lettered diagnostic message never emits a second diagnostic', async () => {
  const bus = new EventBus();
  // The diagnostic topic is itself reliably subscribed with a DLQ + the
  // same diagnostic topic — without the recursion cut this would amplify
  // forever: every dead-lettered diagnostic publishes another diagnostic.
  const sub = bus.subscribeReliable(
    'diag',
    (d) => d.nack(), // always fail, including on diagnostic events
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 1, diagnosticTopic: 'diag' } },
  );
  bus.publish('diag', 'poison');
  for (let i = 0; i < 8; i += 1) await flush();
  // The original message dead-letters (diagnostic #1 admitted), the
  // diagnostic message dead-letters too but emits nothing further.
  assert.equal(bus.getStats().diagnosticEvents, 1);
  const entries = bus.getDeadLetterMessages(sub.id);
  assert.equal(entries.length, 2);
  assert.equal(entries[0].payload, 'poison');
  // The second DLQ entry is the diagnostic event itself.
  assert.equal((entries[1].payload as Record<string, unknown>).payload, 'poison');
});

test('diagnostic goes through admission: an ACL-denied topic emits nothing', async () => {
  const rejected: unknown[] = [];
  const bus = new EventBus({
    onAdmissionRejected: (event) => rejected.push(event),
  });
  bus.setAclRules([{ pattern: 'diag.**', publish: 'deny' }]);
  bus.subscribeReliable(
    't',
    (d) => d.nack(),
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 1, diagnosticTopic: 'diag.dlq' } },
  );
  bus.publish('t', 'poison');
  await flush();
  await flush();
  await flush();
  // The DLQ move itself is unaffected by the failed diagnostic.
  assert.equal(bus.getStats().deadLetteredMessages, 1);
  assert.equal(bus.getStats().diagnosticEvents, 0);
  assert.equal(bus.getStats().authzDenied, 1);
  assert.equal(rejected.length, 1);
  assert.equal((rejected[0] as Record<string, unknown>).reason, 'acl');
});

test('diagnostic counts on admission even with no subscriber on the topic', async () => {
  const bus = new EventBus();
  bus.subscribeReliable(
    't',
    (d) => d.nack(),
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 1, diagnosticTopic: 'diag.nobody' } },
  );
  bus.publish('t', 'poison');
  await flush();
  await flush();
  await flush();
  assert.equal(bus.getStats().deadLetteredMessages, 1);
  assert.equal(bus.getStats().diagnosticEvents, 1);
});

test('no diagnosticTopic configured means no diagnostic events', async () => {
  const bus = new EventBus();
  bus.subscribeReliable(
    't',
    (d) => d.nack(),
    { ackTimeoutMs: 1000, deadLetter: true },
  );
  bus.publish('t', 'poison');
  await flush();
  await flush();
  await flush();
  assert.equal(bus.getStats().deadLetteredMessages, 1);
  assert.equal(bus.getStats().diagnosticEvents, 0);
});

test('diagnosticTopic is validated at subscribe time', () => {
  const bus = new EventBus();
  assert.throws(
    () =>
      bus.subscribeReliable('t', () => {}, {
        deadLetter: { diagnosticTopic: '' },
      }),
    RangeError,
  );
  assert.throws(
    () =>
      bus.subscribeReliable('t', () => {}, {
        deadLetter: { diagnosticTopic: 42 as unknown as string },
      }),
    RangeError,
  );
});

test('diagnostic event is visible in the Prometheus exposition', async () => {
  const { renderPrometheus } = await import('../src/metrics.ts');
  const bus = new EventBus();
  bus.subscribeReliable(
    't',
    (d) => d.nack(),
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 1, diagnosticTopic: 'diag.dlq' } },
  );
  bus.publish('t', 'poison');
  await flush();
  await flush();
  await flush();
  const text = renderPrometheus(bus.getStats());
  assert.ok(text.includes('eventbus_diagnostic_events_total 1'), `missing series in:\n${text}`);
});
