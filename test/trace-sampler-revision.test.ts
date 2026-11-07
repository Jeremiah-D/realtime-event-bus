/**
 * Bus-level head sampler live replacement (EB-414).
 *
 * `setTraceSampler` swaps the custom head-sampling decision function
 * without restarting the bus. Only a bus that enabled `trace` can be
 * revised; the new sampler governs from the next admission-time
 * decision — a publish already sampled keeps its trace and is never
 * re-judged — and `null` clears back to the live `sampleRate`.
 * The sampler function itself is never written to the audit log: only
 * whether one is installed is recorded (`{ hasSampler, oldHasSampler
 * }`). Re-installing the live sampler is a no-op with no audit.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

const flush = () => new Promise((resolve) => setImmediate(resolve));

const allowOrders = ({ topic }: { topic: string }) => topic === 'orders';
const denyAll = () => false;
const allowAll = () => true;

test('setTraceSampler fails closed when trace is not enabled', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setTraceSampler(allowOrders), RangeError);
  assert.throws(() => bus.setTraceSampler(null), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_sampler_revised' }).length, 0);
});

test('setTraceSampler rejects an illegal actor', () => {
  const bus = new EventBus({ trace: true });
  assert.throws(() => bus.setTraceSampler(allowOrders, ''), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_sampler_revised' }).length, 0);
});

test('a closed bus refuses the swap', async () => {
  const bus = new EventBus({ trace: true });
  await bus.close();
  assert.throws(() => bus.setTraceSampler(allowOrders), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_sampler_revised' }).length, 0);
});

test('setTraceSampler rejects non-functions and leaves the live sampler untouched', async () => {
  const bus = new EventBus({ trace: { sampleRate: 0, sampler: allowOrders } });
  const bad: Array<unknown> = ['nope', 42, {}, [], undefined];
  for (const sampler of bad) {
    assert.throws(
      () => bus.setTraceSampler(sampler as never),
      RangeError,
      `expected RangeError for ${String(sampler)}`,
    );
  }
  // The failed calls changed nothing: the allowlist sampler still
  // decides — `orders` is traced, anything else is not.
  bus.publish('orders', 1);
  bus.publish('payments', 1);
  await flush();
  const spans = bus.getStats().traceSpans;
  assert.equal(spans.length, 3);
  assert.ok(spans.every((s) => s.attrs.topic === 'orders'));
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_sampler_revised' }).length, 0);
});

test('re-installing the live sampler is a no-op with no audit', () => {
  const bus = new EventBus({ trace: { sampleRate: 1, sampler: allowOrders } });
  bus.setTraceSampler(allowOrders);
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_sampler_revised' }).length, 0);
});

test('clearing a bus with no sampler is a no-op with no audit', () => {
  const bus = new EventBus({ trace: { sampleRate: 1 } });
  bus.setTraceSampler(null);
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_sampler_revised' }).length, 0);
  assert.equal(bus.getStats().traceHasCustomSampler, false);
});

test('installing a sampler governs the next publish: topic allowlist overrides the rate', async () => {
  // sampleRate 0 would trace nothing — the sampler overrides it.
  const bus = new EventBus({ trace: { sampleRate: 0 } });
  bus.publish('orders', 1);
  await flush();
  assert.deepEqual(bus.getStats().traceSpans, []);
  bus.setTraceSampler(allowOrders);
  bus.publish('orders', 2);
  bus.publish('payments', 2);
  await flush();
  const spans = bus.getStats().traceSpans;
  assert.equal(spans.length, 3);
  assert.ok(spans.every((s) => s.attrs.topic === 'orders'));
  assert.ok(spans.every((s) => (s.attrs.seq as number) === 2));
});

test('clearing falls back to the live sampleRate immediately', async () => {
  const bus = new EventBus({ trace: { sampleRate: 1, sampler: denyAll } });
  bus.publish('orders', 1);
  await flush();
  assert.deepEqual(bus.getStats().traceSpans, []);
  bus.setTraceSampler(null);
  bus.publish('orders', 2);
  await flush();
  // Rate 1 traces everything again once the deny-all sampler is gone.
  assert.equal(bus.getStats().traceSpans.length, 3);
  assert.ok(bus.getStats().traceSpans.every((s) => (s.attrs.seq as number) === 2));

  const zero = new EventBus({ trace: { sampleRate: 0, sampler: allowAll } });
  zero.publish('orders', 1);
  await flush();
  assert.equal(zero.getStats().traceSpans.length, 3);
  zero.setTraceSampler(null);
  zero.publish('orders', 2);
  await flush();
  // Rate 0 traces nothing once the allow-all sampler is cleared.
  assert.equal(zero.getStats().traceSpans.length, 3);
});

test('replacing the sampler retires the old one: call counts hand over', async () => {
  let oldCalls = 0;
  let newCalls = 0;
  const oldSampler = () => {
    oldCalls += 1;
    return true;
  };
  const newSampler = () => {
    newCalls += 1;
    return true;
  };
  const bus = new EventBus({ trace: { sampleRate: 0, sampler: oldSampler } });
  bus.publish('orders', 1);
  await flush();
  assert.equal(oldCalls, 1);
  assert.equal(newCalls, 0);
  bus.setTraceSampler(newSampler);
  bus.publish('orders', 2);
  bus.publish('orders', 3);
  await flush();
  assert.equal(oldCalls, 1);
  assert.equal(newCalls, 2);
});

test('an already-sampled publish is never re-judged', async () => {
  const received: Array<unknown> = [];
  const bus = new EventBus({ trace: { sampleRate: 0, sampler: allowAll } });
  bus.subscribe('orders', (msg) => received.push(msg.payload));
  bus.publish('orders', 1);
  // Swap to a deny-all sampler before the drain runs: the publish was
  // already admitted (and sampled) under the old sampler, so its
  // deliver span must still arrive under the original trace.
  bus.setTraceSampler(denyAll);
  await flush();
  assert.deepEqual(received, [1]);
  const spans = bus.getStats().traceSpans;
  const publishSpan = spans.find((s) => s.name === 'bus.publish');
  const deliverSpan = spans.find((s) => s.name === 'bus.deliver');
  assert.ok(publishSpan !== undefined);
  assert.ok(deliverSpan !== undefined);
  assert.equal(deliverSpan.traceId, publishSpan.traceId);
  // And a publish made after the swap is judged by the new sampler.
  bus.publish('orders', 2);
  await flush();
  assert.deepEqual(received, [1, 2]);
  assert.equal(bus.getStats().traceSpans.length, spans.length);
});

test('a throwing sampler propagates to publish and leaves the recorder intact', async () => {
  const bus = new EventBus({ trace: { sampleRate: 1 } });
  const boom = (): boolean => {
    throw new Error('sampler exploded');
  };
  bus.setTraceSampler(boom);
  assert.throws(() => bus.publish('orders', 1), /sampler exploded/);
  assert.deepEqual(bus.getStats().traceSpans, []);
  // The recorder is unharmed: swapping back to a legal sampler
  // restores sampling immediately.
  bus.setTraceSampler(allowAll);
  bus.publish('orders', 2);
  await flush();
  assert.equal(bus.getStats().traceSpans.length, 3);
});

test('the swap audits exact presence flags with the actor', () => {
  const bus = new EventBus({ trace: { sampleRate: 1 } });
  bus.setTraceSampler(allowOrders, 'ops');
  bus.setTraceSampler(null, 'ops');
  const audit = bus.getSubscriptionAudit({ action: 'trace_sampler_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[0].detail, { hasSampler: true, oldHasSampler: false });
  assert.equal(audit[0].actor, 'ops');
  assert.deepEqual(audit[1].detail, { hasSampler: false, oldHasSampler: true });
  assert.equal(audit[1].actor, 'ops');
  // The function itself is never recorded — only its presence.
  assert.ok(!JSON.stringify(audit).includes('allowOrders'));
});

test('getStats().traceHasCustomSampler follows the live value', () => {
  const bus = new EventBus({ trace: { sampleRate: 1 } });
  assert.equal(bus.getStats().traceHasCustomSampler, false);
  bus.setTraceSampler(allowOrders);
  assert.equal(bus.getStats().traceHasCustomSampler, true);
  bus.setTraceSampler(null);
  assert.equal(bus.getStats().traceHasCustomSampler, false);
  // A bus without tracing reports false, not undefined.
  const plain = new EventBus();
  assert.equal(plain.getStats().traceHasCustomSampler, false);
});

test('swapping the sampler leaves sampleRate, bufferSize and the ring untouched', async () => {
  const bus = new EventBus({ trace: { sampleRate: 1, bufferSize: 6, sampler: allowAll } });
  bus.publish('orders', 1);
  await flush();
  const before = bus.getStats().traceSpans.map((s) => [s.name, s.attrs.seq as number]);
  assert.equal(before.length, 3);
  bus.setTraceSampler(denyAll);
  // The buffered spans survive the swap, and the numeric tuning is
  // exactly what it was: the sampler swap moves no other field.
  assert.deepEqual(
    bus.getStats().traceSpans.map((s) => [s.name, s.attrs.seq as number]),
    before,
  );
  bus.setTrace({ sampleRate: 0 });
  // sampleRate 0 now governs — but only because setTrace moved it, and
  // the deny-all sampler still overrides it: nothing is traced.
  bus.publish('orders', 2);
  await flush();
  assert.deepEqual(
    bus.getStats().traceSpans.map((s) => [s.name, s.attrs.seq as number]),
    before,
  );
});
