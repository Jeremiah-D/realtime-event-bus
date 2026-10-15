import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

describe('manual subscriber pause/resume (EB-113)', () => {
  test('pause holds delivery; the backlog accumulates and resume redelivers in order', async () => {
    const bus = new EventBus();
    const received: unknown[] = [];
    const sub = bus.subscribe('t', (m) => received.push(m.payload));
    assert.equal(bus.pauseSubscriber(sub.id), true);
    bus.publish('t', 'a');
    bus.publish('t', 'b');
    await flush();
    assert.deepEqual(received, []);
    assert.equal(bus.getStats().pausedSubscribers, 1);
    assert.equal(bus.resumeSubscriber(sub.id), true);
    await flush();
    assert.deepEqual(received, ['a', 'b']);
    assert.equal(bus.getStats().pausedSubscribers, 0);
  });

  test('pause is per-subscriber: other subscribers keep receiving', async () => {
    const bus = new EventBus();
    const a: unknown[] = [];
    const b: unknown[] = [];
    const subA = bus.subscribe('t', (m) => a.push(m.payload));
    bus.subscribe('t', (m) => b.push(m.payload));
    bus.pauseSubscriber(subA.id);
    bus.publish('t', 'x');
    await flush();
    assert.deepEqual(a, []);
    assert.deepEqual(b, ['x']);
    bus.resumeSubscriber(subA.id);
    await flush();
    assert.deepEqual(a, ['x']);
  });

  test('pause and resume are idempotent', () => {
    const bus = new EventBus();
    const sub = bus.subscribe('t', () => {});
    assert.equal(bus.pauseSubscriber(sub.id), true);
    assert.equal(bus.pauseSubscriber(sub.id), false);
    assert.equal(bus.getSubscriptionAudit({ action: 'subscriber_paused' }).length, 1);
    assert.equal(bus.resumeSubscriber(sub.id), true);
    assert.equal(bus.resumeSubscriber(sub.id), false);
    assert.equal(bus.getSubscriptionAudit({ action: 'subscriber_resumed' }).length, 1);
  });

  test('unknown subscriber ids throw', () => {
    const bus = new EventBus();
    assert.throws(() => bus.pauseSubscriber('nope'), /unknown subscriber/);
    assert.throws(() => bus.resumeSubscriber('nope'), /unknown subscriber/);
  });

  test('reason must be a non-empty string when provided; it lands in the audit', () => {
    const bus = new EventBus();
    const sub = bus.subscribe('t', () => {});
    assert.throws(() => bus.pauseSubscriber(sub.id, { reason: 42 as never }), TypeError);
    assert.throws(() => bus.pauseSubscriber(sub.id, { reason: '' }), RangeError);
    assert.equal(bus.pauseSubscriber(sub.id, { reason: 'deploy freeze' }), true);
    const [entry] = bus.getSubscriptionAudit({ action: 'subscriber_paused' });
    assert.equal(entry.subId, sub.id);
    assert.equal(entry.pattern, 't');
    assert.deepEqual(entry.detail, { reason: 'deploy freeze' });
    bus.resumeSubscriber(sub.id);
  });

  test('paused subscribers still apply their backpressure policy', async () => {
    const bus = new EventBus();
    const received: unknown[] = [];
    const sub = bus.subscribe('t', (m) => received.push(m.payload), {
      queueSize: 4,
      backpressure: 'drop-oldest',
    });
    bus.pauseSubscriber(sub.id);
    for (let i = 0; i < 6; i++) bus.publish('t', i);
    await flush();
    assert.deepEqual(received, []);
    bus.resumeSubscriber(sub.id);
    await flush();
    // drop-oldest kept the newest 4 of the 6 queued while paused.
    assert.deepEqual(received, [2, 3, 4, 5]);
  });

  test('manual pause outranks degraded recovery: resume() never clears a manual pause', async () => {
    const bus = new EventBus();
    let calls = 0;
    const sub = bus.subscribe(
      't',
      () => {
        calls += 1;
        throw new Error('boom');
      },
      { healthProbe: { maxConsecutiveFailures: 2 } },
    );
    bus.pauseSubscriber(sub.id);
    bus.publish('t', 'a');
    bus.publish('t', 'b');
    await flush();
    // Paused: the handler never ran, so no failures accumulated.
    assert.equal(calls, 0);
    // Degraded recovery on a non-degraded subscriber is a no-op — and it
    // must not clear the manual pause.
    assert.equal(bus.resume(sub.id), false);
    assert.equal(bus.getStats().pausedSubscribers, 1);
    assert.equal(bus.getStats().degradedSubscribers, 0);
    // Manual resume drains the preserved backlog; the throwing handler
    // then trips the health probe independently.
    bus.resumeSubscriber(sub.id);
    await flush();
    assert.equal(calls, 2);
    assert.equal(bus.getStats().pausedSubscribers, 0);
    assert.equal(bus.getStats().degradedSubscribers, 1);
    // The subscriber is degraded but no longer manually paused: resume()
    // clears the degraded flag only.
    assert.equal(bus.resume(sub.id), true);
    assert.equal(bus.getStats().degradedSubscribers, 0);
    assert.equal(bus.getStats().pausedSubscribers, 0);
  });

  test('a degraded subscriber can be manually paused; both gates hold delivery', async () => {
    const bus = new EventBus();
    let calls = 0;
    const sub = bus.subscribe(
      't',
      () => {
        calls += 1;
        throw new Error('boom');
      },
      { healthProbe: { maxConsecutiveFailures: 1 } },
    );
    bus.publish('t', 'a');
    await flush();
    assert.equal(calls, 1);
    assert.equal(bus.getStats().degradedSubscribers, 1);
    // Manual pause on top of the degraded pause.
    assert.equal(bus.pauseSubscriber(sub.id), true);
    // Degraded recovery clears the health flag but the manual pause holds.
    assert.equal(bus.resume(sub.id), true);
    assert.equal(bus.getStats().degradedSubscribers, 0);
    bus.publish('t', 'b');
    await flush();
    assert.equal(calls, 1); // manual pause still holding delivery
    assert.equal(bus.getStats().pausedSubscribers, 1);
    bus.resumeSubscriber(sub.id);
    await flush();
    assert.equal(calls, 2);
  });
});

describe('paused subscribers in stats, audit, and exposition', () => {
  test('getStats().pausedSubscribers tracks live pauses', () => {
    const bus = new EventBus();
    const a = bus.subscribe('t', () => {});
    const b = bus.subscribe('t', () => {});
    assert.equal(bus.getStats().pausedSubscribers, 0);
    bus.pauseSubscriber(a.id);
    assert.equal(bus.getStats().pausedSubscribers, 1);
    bus.pauseSubscriber(b.id);
    assert.equal(bus.getStats().pausedSubscribers, 2);
    bus.resumeSubscriber(a.id);
    assert.equal(bus.getStats().pausedSubscribers, 1);
  });

  test('pause/resume audit entries carry subId and pattern', () => {
    const bus = new EventBus();
    const sub = bus.subscribe('orders.*', () => {});
    bus.pauseSubscriber(sub.id);
    bus.resumeSubscriber(sub.id);
    const entries = bus.getSubscriptionAudit({ action: ['subscriber_paused', 'subscriber_resumed'] });
    assert.deepEqual(
      entries.map((e) => e.action),
      ['subscriber_paused', 'subscriber_resumed'],
    );
    for (const e of entries) {
      assert.equal(e.subId, sub.id);
      assert.equal(e.pattern, 'orders.*');
      assert.equal(e.actor, 'system');
    }
  });

  test('paused subscribers render as eventbus_paused_subscribers', () => {
    const bus = new EventBus();
    const sub = bus.subscribe('t', () => {});
    bus.pauseSubscriber(sub.id);
    assert.ok(renderPrometheus(bus.getStats()).includes('eventbus_paused_subscribers 1'));
    bus.resumeSubscriber(sub.id);
    assert.ok(renderPrometheus(bus.getStats()).includes('eventbus_paused_subscribers 0'));
  });
});
