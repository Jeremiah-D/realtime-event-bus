/**
 * Bus-level global publish rate-limit revision (EB-301).
 *
 * `setGlobalPublishRateLimit` retunes the bus-wide token bucket live —
 * the sustained rate and the burst — without restarting the bus. Only a
 * bus that enabled `globalPublishRateLimit` can be revised; illegal
 * values fail closed with the live tuning and the audit log untouched;
 * an identical (or empty) revision is a no-op with no audit. A partial
 * revision keeps the fields it leaves out — in particular a rate-only
 * revision keeps the live burst instead of recomputing it from the new
 * rate's default. The stored token budget is kept across a retune: a
 * lowered burst clamps the banked tokens, a raised burst does not top
 * the bucket up, and the new rate applies from the next refill.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Injectable clock so bucket refills are deterministic. */
function makeClock(startMs = 0) {
  let nowMs = startMs;
  return {
    now: () => nowMs,
    advance: (ms: number) => {
      nowMs += ms;
    },
  };
}

const flush = () => new Promise((resolve) => setImmediate(resolve));

describe('global publish rate-limit revision (EB-301)', () => {
  it('setGlobalPublishRateLimit fails closed when the limit is not enabled', () => {
    const bus = new EventBus();
    assert.throws(
      () => bus.setGlobalPublishRateLimit({ messagesPerSecond: 10 }),
      RangeError,
    );
    assert.throws(() => bus.setGlobalPublishRateLimit({ burst: 5 }), RangeError);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'global_publish_rate_limit_revised' }).length,
      0,
    );
  });

  it('setGlobalPublishRateLimit rejects illegal values and leaves the live tuning untouched', () => {
    const clock = makeClock();
    const bus = new EventBus({
      now: clock.now,
      globalPublishRateLimit: { messagesPerSecond: 2, burst: 4 },
    });
    bus.subscribe('t', () => {});
    assert.equal(bus.publish('t', 'a'), 1); // 3 tokens banked
    const bad: Array<unknown> = [
      { messagesPerSecond: 0 },
      { messagesPerSecond: -5 },
      { messagesPerSecond: Number.NaN },
      { messagesPerSecond: Infinity },
      { burst: 0 },
      { burst: -1 },
      { burst: Number.NaN },
      { burst: Infinity },
      'nope',
      42,
      ['x'],
    ];
    for (const revision of bad) {
      assert.throws(
        () => bus.setGlobalPublishRateLimit(revision as never),
        RangeError,
        `expected RangeError for ${JSON.stringify(revision)}`,
      );
    }
    // The failed calls changed nothing: exactly the 3 banked tokens
    // remain, then the bucket sheds — burst still 4, rate still 2/s.
    assert.equal(bus.publish('t', 'b'), 1);
    assert.equal(bus.publish('t', 'c'), 1);
    assert.equal(bus.publish('t', 'd'), 1);
    assert.equal(bus.publish('t', 'e'), 0);
    clock.advance(500); // one token refills at 2/s
    assert.equal(bus.publish('t', 'f'), 1);
    assert.equal(bus.publish('t', 'g'), 0);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'global_publish_rate_limit_revised' }).length,
      0,
    );
  });

  it('setGlobalPublishRateLimit with identical values or an empty revision is a no-op with no audit', () => {
    const clock = makeClock();
    const bus = new EventBus({
      now: clock.now,
      globalPublishRateLimit: { messagesPerSecond: 2, burst: 4 },
    });
    bus.subscribe('t', () => {});
    bus.setGlobalPublishRateLimit({ messagesPerSecond: 2 });
    bus.setGlobalPublishRateLimit({ burst: 4 });
    bus.setGlobalPublishRateLimit({ messagesPerSecond: 2, burst: 4 });
    bus.setGlobalPublishRateLimit({});
    bus.setGlobalPublishRateLimit(null as never);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'global_publish_rate_limit_revised' }).length,
      0,
    );
    // The untouched tuning still governs: 4 instant publishes, then shed.
    assert.equal(bus.publish('t', 1), 1);
    assert.equal(bus.publish('t', 2), 1);
    assert.equal(bus.publish('t', 3), 1);
    assert.equal(bus.publish('t', 4), 1);
    assert.equal(bus.publish('t', 5), 0);
  });

  it('a rate-only revision keeps the live burst instead of recomputing its default', async () => {
    const clock = makeClock();
    const received: unknown[] = [];
    const bus = new EventBus({
      now: clock.now,
      globalPublishRateLimit: { messagesPerSecond: 2, burst: 10 },
    });
    bus.subscribe('t', (msg) => received.push(msg.payload));
    for (let i = 0; i < 3; i++) assert.equal(bus.publish('t', i), 1); // 7 tokens banked
    bus.setGlobalPublishRateLimit({ messagesPerSecond: 100 });
    // Had the burst been recomputed from the new rate's default
    // (max(1, ceil(100)) = 100), all 11 would pass; with the live burst
    // of 10 only the 7 banked tokens pass.
    let admitted = 0;
    for (let i = 0; i < 11; i++) admitted += bus.publish('t', `n${i}`);
    assert.equal(admitted, 7);
    await flush();
    assert.equal(received.length, 10);
    const audit = bus.getSubscriptionAudit({ action: 'global_publish_rate_limit_revised' });
    assert.equal(audit.length, 1);
    assert.deepEqual(audit[0].detail, {
      messagesPerSecond: 100,
      oldMessagesPerSecond: 2,
      burst: 10,
      oldBurst: 10,
    });
  });

  it('lowering the burst clamps the banked tokens; the new rate applies from the next refill', () => {
    const clock = makeClock();
    const bus = new EventBus({
      now: clock.now,
      globalPublishRateLimit: { messagesPerSecond: 1, burst: 5 },
    });
    bus.subscribe('t', () => {});
    assert.equal(bus.publish('t', 'a'), 1); // 4 tokens banked
    bus.setGlobalPublishRateLimit({ burst: 2 });
    // The 4 banked tokens are clamped to the new burst of 2.
    assert.equal(bus.publish('t', 'b'), 1);
    assert.equal(bus.publish('t', 'c'), 1);
    assert.equal(bus.publish('t', 'd'), 0);
    const audit = bus.getSubscriptionAudit({ action: 'global_publish_rate_limit_revised' });
    assert.equal(audit.length, 1);
    assert.deepEqual(audit[0].detail, {
      messagesPerSecond: 1,
      oldMessagesPerSecond: 1,
      burst: 2,
      oldBurst: 5,
    });
    // Raising the rate does not top the bucket up: at the frozen clock
    // the bucket is still empty, so a publish still sheds.
    bus.setGlobalPublishRateLimit({ messagesPerSecond: 10 });
    assert.equal(bus.publish('t', 'e'), 0);
    // The new rate applies from the next refill: 100ms at 10/s banks
    // one token.
    clock.advance(100);
    assert.equal(bus.publish('t', 'f'), 1);
    assert.equal(bus.publish('t', 'g'), 0);
  });

  it('raising the burst does not top the bucket up', () => {
    const clock = makeClock();
    const bus = new EventBus({
      now: clock.now,
      globalPublishRateLimit: { messagesPerSecond: 1, burst: 1 },
    });
    bus.subscribe('t', () => {});
    assert.equal(bus.publish('t', 'a'), 1); // bucket empty
    bus.setGlobalPublishRateLimit({ burst: 10 });
    // A raised burst banks nothing new: still empty at the frozen clock.
    assert.equal(bus.publish('t', 'b'), 0);
    clock.advance(1000); // one token refills at 1/s
    assert.equal(bus.publish('t', 'c'), 1);
    assert.equal(bus.publish('t', 'd'), 0);
  });

  it('the publishAtomic shadow reservation reads the live burst', async () => {
    const clock = makeClock();
    const bus = new EventBus({
      now: clock.now,
      globalPublishRateLimit: { messagesPerSecond: 1, burst: 5 },
    });
    bus.subscribe('t', () => {});
    bus.subscribe('u', () => {});
    const two = bus.publishAtomic([
      { topic: 't', payload: 1 },
      { topic: 'u', payload: 2 },
    ]);
    assert.equal(two.rejected, undefined); // 3 tokens banked
    bus.setGlobalPublishRateLimit({ burst: 2 });
    // The clamp is visible through the shadow reservation: a 3-entry
    // batch no longer fits the 2 banked tokens, a 2-entry batch does.
    const three = bus.publishAtomic([
      { topic: 't', payload: 3 },
      { topic: 'u', payload: 4 },
      { topic: 't', payload: 5 },
    ]);
    assert.notEqual(three.rejected, undefined);
    assert.equal(three.rejected?.reason, 'global-rate-limit');
    const ok = bus.publishAtomic([
      { topic: 't', payload: 6 },
      { topic: 'u', payload: 7 },
    ]);
    assert.equal(ok.rejected, undefined);
    await flush();
  });
});
