# realtime-event-bus

> **Portfolio reconstruction** — a project built from scratch to demonstrate
> realtime systems engineering. It is not employer production code and does not
> contain any proprietary material.

## Inspiration

This project is inspired by a publicly stated engineering focus:

> "Exposed real-time market data, order-room negotiation and reputation over WebSocket + gRPC"

It rebuilds the *core primitives* behind that kind of system — topic fan-out,
backpressure, and reconnect control — as a small, zero-dependency TypeScript
library.

## What it implements

- **`src/bus.ts` — `EventBus`**: in-process publish/subscribe with wildcard
  topic patterns (`market.*` matches `market.btc`; `**` matches zero or more
  segments, so `market.**` matches `market`, `market.btc` and
  `market.btc.trades`; a bare `*` or `**` matches every topic). Each distinct
  pattern is compiled to a `RegExp` once and shared by all subscribers on
  that pattern (evicted when its last subscriber leaves), so the hot publish
  path never re-parses patterns. `publish()` fans out to all matching subscribers and returns how
  many accepted the message. Each subscriber gets an independent bounded
  queue; delivery is batched on a microtask so slow consumers exert real
  backpressure instead of blocking publishers. `publishBatch(messages)` fans
  out a whole batch and schedules a single flush for it — N messages cost one
  event-loop round instead of N — with identical matching, drop, and
  backpressure semantics, returning the total accepted deliveries.
  `subscribe()` accepts an
  `onBackpressure` callback that fires — once per excursion, re-armed after the
  queue drains — when a subscriber's queue reaches its 80% high-water mark,
  reporting queue size, capacity, and the cumulative `droppedCount`. A
  mirrored `onDrained` callback fires when the queue recedes below the mark,
  signalling the consumer may resume full speed. `subscribeReliable()` opts a
  subscriber into at-least-once delivery: the handler receives a `Delivery`
  envelope (`msg`, per-subscriber `seq`, `redeliveries` count) with `ack()` /
  `nack()` — `nack()` requeues immediately at the tail (FIFO order kept),
  and deliveries still outstanding after `ackTimeoutMs` (default 5 s) are
  requeued automatically. Redelivered messages keep their original TTL
  deadline. `unackedCount(subId)` and `getStats().unackedDeliveries` expose
  the outstanding-delivery backlog. Every message carries a per-topic `seq`
  (monotonic from 1, assigned at publish time); the bus watches each
  subscriber's deliveries and counts skipped numbers into `getStats()`
  `sequenceGaps` (per topic and global, with `lastSeq` per topic) —
  backpressure drops and TTL expiries surface as gaps, while at-least-once
  redeliveries never do.
- **`src/backpressure.ts` — `BoundedQueue<T>`**: fixed-capacity FIFO queue
  with `drop-oldest` / `drop-newest` policies, a drop counter, and a
  high-water-mark callback that fires once at 80% capacity and re-arms after
  the queue recedes. `push(item, priority = 0)` is priority-aware: under
  `drop-oldest` the oldest lowest-priority entry is shed first, so
  high-priority messages are dropped last; an incoming item that is strictly
  lower priority than the whole backlog is discarded instead of evicting it.
  `droppedByPriority` exposes per-priority drop counts. The high-water mark is
  a runtime-tunable ratio of capacity (`setHighWaterMarkRatio`, in (0, 1],
  also settable per subscriber via `EventBus.setHighWaterMarkRatio`); an
  `onDrained` callback fires once when the queue recedes below the mark after
  an excursion — or immediately if the mark is lowered below the current size
  mid-excursion.
- **`src/throttle.ts` — `TokenBucket` + adaptive publish-side throttling**:
  opt-in per subscriber via `subscribe(..., { throttle: true })`. When the
  subscriber's queue crosses the high-water mark, the bus rate-limits how many
  freshly published messages are fanned out to it (per-subscriber token
  bucket, one second of burst) instead of letting every publish churn through
  the queue's drop policy. Messages shed by the throttle never reach the
  queue — `throttledCount(subId)` counts them separately and they surface as
  sequence gaps like any other loss. When the queue drains below the mark,
  throttling disengages and full speed resumes; the drain rate measured over
  the excursion seeds the next engagement, so the throttle converges on the
  consumer's real speed. `onThrottled` fires once per engagement with the
  enforced rate and whether it was adapted from a measured drain rate;
  `getStats().throttledSubscribers` reports how many subscribers are
  currently throttled. Rate bounds are tunable
  (`{ minRatePerSec, maxRatePerSec, initialRatePerSec }`); invalid rates
  throw `RangeError` from `subscribe`. Disabled by default — existing
  backpressure behavior is unchanged unless a subscriber opts in.
- **`src/reconnect.ts` — `ReconnectController`**: connection state machine
  (`idle → connecting → connected → backoff → connecting …`) with an
  injectable `BackoffStrategy` (`ReconnectOptions.strategy`). The default is
  `ExponentialBackoff` (exponential backoff plus jitter with a configurable
  delay cap and injectable randomness for deterministic tests); pass a custom
  strategy for fixed, linear or decorrelated-jitter policies — it takes
  precedence over the legacy `baseDelayMs`/`maxDelayMs`/`jitterMs` options.
  Supports `onGiveUp` after `maxAttempts` failures. The connect function is
  injectable for testing.
- **`src/heartbeat.ts` — `HeartbeatMonitor`**: keep-alive for a
  `ReconnectController`. While the controller reports `connected`, it sends a
  ping every `intervalMs` through an injected `pingFn` (resolved on pong);
  a pong that does not arrive within `timeoutMs` counts as a missed
  heartbeat — `onTimeout(missed)` fires and the connection is handed back to
  the reconnect state machine via `controller.notifyDisconnected()`, driving
  the normal backoff/reconnect flow. Pings are skipped whenever the
  controller is not `connected`, an in-flight ping blocks the next tick so a
  hung peer cannot stack pings, and `onPong(latencyMs)` reports each
  round-trip.

## Run

Requires Node.js ≥ 22 (uses native TypeScript type stripping — no build step,
no dependencies).

```bash
npm test
```

## Test

`npm test` runs `node --test test/`. Coverage includes:

- `test/bus.test.ts` — exact, single-segment (`*`) and multi-level (`**`)
  wildcard delivery, non-matching topics, fan-out counts, unsubscribe,
  queue-full drop semantics, the per-subscriber `onBackpressure` callback
  (one-shot high-water-mark firing, re-arm after drain, dropped-count
  reporting), and `publishBatch` (single-flush batch fan-out, per-message
  ordering, both drop policies, empty-batch no-op).
- `test/backpressure.test.ts` — both drop policies, one-shot high-water-mark
  behavior, drain ordering, runtime watermark adjustment (`setHighWaterMarkRatio`,
  validation, mid-excursion lowering/raising semantics) and the `onDrained`
  recovery callback.
- `test/ack.test.ts` — at-least-once delivery: ack suppresses redelivery,
  nack redelivers immediately with a bumped `redeliveries` count, ack timeout
  auto-requeues, double/late settles are no-ops, unsubscribe cancels pending
  timers, redelivered messages keep their original TTL deadline, and
  `ackTimeoutMs` validation.
- `test/reconnect.test.ts` — exponentially increasing backoff delays within
  the jitter window, give-up after `maxAttempts`, attempt-counter reset on
  successful connect, the disconnect → backoff transition, plus custom
  strategy injection: a fixed-delay strategy is honored verbatim (taking
  precedence over the legacy numeric knobs), a linear strategy receives
  1-based attempt numbers, and the default exponential backoff grows/caps
  deterministically with injected randomness.
- `test/heartbeat.test.ts` — successful pongs report latency and keep the
  connection up, a hung peer times out exactly once and hands the connection
  back to reconnect (no stacked pings), pings stay silent while the
  controller is not connected, consecutive timeouts grow the miss counter
  until a pong resets it, and `stop()` halts the interval.
- `test/sequence.test.ts` — per-topic sequence numbers (starting at 1,
  independent per topic, consecutive across `publishBatch`), `lastSeq` in
  `getStats`, and gap detection: backpressure drops and TTL expiries count
  as gaps, redeliveries and late-joining subscribers don't, and gaps
  aggregate per topic across subscribers.

## Benchmark

`bench/fanout.bench.ts` measures the synchronous cost of one `publish()` —
matching the topic against the subscription patterns and enqueuing the
message into every matching subscriber's bounded queue — with **10,000**
subscribers on a single process (6,000 exact `market.btc.trades`, 2,500
`market.btc.*`, 1,500 `market.**`). Each sample drains the queues on a
microtask before the next publish, so the numbers are pure fan-out latency
with no backpressure drops.

```bash
node bench/fanout.bench.ts
```

Results measured 2026-10-06 on Node v24.20.0 (linux/x64, V8 13.6),
2-vCPU AMD EPYC 9D25 VM — 2,000 samples after 1,000 warmup publishes:

| metric | latency |
| ------ | ------- |
| mean   | 2.93 ms |
| p50    | 2.67 ms |
| p95    | 4.02 ms |
| p99    | 5.43 ms |
| min / max | 2.60 ms / 27.47 ms |

Throughput: ~342 publishes/s → **~3.42M deliveries/s** (in-process,
single thread). A second run gave similar numbers (p50 2.70 ms, p99
4.83 ms). Numbers depend on hardware — rerun the script to reproduce.
