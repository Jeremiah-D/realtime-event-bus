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
  path never re-parses patterns. A publish-side prefix inverted index
  (`Map<prefix, Set<subscriberId>>`) prunes the per-publish regex tests:
  each pattern files under its literal segment prefix (the segments before
  its first `*`/`**` — e.g. `market.btc.*` files under `market.btc`,
  patterns starting with a wildcard file under the empty key), and each
  publish looks up only the full topic, its progressively shorter segment
  prefixes, and the empty key. A pattern that matches is guaranteed to be
  among the candidates, so the compiled regex — still the final authority —
  confirms with identical matching semantics. The index is maintained
  incrementally (keys are evicted when their last subscriber leaves) and
  `getStats().indexSize` reports how many distinct prefix keys are live.
  `publish()` fans out to all matching subscribers and returns how
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
  `subscribeToGroup(groupId, pattern, handler, opts)` registers a
  Kafka-style consumer-group member: members of the same
  `(groupId, pattern)` compete — each matching message is delivered to
  exactly one member, picked round-robin in join order — while different
  groups on the same pattern each receive a copy. A slow member never
  blocks the group: assignment does not skip full queues, the member's own
  drop policy applies. Membership changes rebalance implicitly
  (`onRebalance` fires on affected members with the new roster). The bus
  tracks the per-group assignment watermark (`getGroupOffsets`: highest
  per-topic `seq` handed to any member) and consumer checkpoints
  (`commitOffset` / `getCommittedOffsets`); `getStats().consumerGroups`
  lists live groups.
- **`src/durablelog.ts` — `DurableTopicLog`**: opt-in append-only per-topic
  JSONL log (`new EventBus({ durableLogDir })`). Every published message is
  appended as one line (`{v, seq, topic, at, expiresAt?, payload}`, one file
  per URL-encoded topic) before fan-out, so a restarted process pointed at
  the same directory recovers per-topic sequence counters — numbering
  continues gap-free, never reused — and `getStats()` reflects the recovered
  history. A resubscribing consumer passes
  `subscribe(pattern, handler, { resumeFromSeq })` to pre-fill its queue with
  every logged message on matching topics with `seq` greater than
  `resumeFromSeq` (`0` = everything): per-topic order follows `seq`,
  cross-topic order follows publish time, original TTL deadlines are kept
  (already-expired replays are dropped as expired at drain, not resurrected),
  and replay goes through the normal queue backpressure policy. For
  consumer-group members replay is per member from each member's own offset
  (seed from that member's `commitOffset`). `resumeFromSeq` without
  `durableLogDir` throws instead of silently replaying nothing. Hot topics
  compact automatically (`durableLogMaxEntriesPerTopic`, default 10000,
  amortized rewrite at 2x). Corrupt log lines are skipped and counted
  (`getStats().durableLog.corruptLines`), never fatal; payloads
  `JSON.stringify` cannot represent are delivered live but skipped by the
  log. Durability is process-restart grade (synchronous appends, no
  per-message `fsync`) — a crash log for recovery, not a write-ahead log.
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
- **Subscriber health probing** (in `src/bus.ts`, opt-in via
  `subscribe(..., { healthProbe: true })`): the bus watches every delivery to
  the subscriber — a handler that throws, or one that overruns its
  `processingTimeoutMs` budget (measured with the bus clock), counts as one
  failure, and every success resets the consecutive-failure counter to 0.
  When the failures reach `maxConsecutiveFailures` (default 5), delivery
  auto-pauses: the backlog stays queued under the normal backpressure policy
  (preserved in FIFO order, never dropped — even a degradation that trips
  mid-drain requeues the messages it did not attempt) until the subscriber
  is resumed manually with `EventBus.resume(subId)` or automatically after
  the `autoResumeAfterMs` cooldown. `onDegraded` fires once per degradation
  with the failure count, the reason (`'error'` / `'timeout'`), and the
  preserved backlog size; `getStats().degradedSubscribers` reports how many
  subscribers are currently paused, and `subscriberHealth(subId)` reads the
  per-subscriber state. Invalid probe options throw `RangeError` from
  `subscribe`. Disabled by default — a throwing handler behaves exactly as
  before unless a subscriber opts in.
- **Per-topic publish rate limiting** (in `src/bus.ts`, via
  `setTopicRateLimit(pattern, messagesPerSec, { burst })`): a token bucket
  per concrete topic (refilled from the bus clock, so tests stay
  deterministic) caps how many messages per second a hot topic may publish;
  the first `burst` messages (default: one second of budget) publish
  instantly. A publish that finds the bucket empty is shed at the publish
  side — never fanned out, never written to the durable log, never queued —
  so subscriber backpressure policies do not churn on it; `publish` returns 0
  for a shed message. The shed consumes a sequence number and is counted in
  `TopicStats.rateLimitedMessages` (and the global total), so subscribers
  observe the loss as a sequence gap — the same visibility backpressure drops
  and TTL expiries get. Rule matching mirrors `setTopicTtl`: an exact-topic
  rule wins over patterns, the earliest-registered matching pattern wins, and
  re-setting a rule resets the topic's budget. `clearTopicRateLimit` removes
  a rule. Invalid configurations throw `RangeError`.
- **Publish-side schema validation** (in `src/bus.ts`, via
  `setTopicSchema(pattern, validator)`): an admission gate for malformed
  payloads. A publish whose payload makes the validator return `false` is
  rejected before admission — it consumes no sequence number (subscribers
  see no gap), is never written to the durable log, never reaches a queue,
  and does not burn rate-limit budget; `publish` returns 0 for it.
  Rejections are counted in `TopicStats.rejectedMessages` (and the global
  total). Rule matching mirrors `setTopicTtl`: an exact-topic rule wins over
  patterns, the earliest-registered matching pattern wins, and re-setting a
  rule replaces it. A validator that throws propagates the error to the
  publish caller — validation runs before any state is mutated for that
  message. `clearTopicSchema` removes a rule. Empty patterns and
  non-function validators throw `RangeError`.
- **Per-topic payload compression** (in `src/bus.ts`, via
  `setTopicCompression(pattern, { thresholdBytes, level })`): opt-in
  `node:zlib` deflate for large payloads — zero new dependencies. A publish
  whose payload serializes to more than `thresholdBytes` UTF-8 JSON bytes is
  deflate-compressed (default level 6, tunable 0–9); smaller payloads pass
  through untouched and never pay deflate CPU. Pipeline order is deliberate
  and documented in `fanOut`: schema validation always sees the *raw*
  payload (validators are written against the application shape), rate-limit
  sheds happen *before* compression (a shed message never pays deflate CPU
  and never touches compression metrics), and the compressed bytes are what
  get written to the durable log and fanned out to subscriber queues. Each
  compressed message carries a bus-internal envelope marker; subscribers
  inflate transparently just before delivery, so handlers always receive the
  original payload. Decompression is gated on a bus-side `WeakSet` of
  messages the publish path actually compressed — not on the marker alone —
  so a user payload that happens to share the envelope's shape is never
  mistaken for a compressed one. Compression is a no-op unless it shrinks
  the payload (an encoding larger than the input is never adopted), and
  payloads with no JSON encoding (`undefined`, functions, circular
  structures, BigInt) pass through untouched. It never changes message
  semantics: no sequence numbers consumed, no TTL deadlines moved, and ACK
  redelivery reuses the once-inflated message. `getStats()` exposes
  per-topic and global `compressedMessages`, `compressedBytesBefore`,
  `compressedBytesAfter`, `compressionRatio` (after/before, < 1 means the
  wire shrank), and `meanCompressionMs` (deflate time, bus clock). Rule
  matching mirrors `setTopicTtl`: an exact-topic rule wins over patterns,
  the earliest-registered matching pattern wins; `clearTopicCompression`
  removes a rule (already-queued messages keep the form they were published
  with). Invalid options throw `RangeError`. Disabled by default.
- **Subscriber output rate shaping** (in `src/bus.ts`, opt-in via
  `subscribe(..., { deliveryShaping: true })`): delivery-side pacing for a
  slow downstream consumer. A per-subscriber token bucket caps deliveries at
  `messagesPerSec` (default 100, `burst` defaulting to one second of budget);
  messages over budget stay queued — in FIFO order, never dropped — and are
  delivered on later flush rounds as the bucket refills, so delivery is
  smoothed to the downstream's pace instead of arriving in bursts. Unlike
  adaptive publish-side `throttle` (which sheds), shaping never drops: the
  backlog accumulates under the subscriber's normal backpressure policy, so
  size the queue for the expected backlog — a full queue still applies its
  drop policy. Shaping does not extend TTL: a message whose deadline passes
  while it waits is dropped as expired, and paced delivery produces no
  sequence gaps. A re-flush timer (unref'd, derived from the bus clock) keeps
  draining the backlog when no new publishes arrive; `getStats()` reports
  actively-shaped subscribers via `shapedSubscribers`. Invalid options throw
  `RangeError` from `subscribe`. Disabled by default.

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
- `test/groups.test.ts` — consumer groups: exactly-once-per-group delivery
  with round-robin order, cross-group broadcast, per-pattern independence,
  leave/join rebalance events, double-unsubscribe no-op, slow-member
  isolation (own drop policy), `getGroupOffsets` watermark,
  `commitOffset`/`getCommittedOffsets` round-trip and validation,
  `getStats().consumerGroups`, and `**` patterns.
- `test/durablelog.test.ts` — durable topic log: append/readSince round-trip,
  reopen recovery of topics/seqs/counts, corrupt-line tolerance, amortized
  compaction, option validation; bus integration: per-publish logging with
  seq and TTL deadline, seq continuity across restart, `resumeFromSeq`
  replay (missed-only, pattern-scoped, `0` = everything), loud failure when
  `durableLogDir` is missing, TTL-expired replays counted as expired, no
  phantom sequence gaps after replay, per-member group replay, unserializable
  payloads delivered live but not logged, and the on-disk JSONL line format.
- `test/backpressure.test.ts` — both drop policies, one-shot high-water-mark
  behavior, drain ordering, runtime watermark adjustment (`setHighWaterMarkRatio`,
  validation, mid-excursion lowering/raising semantics) and the `onDrained`
  recovery callback.
- `test/ack.test.ts` — at-least-once delivery: ack suppresses redelivery,
  nack redelivers immediately with a bumped `redeliveries` count, ack timeout
  auto-requeues, double/late settles are no-ops, unsubscribe cancels pending
  timers, redelivered messages keep their original TTL deadline, and
  `ackTimeoutMs` validation.
- `test/health.test.ts` — subscriber health probing: consecutive-error and
  processing-timeout thresholds auto-pause delivery, success resets the
  counter, a mid-drain degradation requeues unattempted messages in order,
  manual `resume` and cooldown auto-resume redeliver the preserved backlog
  FIFO, `onDegraded` fires once per excursion, stats/per-subscriber health
  exposure, option validation, failure isolation across subscribers, reliable
  (ack) coexistence, and auto-resume timer cleanup on unsubscribe.
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
- `test/index.test.ts` — the publish-side prefix index: an oracle-based
  equivalence test (delivery sets under the index match `compilePattern`
  exactly, across exact/`*`/`**`/leading/middle/trailing/consecutive-`**`
  patterns), incremental subscribe/unsubscribe maintenance (`indexSize`
  bookkeeping, key eviction, idempotent unsubscribe), and boundary cases
  (`a.**` matching the bare topic `a`, leading-wildcard patterns under the
  empty key, `subscribeReliable` routing through the same index).
- `test/compress.test.ts` — per-topic opt-in payload compression:
  below-threshold passthrough, above-threshold deflate with transparent
  subscriber-side inflation (one inflation per message across subscribers),
  no sequence-number consumption or reordering, `clearTopicCompression`
  restoring passthrough, rule matching (exact-topic beats patterns,
  earliest-registered pattern wins), never adopting an encoding that does
  not shrink the payload (level 0), unserializable payload passthrough,
  schema validation seeing the raw payload, rate-limit sheds happening
  before compression, durable-log round-trip (compressed bytes on disk,
  resume inflates), ACK redelivery of compressed messages, TTL deadline
  preservation, envelope-shaped user payloads never mistaken for
  compressed, and option validation.

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

## Prefix index

`bench/index.bench.ts` isolates the publish-side matching cost with 5,000
subscribers, measuring the synchronous `publish()` latency distribution
(mean / p50 / p95 / p99 over 2,000 samples after 1,000 warmups, µs):

- **Scenario A** — 5,000 subscribers with mutually distinct literal prefixes
  (`t0.*` … `t4999.*`), publishing `t42.price`, which matches exactly one
  of them. The index turns 5,000 regex tests into a few map lookups plus
  one confirmation.
- **Scenario B (degenerate control)** — 5,000 subscribers all on `**`, so
  every pattern files under the empty key and the index degrades to the
  full candidate set; this measures the index's own overhead.

Before/after protocol: run the bench on the new code, then
`git stash push -- src/bus.ts` to restore the old linear-scan `fanOut`,
run the bench again, and `git stash pop` to restore. The script uses only
the public API, so it runs unchanged against both versions.

Results measured 2026-10-07 on Node v24.20.0 (linux/x64, V8 13.6), same
2-vCPU AMD EPYC 9D25 VM as above:

| scenario | before (linear scan) | after (prefix index) |
| -------- | -------------------- | -------------------- |
| A · mean | 1028.07 µs | 104.31 µs |
| A · p50  | 993.95 µs  | 57.23 µs |
| A · p95  | 1207.69 µs | 155.39 µs |
| A · p99  | 1591.97 µs | 1166.46 µs |
| B · mean | 595.46 µs  | 2712.40 µs |
| B · p50  | 462.52 µs  | 1275.23 µs |
| B · p95  | 1132.57 µs | 7847.96 µs |
| B · p99  | 3484.05 µs | 24128.15 µs |

Scenario A is ~17× faster at p50 (993.95 µs → 57.23 µs): the index skips
~4,999 wasted regex tests per publish. Scenario B is ~2.8× slower at p50
(462.52 µs → 1275.23 µs) — the honest price of the degenerate case, where
building the candidate set buys nothing because every subscriber is a
candidate. Note the shared VM shows run-to-run tail jitter (occasional
multi-ms outliers); the p50 band was stable across repeated runs (~55–90 µs
for A-after, ~1150–1450 µs for B-after, ~990 µs for A-before, ~460 µs for
B-before). Rerun the script to reproduce.

```bash
node bench/index.bench.ts
```
