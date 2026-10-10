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
  the outstanding-delivery backlog. Opt-in `deadLetter` bounds redelivery:
  a message requeued more than `maxRedeliveries` times (default 5) moves to
  the subscriber's dead-letter queue instead of being retried forever —
  inspect it with `getDeadLetterMessages(subId)` (DLQ-local `seq`, topic,
  payload, `redeliveries` count, `deadLetteredAt`, `lastError` — the thrown
  error message, or `nack`/`ack-timeout` when nothing was thrown — the
  end-to-end `traceId` when the message was traced, original TTL deadline;
  `{ limit }` caps the result to the newest entries) and hand it back with
  `replayDeadLetter(subId, seq)` (fresh redelivery budget, original
  `seq`/deadline preserved — no false sequence gap). The
  DLQ is bounded (`maxEntries`, default 1000, oldest evicted first) and a
  synchronously throwing handler counts as an immediate redelivery attempt
  rather than crashing the flush. `getStats().deadLetteredMessages` counts
  every dead-lettering. Every message carries a per-topic `seq`
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
  lists live groups. Opt-in `partitions` on `subscribeToGroup` switches a
  competing set from per-message round-robin to Kafka-style partition
  assignment: the key space is divided into N logical partitions, each owned
  exclusively by one member. A keyed message's partition is
  `sha256("k\0" + key) mod N` (one key always lands on one partition, so
  per-key order is preserved within its exclusive consumer); a keyless
  message hashes `(topic, seq)` for a deterministic spread. Assignment is
  rendezvous (highest-random-weight) hashing over the member roster —
  deterministic from the roster alone, so joins/leaves only migrate the
  partitions whose winner actually changed. The bus tracks per-partition
  assignment watermarks (`getPartitionWatermarks`: highest per-topic `seq`
  per partition) and per-partition checkpoints
  (`commitOffset(group, topic, seq, { partition })`, journaled to the offset
  log and recovered across restarts; `getPartitionCommittedOffsets`). On
  every rebalance the new owner of each migrated partition automatically
  replays its uncommitted backlog — `(partitionCommitted ?? groupCommitted,
  watermark]` per topic — from the durable log (requires `durableLogDir`;
  without it the rebalance event still carries the watermarks for the
  operator). `onRebalance` events carry `partitionRebalance`
  (`assignment` + `migrated[]` with from/to/watermarks); a member that
  subscribes with `resumeFromSeq` replays only its own partitions past its
  resume point (its explicit window suppresses the automatic migration
  replay — no double delivery). The partition count is fixed by the group's
  first member: a later joiner that disagrees throws `RangeError` and its
  subscription is rolled back. **Consumer-group lag monitoring**
  (`src/grouplag.ts`): `getStats().groupLag` reports one row per
  (group, topic) — per (group, partition, topic) for partitioned groups —
  with `assignedSeq` (highest seq handed to the group), `committedSeq`
  (consumer checkpoint), and `lag = assigned - max(committed, lingerHeld)`.
  Live handoff-linger windows are excluded from lag (reported separately as
  `lingerHeldToSeq`), so a rebalance holding a leaver's backlog does not
  read as consumer lag. Alerting is opt-in via
  `EventBusOptions.groupLag`: `onGroupLag` fires once per threshold
  excursion (latch semantics: rearms after the lag falls back),
  `thresholdMessages` sets the default (100; `0` alerts on any lag),
  `thresholds` overrides per groupId, and
  `setGroupLagThreshold`/`clearGroupLagThreshold` adjust at runtime with an
  immediate re-evaluation — lag is re-checked on every assignment, commit,
  and rebalance, so a lag spike during a rebalance alerts promptly. Lag rows
  are exposed as `eventbus_group_lag_messages{group,topic}` and
  `eventbus_group_partition_lag_messages{group,topic,partition}` gauges in
  `src/metrics.ts`. `subscribe()` also accepts an opt-in content `filter`
  predicate: a message the filter rejects never enters that subscriber's
  queue — no backpressure budget consumed, no adaptive-throttle token
  burned — and the subscriber's per-topic baseline advances over it, so a
  deliberately skipped message never counts as a sequence gap (a message
  genuinely dropped while later ones are filtered can still stay invisible
  — the sampling limit of any gap detector). The filter sees the raw
  application payload and the concrete topic, applies to durable-log replay
  too, and for group members is evaluated on the assigned member only;
  `getStats()` reports `filteredMessages` per topic and globally.
- **`src/cluster.ts` — `ClusterHub` / `ClusterLink`**: cross-process
  cluster federation. Multiple Node processes form a cluster through a
  central TCP hub (`new ClusterHub({ port })`, TLS via `tls: { key, cert }`);
  each node joins with `bus.connectToHub({ url: 'tcp://host:port' })`,
  advertises its subscribed topic patterns, and receives the hub's route
  table. The hub is authoritative for routing: every membership or pattern
  change bumps a monotonic route version and rebroadcasts the table; nodes
  apply only newer versions (a hub restart is detected via a new hub epoch,
  which supersedes the version — the version-conflict merge). A publish
  whose topic matches subscribers on other members is additionally forwarded
  to the hub, which stamps hub-global per-topic sequence numbers (and
  hub-global per-key numbers for keyed messages) and forwards only to the
  members whose advertised patterns match — route-aware, never a blind
  broadcast; purely local traffic never touches the network. The sending
  node serves its own subscribers from its local publish stream, so a
  transport failure can never lose an admitted message locally. Sequence
  epochs (`BusMessage.epoch`: absent locally, `hub:<epoch>` for cluster
  traffic) keep gap detection and per-key ordering honest across the two
  numbering spaces: an epoch change re-establishes the baseline instead of
  counting a phantom gap. When the transport drops, the node degrades to
  local-only mode on its cached routes (`clusterStatus()` /
  `getStats().cluster` report `degraded`) and reconnects with backoff
  (see `src/reconnect.ts`; `reconnect: false` disables it). Hub-side
  heartbeat management sweeps members that stop heartbeating
  (`heartbeatIntervalMs` / `heartbeatTimeoutMs`). Payloads must be
  JSON-serializable for the wire (`Buffer` payloads travel as base64);
  compressed payloads are forwarded as envelopes and inflated on receipt
  (preset-dictionary bytes must be registered identically on every node).
  `ClusterLink.getStats()` exposes forwarded/received/error counters.
- **`src/bridge.ts` — `BridgeTransport` / `receiveFromBridge`**: cross-process
  fan-out bridge. Where the cluster link federates buses through a dedicated
  TCP hub, the bridge is the generic alternative: a pluggable transport
  interface (`new EventBus({ bridge: { transport } })`) that mirrors every
  admitted local publish to an external broker — Redis Streams, NATS, a queue —
  with the core staying zero-dependency (the adapter is implemented by the
  caller). Outbound, the bus hands the transport a `BridgeEnvelope` carrying
  the raw application payload (never a compressed envelope); a throwing
  transport or a rejected promise never reaches the publish path. Inbound,
  the transport hands envelopes to `bus.receiveFromBridge(envelope)`: they
  go through the same admission local publishes go through (alias
  resolution, ACL, schema validation, per-topic rate-limit budget, TTL at
  drain), get node-local sequence numbers, and are never mirrored back —
  the bridge stays loop-free. `key` (with its publish-order `keySeq`, so
  per-key ordering survives out-of-order transport delivery when each key
  has a single publishing node), `messageId`, the end-to-end `traceId`
  (32-hex, continued as a W3C `traceparent` on the peer), and the source
  TTL deadline (verbatim; otherwise the peer's own TTL rules apply) ride
  the envelope. Inbound envelopes queue in a bounded ingress buffer
  (`maxInboundQueue`, default 1024) drained on a microtask — a full buffer
  sheds the newest envelope and counts it. `getStats().bridge` reports
  `{ inbound, outbound, dropped }`, and the Prometheus exposition carries
  `eventbus_bridge_{outbound,inbound,dropped}_total`.
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
  (seed from that member's `commitOffset`). A brand-new consumer (cold start)
  or a disaster-recovery replay that has no seq checkpoint can instead pass
  `subscribe(pattern, handler, { resumeFromTime })`: the queue is pre-filled
  with every logged message published strictly after that wall-clock
  timestamp (bus clock, `EventBusOptions.now`), in publish-time order, with
  the same TTL/filter/backpressure semantics. `resumeFromTime` and
  `resumeFromSeq` are mutually exclusive (one replay cursor), and honest
  about history: only what the log still retains replays — messages already
  compacted away cannot be recovered by time. `resumeFromSeq` without
  `durableLogDir` throws instead of silently replaying nothing. Hot topics
  compact automatically (`durableLogMaxEntriesPerTopic`, default 10000,
  amortized rewrite at 2x). Corrupt log lines are skipped and counted
  (`getStats().durableLog.corruptLines`), never fatal; payloads
  `JSON.stringify` cannot represent are delivered live but skipped by the
  log. Opt-in Kafka-style keyed compaction (`durableLogKeyCompaction`):
  `publish(topic, payload, { key })` (also on batch/atomic/delayed
  publishes) stamps a compaction key on the log record, and the log then
  retains only the latest record per (topic, key) — older values for the
  same key are superseded: `readSince` never replays them (even before the
  file is rewritten), restart recovery rebuilds the key index from disk,
  and compaction rewrites the file keeping the latest record per key plus
  the newest `maxEntriesPerTopic` keyless messages (a full budget's worth of
  superseded records also triggers the rewrite early). Delayed-delivery
  schedule records are timer intents and are never compacted away; a keyed
  delayed message keeps its key across restart. Durability is process-restart grade (synchronous appends, no
  per-message `fsync`) — a crash log for recovery, not a write-ahead log.
  Alongside the per-topic files the log keeps one group-offset journal
  (`__group_offsets.jsonl`): every `commitOffset(groupId, topic, seq)` is
  appended as one line, and a bus opened over the same directory reseeds
  `getCommittedOffsets` from the highest committed seq per (group, topic)
  — checkpoints survive restarts, so a rejoining consumer that seeds
  `resumeFromSeq` from `getCommittedOffsets` picks up exactly where its
  predecessor committed. The journal compacts itself to the latest commit
  per (group, topic) past twice the entry budget (`getStats().durableLog.offsetEntries`
  reports its size). A leaving group member may open a graceful-handoff
  linger window (`subscribeToGroup(..., { handoffLingerMs })`): while the
  window is open, durable-log replay skips the group's assigned-but-
  uncommitted backlog for that member's in-flight work — a rejoining
  member's resume never delivers it twice — and after the window expires
  the backlog becomes replayable again (at-least-once). The `leave`
  rebalance event carries `lingerUntil` / `lingering` so operators can see
  the handoff; the operational recipe is `commitOffset` before leaving —
  the linger only covers what the leaver did not commit.
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
- **Unified admission-rejection hook** (in `src/bus.ts`, via
  `EventBusOptions.onAdmissionRejected`): every publish-side admission
  rejection — schema-validation rejections, per-topic rate-limit sheds,
  idempotency-duplicate suppressions, and `publishAtomic` batch rejections
  (surfaced with the failing entry's gate reason) — fires one
  `AdmissionRejectionEvent { topic, reason, payloadBytes, at }`. `reason`
  (`'acl' | 'schema' | 'rate-limit' | 'duplicate' | 'alias-retired'`) maps onto the stats counters
  (`rejectedMessages` / `rateLimitedMessages` / `duplicateMessages` /
  `aliasRetiredMessages`), so
  hook events reconcile exactly with `getStats()`. The hook fires after the
  counters move, carries the rejected payload's byte size (never the payload
  itself), and is error-isolated — a throwing hook is swallowed so it can
  never disturb the publish path. Unset by default (stats counting only —
  fully backward compatible).
- **Broker-level topic ACL** (in `src/bus.ts`, via `EventBusOptions.acl` /
  `setAclRules(rules)`): per-topic publish/subscribe permissions with
  allow/deny decisions on wildcard patterns (`{ pattern: 'admin.**',
  publish: 'deny' }`). Rules are evaluated in registration order — the first
  rule whose pattern matches, with an explicit decision for the action, wins;
  `defaultPolicy` (default `'allow'`) covers everything no rule decides, so a
  bus without ACL behaves exactly as before, while `'deny'` turns the rule
  set into a whitelist. Publish checks match the rule pattern against the
  concrete topic; subscribe checks use overlap semantics (`patternsOverlap`,
  exported): a deny rule covering any part of a subscription's scope denies
  the whole subscription, so a broad pattern cannot slip past a narrow deny.
  The ACL is the first admission gate everywhere (`publish`,
  `publishBatch`, `publishAtomic` shadow admission, `publishDelayed`
  fail-fast, and before the `publishIdempotent` dedup gate): an unauthorized
  publish returns 0, consumes no sequence number, never touches the durable
  log, and burns no rate-limit budget — counted in
  `TopicStats.rejectedMessages` (the "rejected" metric), surfaced on
  `onAdmissionRejected` with reason `'acl'`, and audited via
  `EventBusOptions.onAuthzDenied` as `authz_denied`. An unauthorized
  subscribe throws `AclDeniedError` before anything registers (also
  audited — the hook is its only audit channel, since nothing is returned).
  `setAclRules` replaces the rule list with immediate effect — no cached
  verdicts, no restart; `getAclRules` returns a copy. `getStats().authzDenied`
  counts every denied publish and subscribe — the security-relevant counter
  to alert on. Invalid rules throw `RangeError` at configuration time.
- **Topic aliases with zero-downtime migration** (in `src/bus.ts`, via
  `setTopicAlias(oldTopic, newTopic, { ttlMs })`): rename a topic without
  dropping a single consumer. While the alias is live, publishes to
  `oldTopic` resolve to `newTopic` *before every other admission gate*
  (ACL, schema, rate-limit, TTL, compression, the durable log and the
  per-topic sequence all key off the resolved topic) — producers still
  writing the old name are transparently redirected — and fan-out to
  `newTopic` additionally reaches subscribers of `oldTopic` (the dual-write
  window), so consumers still on the old name keep flowing. The mirror is
  one fan-out pass testing the resolved topic and the aliased old topics
  together: a subscriber matching via old and new patterns is still visited
  exactly once — the admitted message keeps a single `(topic, seq)` identity
  and is never double-delivered. Alias chains (`a → b → c`) forward through
  the whole live chain; a registration that would close a cycle (or a
  self-alias) throws `RangeError`. With `ttlMs` the alias retires that many
  milliseconds after registration: the old topic becomes read-only and
  publishes to it are rejected with admission reason `'alias-retired'` — no
  sequence number consumed, counted in `TopicStats.aliasRetiredMessages` /
  `BusStats.aliasRetiredMessages` and surfaced on `onAdmissionRejected`
  like every other admission rejection. Without `ttlMs` the alias never
  expires. Re-registering replaces the alias (and restarts its TTL);
  `clearTopicAlias(oldTopic)` removes it. `getStats().aliases` exposes the
  table (`{ oldTopic, newTopic, expiresAt, expired }`). Durable-log replay
  (`resumeFromSeq` / `resumeFromTime`) attributes records to their
  alias-resolved topic: a new consumer subscribing with the new topic name
  replays history logged under the old name (records keep their logged
  topic and per-topic seq identity — only matching widens), and old-topic
  subscribers replay the mirrored new-topic history. Alias-aware publish
  paths: `publish`, `publishBatch`, `publishIdempotent` (the dedup identity
  is `(resolved topic, messageId)`), `publishAtomic` (shadow admission
  resolves first; a retired old topic aborts the batch with reason
  `'alias-retired'`), and `publishDelayed` (resolved at schedule time).
- **Topic routes** (in `src/bus.ts`, via `setTopicRoute(src, dst,
  { predicate })`): forward every message admitted on `src` to `dst`
  automatically. The forward is a normal `dst` publish — it passes the
  destination's full admission pipeline (broker-level ACL, schema
  validation, rate-limit budget) and consumes the destination's sequence
  number and rate-limit budget, exactly as if a producer had published
  there directly. Identity is preserved across the hop: the forwarded
  message continues the source message's end-to-end trace (same `traceId`),
  keeps its TTL deadline verbatim (routing never resets it — when the
  source message had no deadline, the destination's TTL rules apply
  normally), and carries the application `key` / `messageId` along so
  per-key publish order and subscriber-side dedup keep working across the
  route. The forwarded message is marked `routed`
  (`PublishOptions.routed`), so it never triggers routing again — a route
  chain (`a → b → c`) forwards one hop per message, and registration
  additionally rejects cycles (`a → b` live, then `b → a`, or longer
  chains) and self-routes with `RangeError`. Routes match the
  alias-resolved publish topic, like every other admission gate. An
  optional `predicate(payload, { topic, seq, messageId })` filters which
  messages forward (a throwing predicate propagates to the publish caller,
  like a throwing schema validator). Re-registering a `src` replaces its
  route (and resets its count); `clearTopicRoute(src)` removes it.
  `getStats().routes` exposes the table (`{ src, dst, predicate,
  forwarded }`), with `forwarded` counting every forward attempt —
  including attempts the destination's admission gates then rejected.
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
  - **Preset dictionary** (`dictionary` option): a `Uint8Array` /
    `ArrayBuffer` / `DataView` of up to 32 KiB (zlib's cap) passed as the
    deflate preset dictionary. Pre-seeding the compressor with the topic's
    recurring byte patterns — field names, enum values, venue prefixes —
    makes SMALL JSON messages compressible that deflate alone barely
    shrinks: build it once by concatenating a few hundred representative
    serialized payloads. The bus snapshots the bytes at registration, so
    later caller mutation cannot corrupt inflation; the same bytes inflate,
    carried per live message and resolved from the bus's dictionary
    registry for durable-log replay (the log record stores the dictionary's
    SHA-256 id, not the bytes). Replaying a dictionary-compressed record
    whose dictionary is not registered fails loudly at `subscribe` time
    instead of delivering garbage — re-register the rule with identical
    bytes first. Invalid dictionaries (empty, > 32 KiB, wrong shape) throw
    `RangeError`. Measured on `bench/compress-dict.bench.ts` (2,000
    market-tick messages, mean 142 bytes serialized; 300-sample dictionary
    capped at 32 KiB; Node v24, AMD EPYC 9D25):

    | mode | wire ratio (after/before) | deflate p50 | deflate p99 |
    | ---- | ------------------------- | ----------- | ----------- |
    | no dictionary | 0.909 | ~21µs | ~140–290µs |
    | with dictionary | 0.192 | ~90µs | ~390–940µs |

    The dictionary shrinks the wire ~4.7x further (0.909 → 0.192) and —
    decisively for small messages — compresses all 2,000 samples while
    plain deflate fails the never-adopt-a-larger-encoding guard on the
    least redundant ones. The trade is CPU: dictionary deflate costs ~4x
    the p50 time, so size the threshold for your payload family.
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
- **Per-subscriber sliding-window delivery rate limit** (in
  `src/ratewindow.ts`, opt-in via
  `subscribe(..., { rateLimit: { maxMessages, perWindowMs } })`): a hard
  delivery ceiling for a downstream with a strict quota. At most
  `maxMessages` deliveries per rolling `perWindowMs` window — the window is
  exact, not a token bucket: a delivery counts for a full `perWindowMs`
  after it happens (a delivery exactly `perWindowMs` old still counts, one
  millisecond older does not), so there is no gradual refill and two quick
  deliveries block the window for its whole width. Messages over budget stay
  queued — in FIFO order, never dropped, never counted as sequence gaps —
  and are delivered on later flush rounds as the window slides, so the
  backlog drains even when no new publishes arrive. Composes with
  delivery-side shaping: shaping paces each flush round first (smoothing
  bursts), then the window enforces the hard cap — a message is delivered
  only when both allow it; it also composes with batch delivery (the window
  bounds batch collection, with pending messages reserving budget until
  hand-off), reliable subscriptions, TTL expiry (an expired waiter is
  dropped without consuming window budget), content filters (filtered
  messages never reach the window), and health probing (a degraded
  subscriber consumes nothing while paused). A re-flush timer (unref'd,
  derived from the bus clock, cleared on unsubscribe) fires when the oldest
  delivery slides out of the window; `getStats()` exposes the per-subscriber
  backlog held back by the window via `rateLimitedWaiting`. `maxMessages`
  must be an integer >= 1 and `perWindowMs` a positive finite number of
  milliseconds — invalid values throw `RangeError` from `subscribe`.
  Disabled by default.
- **Per-subscriber delivery-latency sampling** (in `src/latency.ts`, opt-in
  via `subscribe(..., { deliveryLatency: true })`): measures each delivered
  message's queue dwell — from enqueue into the subscriber's queue to the
  handler hand-off (handler processing time excluded, so a high p99 means
  the subscriber is starved behind its own backlog, not that its handler is
  slow). One sample per delivery (redeliveries sample again); messages that
  never reach a handler (drops, TTL expiries, throttle sheds, filter skips)
  are never sampled. The clock is the bus's injected clock, so tests drive
  deterministic latencies. Each tracked subscriber keeps a bounded rolling
  window (default 1024 samples, `windowSize` tunable); `getStats()` exposes
  `deliveryLatency` (per-subscriber p50/p95/p99 nearest-rank + min/max/mean)
  and `slowestSubscribers` (top 5 by p99 — the first place to look when
  end-to-end lag grows). `src/metrics.ts` renders
  `eventbus_delivery_latency_ms{quantile="0.5"|"0.95"|"0.99",subscriber,pattern}`
  and `eventbus_delivery_latency_samples{subscriber,pattern}` gauges (four
  series per tracked subscriber; untracked subscribers add none). Invalid
  options throw `RangeError` from `subscribe`. Disabled by default.
- **Per-subscriber end-to-end ack-latency SLO tracking** (in
  `src/acklatency.ts`, opt-in via
  `subscribeReliable(..., { ackLatency: true })`): measures each reliably
  delivered message's full accepted→ack round trip — from the moment the
  message is accepted into the subscriber's queue to the moment its
  delivery's `ack()` finishes. Where delivery-latency sampling records the
  enqueue→handler-hand-off queue dwell (handler time excluded), this
  includes handler processing and consumer think time: the round trip the
  producer's SLO actually depends on. `nack()` and ack-timeout redeliveries
  restart the accepted clock, but each message still contributes exactly
  one sample — repeated deliveries never sample twice, and acking a stale
  delivery handle records nothing. Each `Delivery` carries the bus-clock
  `acceptedAtMs` stamp of its hand-off. `getStats()` exposes `ackLatency`
  (per-subscriber p50/p95/p99 nearest-rank + min/max/mean over a bounded
  rolling window, default 1024, plus `withinSlo` / `sloAttainment` against
  `ackSloMs`, default 30000ms); `src/metrics.ts` renders
  `eventbus_ack_latency_ms{quantile="0.5"|"0.95"|"0.99",subscriber,pattern}`
  and `eventbus_ack_latency_samples{subscriber,pattern}` gauges (four
  series per tracked subscriber). An over-budget ack fires `onAckSloMiss`
  synchronously with the sample, the SLO, and the subscriber's identity.
  Invalid options throw `RangeError` from `subscribe`. Disabled by
  default.
- **Per-subscriber handler processing-latency p99 SLO alerting** (in
  `src/latency.ts` + `src/bus.ts`, opt-in via
  `subscribe(..., { latencySlo: { p99ThresholdMs: 50 } })`): measures each
  handler invocation's *processing* time — delivery→handler-return for
  plain subscriptions, delivery→`ack()` completion for reliable ones —
  into a bounded rolling window (default 1024 samples, `windowSize`
  tunable, oldest evicted) and fires `onLatencySloMiss` once when the
  windowed nearest-rank p99 exceeds `p99ThresholdMs`, re-arming after it
  drops back to or below the threshold (the `onBackpressure` / `onLag`
  latch). The alert is advisory only: it never pauses, degrades, or
  otherwise disturbs delivery, and a throwing callback is swallowed —
  unlike the other monitoring callbacks it cannot propagate into the
  flush loop. Relationship to the neighboring features: `deliveryLatency`
  (EB-31) measures enqueue→hand-off queue dwell (handler time *excluded*);
  `ackLatency` (EB-40) measures the accepted→ack round trip per message
  with a *per-sample* SLO; this one isolates pure consumer processing
  time and alerts on the *windowed p99*. It is orthogonal to `healthProbe`
  (EB-18): a slow handler trips this alert while only throws and
  `processingTimeoutMs` overruns count toward health degradation. All
  three latency features sample independently — a reliable subscriber's
  processing samples come from `ack()` completion only, never from the
  synchronous invocation timing, so enabling several never double-counts.
  `getStats()` exposes `subscriberLatencyP99` (per-subscriber p99,
  sample count, threshold, and whether the alert latch is set), and
  `src/metrics.ts` renders
  `eventbus_subscriber_processing_latency_p99{subscriber,pattern}` (one
  series per tracked subscriber). `p99ThresholdMs` is required and must
  be a positive finite number; invalid options throw `RangeError` /
  `TypeError` from `subscribe`. Disabled by default.
- **Per-subscriber lag watermark monitoring** (in `src/lag.ts`, opt-in via
  `subscribe(..., { lagMonitor: true })`): answers the live question — "how
  far behind is this consumer *right now*?" — Kafka-consumer-lag style.
  Reports the live watermark (`now - enqueuedAt(oldest queued message)`, 0
  when the queue is empty) plus a bounded rolling window (default 1024) of
  enqueue→drain dwell samples (p50/p99 nearest-rank + min/max/mean).
  Where delivery-latency sampling records a historical distribution of
  completed deliveries, the watermark is a gauge of current backlog age: a
  subscriber whose handler is stuck shows a watermark that keeps growing
  while nothing is delivered. With `thresholdMs` + `onLag`, the watermark
  also drives alerting — the callback fires once per excursion when the
  watermark reaches the threshold and re-arms after it drops below
  (mirroring the `onBackpressure` / `onDrained` latch); the watermark is
  evaluated on every enqueue and every drain. `getStats()` exposes `lag`
  (per-subscriber watermark + distribution) and `laggingSubscribers` (top 5
  by p99); `src/metrics.ts` renders
  `eventbus_lag_ms{quantile="0.5"|"0.99",subscriber,pattern}`,
  `eventbus_lag_samples{subscriber,pattern}`, and
  `eventbus_lag_watermark_ms{subscriber,pattern}` gauges (four series per
  monitored subscriber). `onLag` without `thresholdMs` throws `RangeError`
  (a callback that could never fire); other invalid options throw
  `RangeError`/`TypeError` from `subscribe`. Disabled by default.
- **Per-(subscriber, key) hotspot monitoring** (in `src/keyhotspot.ts`, opt-in
  via `subscribe(..., { keyHotspot: true })`): samples the depth of each of
  the subscription's keyed ordering reorder buffers (see `PublishOptions.key`)
  — the number of keyed messages held because an earlier keySeq has not been
  fanned out yet. A shallow buffer is normal; a deep one means a predecessor
  is stuck (a delayed schedule that never becomes due, a shed keySeq only
  lazily skipped), the Kafka-hot-partition signal for keyed delivery.
  `getStats()` exposes `hotKeys` (top 10 by depth, hottest first, each with
  subscriber/pattern/key/depth/threshold); `src/metrics.ts` renders the
  `eventbus_key_hotspot_buffer_depth{subscriber,pattern,key}` gauge (at most
  10 series). With `onKeyHotspot`, a stream whose buffer reaches
  `thresholdDepth` (default 100) fires the callback once per excursion,
  re-arming after the depth drains below the threshold. Detection only reads
  buffer depth — it never mutates the reorder buffer, the per-key
  expectation, or delivery order. Only monitored subscriptions are sampled;
  invalid options throw `RangeError`/`TypeError` from `subscribe`. Disabled
  by default.
- **Opt-in delivery-pipeline trace spans** (in `src/trace.ts`, via
  `new EventBus({ trace: true })`): one trace per sampled publish, threading
  the message through `bus.publish` → `bus.admission` → `bus.fanout` →
  `bus.enqueue` (per subscriber) → `bus.deliver` (per subscriber) →
  `bus.ack` (reliable subscriptions, when `ack()` finishes). Each span
  carries `{ traceId, spanId, parentId, name, at, durationMs, attrs }`, with
  `traceId` in 32-hex — the same format as the `webhook-relay-ts` WR-18
  trace ID, so traces correlate across the two by string equality. Sampling
  is head-based: the decision is taken once at admission (rejected or shed
  publishes never start a trace) and every downstream span shares the
  verdict; `sampleRate` (default 1) keeps a fixed fraction, or `sampler`
  plugs in a custom decision. A publish may continue an upstream trace via
  `PublishOptions.traceparent` (W3C `traceparent` header value; also
  accepted per message by `publishBatch`/`publishAtomic` and carried by
  `publishDelayed` to fan-out time) — a missing or malformed value mints a
  fresh trace id. Sampled spans go to the error-isolated `onTraceSpan`
  callback and to a bounded ring buffer exported as
  `getStats().traceSpans` (oldest evicted first, `bufferSize` default
  1024). Disabled by default, and the disabled path is allocation-free
  (one branch per instrumentation site — no WeakMap lookup, no clock read).
  Invalid options throw `RangeError` from the constructor.
- **Per-topic sliding-window publish rates** (in `src/rates.ts`, always
  on): each topic's publish rate in messages/sec over trailing 1s / 1m / 5m
  windows, load-average style (`r1s` / `r1m` / `r5m`) — the real-time signal
  for rate-limiting and scaling decisions. Sampling is a zero-allocation
  O(1) step on the publish hot path: one timestamp written into a
  preallocated per-topic ring buffer (a `Float64Array` of 60,000 slots =
  480 KiB per topic that has ever published), stamped the moment a message
  is accepted for publish — after schema validation and the rate-limit
  budget, so rejections and sheds never pollute the rates; delayed messages
  sample at actual fan-out, not at schedule time. The clock is the bus's
  injected clock, so rates are deterministic in tests. `getStats()` exposes
  per-topic `rates` plus `hotTopics` (top 10 by 1m rate, hottest first —
  the first place to look when deciding where to tighten rate limits or add
  capacity). `src/metrics.ts` renders
  `eventbus_topic_rate_msg_per_sec{topic,window="1s"|"1m"|"5m"}` gauges for
  the hot-topics set only (30 series max — emitting them for every topic
  would tie series cardinality to publisher-chosen topic names). Sizing
  tradeoff: the 5m window is exact up to 200 msg/s sustained per topic;
  past that the ring wraps and the 5m rate degrades to a lower bound, while
  the 1s/1m windows stay exact much longer.
- **Subscriber batch delivery** (in `src/bus.ts`, opt-in via
  `subscribe(..., { batch: true })`): the drain collects up to `maxSize`
  queued messages (default 100) and invokes the handler once with the
  array, amortizing per-message callback overhead — the same `BusMessage`
  objects a plain handler would receive, in publish order. A partial batch
  lingers for up to `maxWaitMs` (default 10 ms) to let a burst that is
  still arriving fill it; a full batch is always delivered immediately.
  Composes with the other delivery features: reliable subscriptions
  receive one `Delivery` envelope per message in the batch (ack each to
  confirm; nacking the whole batch requeues it in FIFO order, and ack
  timeouts / the DLQ keep per-message semantics); `deliveryShaping` bounds
  the batch by the shaping budget (the whole batch consumes one token per
  message); the health probe counts one batch call as one failure; latency
  sampling records every message individually; a message that expires
  while its batch is filling is dropped as expired at hand-off, not
  resurrected. Invalid options throw `RangeError` from `subscribe`.
  Disabled by default. `bench/batch.bench.ts` measures the before/after
  throughput — see [Benchmark](#benchmark).
- **Atomic cross-topic batch publish** (in `src/bus.ts`, via
  `publishAtomic(entries)`): all-or-nothing fan-out for multi-message
  batches — subscribers either see every message or none. Admission runs in
  two phases: first every entry is checked against the same publish-time
  gates `publish` applies (schema validation, then the per-topic rate-limit
  budget) without mutating any bus state, with rate-limit tokens charged
  against a per-batch shadow balance so a batch cannot overdraft the bucket
  with its own entries; then, if all pass, the batch commits through the
  normal `fanOut` path in one synchronous turn with a single scheduled
  flush. A rejected batch returns `{ published: 0, rejected: { index, topic,
  reason } }` and leaves the bus exactly as before — no sequence numbers
  consumed, no rate-limit tokens taken, no durable-log writes, no stats
  changes (not even the rejection counters). TTL is drain-time and never
  rejects a batch; downstream per-message semantics (queue drop policies,
  adaptive throttling, delivery shaping, ACK) still apply to each committed
  message. A throwing validator propagates to the caller, always during
  admission, so a throw can never leave a half-committed batch. An empty
  batch is a no-op.
- **Subscriber-side exactly-once dedup window** (in `src/bus.ts`, opt-in via
  `subscribe(pattern, handler, { deduplicateMessages })`): the complement of
  `publishIdempotent`'s publish-side dedup. Every publish may carry an
  application-level identity (`publish(topic, payload, { messageId })` —
  always stamped by `publishIdempotent`; also on batch/atomic/delayed
  publishes and forwarded across the cluster), and the bus remembers each
  `(topic, messageId)` that entered a dedup-enabled subscriber's queue for
  `windowMs` (default: the bus's publish-side idempotency window, 60 s).
  A re-arrival within the window — via durable-log replay, ack-timeout /
  nack redelivery, or a republished retry — is suppressed before the queue:
  no backpressure budget, no throttle token, and no sequence gap (the
  baseline advances over the deliberate skip, like a content-filtered
  message). Only identified messages participate; an identity that aged out
  of the window is "unknown" and delivered again. Within the window, dedup
  wins over at-least-once: redeliveries are suppressed instead of requeued
  (an operator's `replayDeadLetter` stays a deliberate fresh chance and
  bypasses the window). Pass a stable `consumerId` with
  `EventBusOptions.durableLogDir` to journal the window to
  `__dedup.jsonl` — a restarted bus rehydrates it, so crash recovery cannot
  double-deliver on resume. Suppressions are counted in
  `getStats().dedupDropped` (per topic and global) and exported as
  `eventbus_dedup_dropped_messages_total`.
- **Per-key publish-order delivery** (in `src/bus.ts`, via
  `publish(topic, payload, { key })` — also on batch/atomic/delayed/idempotent
  publishes): every keyed message carries a per-key sequence number assigned
  by the bus at admission in publish order (schedule time for delayed
  deliveries, fan-out for direct publishes; `publishAtomic` draws from a
  shadow cursor so a rejected batch consumes nothing), and each subscriber
  receives same-key messages in strict publish order across topics. When a
  keyed message would arrive out of order — a delayed schedule fanning out
  after a live publish with a higher keySeq — it waits in a per-(subscriber,
  key) reorder buffer, pre-queue (no backpressure budget consumed, no filter
  or throttle evaluated yet), until its predecessors are admitted; different
  keys have independent buffers and never block each other, and unkeyed
  messages bypass the gate entirely. The baseline rule mirrors gap
  detection: the first keyed message fanned out to a subscriber for a key
  establishes its expectation — a late joiner never hangs on pre-subscription
  keySeqs. Every wait terminates: keySeqs that will never be fanned out to a
  subscriber are marked skipped (published to non-matching topics, at
  schedule time for delayed and at fan-out for direct; cancelled/expired/shed
  delayed schedules; filtered replay), so the expectation cascades past
  them instead of hanging. Consumer-group members each observe an ordered
  subsequence (a keyed message assigned to one member advances the others'
  baselines past it). The keySeq rides the durable-log record, so replay
  preserves per-key order and a restarted bus reseeds its cursors instead of
  renumbering. `getStats().keyedReorderedMessages` counts buffered messages;
  `src/metrics.ts` renders `eventbus_keyed_reordered_messages_total`. Composes
  with batch delivery, reliable ACK, health probing, and delivery shaping —
  the gate runs before the queue, so downstream features see messages in
  order.
- **Delayed delivery** (in `src/bus.ts` + `src/delayed.ts`, via
  `publishDelayed(topic, payload, { delayMs } | { deliverAt })`): the message
  waits in a timer min-heap and fans out once the bus clock reaches its due
  time. Returns a delay id; `cancelDelayed(id)` cancels a pending delivery
  (`false` for unknown/already-resolved ids — never throws). Admission
  semantics are deliberate: schema validation fails fast at schedule time
  (rejection returns no id and is counted like a `publish` rejection), but
  the message consumes no sequence number, writes no durable-log message
  record, and burns no rate-limit budget until it actually fans out — at
  which point it goes through the full publish pipeline exactly as if
  published at that moment. TTL is stamped at schedule time: a message whose
  deadline passes before its due time is dropped as expired, never
  delivered. With `durableLogDir`, the schedule is persisted before
  `publishDelayed` returns (a seq-0 record carrying `deliverAt`), so a
  restart rebuilds pending timers; delivery records carry `deliverAt` /
  `delayId`, and cancellations/expiry-drops write tombstones, so a restart
  never resurrects or double-counts a resolved schedule. The wall-clock
  wake-up timer is unref'd (a pending delay never keeps the process alive),
  and due sweeps also run at the start of every flush, so advancing an
  injected clock past a `deliverAt` and publishing anything delivers
  deterministically in tests. `getStats().pendingDelayed` reports the
  scheduled count. Invalid timing options throw `RangeError`; with a durable
  log, a non-JSON-serializable payload throws instead of scheduling
  something that could not survive a restart.

## Multi-tenant namespaces

`bus.createNamespace(prefix)` registers a tenant scope and returns a
thin handle; topics inside namespace `t1` are stored as `t1/<topic>`:

```ts
const t1 = bus.createNamespace('t1');          // NamespaceHandle
const t2 = bus.createNamespace('t2');

t1.subscribe('**', (msg) => { /* every t1 topic; never t2's */ });
t1.publish('orders', { id: 1 });               // → concrete topic `t1/orders`
t2.publish('orders', { id: 2 });               // → concrete topic `t2/orders`
```

Semantics:

- **Isolation is between tenants.** A namespaced publish resolves to
  the concrete `<prefix>/<topic>` and runs the *full* admission
  pipeline on it — ACL, schema, rate limit, TTL, idempotency, durable
  log — exactly like a global publish. A namespaced subscribe matches
  the sub-topic after the prefix, so `**` receives everything in the
  namespace and structurally cannot reach another namespace's topics.
  The global bus (no namespace) is the admin view and still sees every
  topic.
- **Escape attempts are denied.** Topics/patterns passed with a
  namespace must not contain `/` — `publish('t2/x', …, { namespace:
  't1' })` throws `NamespaceDeniedError` (as does any `/` in a
  namespaced topic). Unknown namespaces throw `RangeError`.
- **Flags.** `createNamespace(prefix, { allowPublish: false })` makes a
  read-only scope (publishes → `NamespaceDeniedError`);
  `allowSubscribe: false` makes a write-only scope.
- **Lifecycle.** `getNamespaces()` snapshots prefix, flags, and live
  subscriber counts. `deleteNamespace(prefix)` throws
  `NamespaceNotEmptyError` while live subscribers or retained
  durable-log entries remain, and `RangeError` for an unknown prefix.
- **Durable log.** With `durableLogDir`, each namespace gets a child
  `DurableTopicLog` under
  `<durableLogDir>/namespaces/<url-encoded-prefix>/`: namespaced
  publishes append there, and namespaced replays (`resumeFromSeq` /
  `resumeFromTime`) read from it. Re-registering a namespace recovers
  its on-disk history (sequence/stats cursors reseeded, like the root
  log's construction-time recovery).
- **Stats & metrics.** `getStats()` gains per-namespace aggregates
  (derived from the existing per-topic `TopicStats`); Prometheus
  exposition adds `eventbus_namespace_published_messages_total{namespace}`
  and `eventbus_namespace_subscribers{namespace}`. Both are omitted
  entirely when no namespace is registered, so a namespace-free bus is
  byte-for-byte identical to before.
- **Deliveries name the concrete topic** (`t1/orders`): the handle is a
  scope, not a rename.

Implementation note (deliberate deviation): the bus's wildcard segments
split on `.` only — `/` is a literal character *inside* a segment. That
means the string pattern `t1/**` is a single literal segment matching
nothing but the exact topic `t1/**`, so a namespaced `**` is *not*
translated to the string `t1/**`. Instead the namespace compiles to a
regex prefix `^t1/` over the user's pattern (`**` → `^t1/.*$`), which
is exactly equivalent to string-prefixing wherever string-prefixing
works, and correct where it doesn't. The registered pattern *string*
stays the readable `t1/<pattern>` form. The same literal-`/` property
keeps the EB-15 publish-side prefix index sound: index keys are computed
as `<prefix>/<literal-dot-prefix>`, a candidate key of every concrete
`<prefix>/…` topic. One corollary for operators: no single pattern
covers a whole namespace subtree in TTL / rate-limit / schema / ACL
rule tables (e.g. `t1/**` won't match `t1/orders`) — write
namespace-aware rules against concrete `t1/<topic>` forms. Cluster
advertisement of a leading-`**` namespaced pattern (`t1/**`) likewise
carries hub-native scope; namespace-aware federation is out of scope.

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
- `test/group-offsets.test.ts` — durable group-offset journal
  (`__group_offsets.jsonl`): checkpoint reseeding after restart, max-seq
  wins, corrupt-line tolerance, journal self-compaction; rebalance linger
  (`handoffLingerMs`): leave-event `lingerUntil`/`lingering`, replay
  skipping in-flight seqs during the window, live traffic unaffected,
  backlog replayable after expiry, no window when fully committed, inert
  without a durable log, option validation.
- `test/cluster.test.ts` — cluster federation: route-table sync on join,
  route-aware forwarding with hub-global per-topic seqs, monotonic hub seqs
  across publishers, local vs hub seq spaces, no phantom gaps across seq
  epochs, pattern re-announce + route version bumps, degrade-to-local on hub
  death, reconnect with a new hub epoch, hub heartbeat sweep of dead
  members (raw TCP abrupt death), stale route versions ignored, TLS hub,
  plain-vs-TLS rejection, keyed ordering via hub keySeq, unserializable
  payloads surfacing as forward errors, option validation, double-connect
  rejection, `getStats().cluster`, compressed forward + inflate + raw-payload
  filtering.
- `test/bridge.test.ts` — cross-process fan-out bridge: two-node mirror via
  an in-memory transport, loop-free inbound (no re-mirror), key/keySeq/
  messageId/traceId passthrough with trace continuation on the peer,
  per-key publish order surviving out-of-order transport delivery, inbound
  keySeq advancing the local key cursor, inbound admission (schema
  rejection, ACL deny), source TTL deadline carried verbatim and expiring
  at drain, full ingress buffer shedding the newest envelope with
  `bridgeDropped` counting, malformed envelopes reported never thrown,
  `not-configured`, constructor validation, throwing/rejecting transports
  never breaking publishing, and the bridge Prometheus series.
- `test/partitions.test.ts` — partitioned consumer groups: deterministic
  rendezvous assignment, exclusive per-partition delivery for keyed messages
  (key→partition→owner mapping verified independently), deterministic spread
  of keyless messages, minimal-disruption migration on join/leave (only the
  partitions whose winner changed move), automatic backlog replay of a
  migrated partition to the new owner on leave, replay precision from
  per-partition commits (not from zero), partition-aware `resumeFromSeq`
  (only owned partitions, suppressing double delivery), partition option
  validation with subscriber rollback on count mismatch, per-partition
  commit validation, per-partition checkpoint persistence across restarts,
  `getStats().consumerGroups` partition counts, and round-robin groups
  untouched (no partition metadata).
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
- `test/deadletter.test.ts` — subscriber dead-letter queues: nack/ack-timeout
  exhaustion and repeatedly throwing handlers move poison messages to the DLQ,
  DLQ-local seqs and arrival order, snapshot reads, replay with original seq
  and TTL deadline (no false sequence gap, expired replays dropped as
  expired), fresh redelivery budget on replay, bounded eviction with
  `onDeadLetter`, option validation, `deadLetter: true` defaults, and the
  unchanged retry-forever behavior without the option.
- `test/poison.test.ts` — poison-message diagnosability on the DLQ:
  `lastError` records the handler's thrown message (or `nack`/`ack-timeout`
  when nothing was thrown) and always describes the final failure, batch
  handlers record per message, DLQ entries carry the end-to-end `traceId`,
  `{ limit }` newest-first triage queries, unsubscribe clears the DLQ,
  replay resets the failure record with the budget, and
  `deadLetteredMessages` accounting.
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
- `test/atomic.test.ts` — atomic cross-topic batch publish: all-or-nothing
  commit across topics, schema rejection rolling back with zero side effects
  (no seq consumed, no rate-limit budget burned, no durable-log writes, no
  stats changes), rate-limit rejection restoring the full budget,
  per-batch shadow budgeting for repeated same-topic entries, first-failure
  reporting, empty-batch no-op, throwing validators propagating without a
  partial commit, TTL remaining drain-time (never an admission rejection),
  pattern-vs-exact rule resolution matching `publish`, batch-order delivery
  with independent per-topic seqs, durable-log writes on commit, and
  downstream per-message drop policies still applying.
- `test/batch.test.ts` — subscriber batch delivery: full batches delivered
  immediately as one array call, partial batches lingering up to
  `maxWaitMs` (including arrivals during the linger), `maxWaitMs: 0`
  per-flush delivery, backlog draining across flushes, option validation
  (`RangeError`/`TypeError` before registering), `batch: true` defaults,
  unsubscribe dropping the pending batch and clearing the timer, TTL
  expiry while lingering (dropped, not resurrected), per-message sequence
  gap detection, per-message latency sampling, reliable batches (per-message
  `Delivery` envelopes, batch ack, whole-batch nack requeuing in FIFO
  order, throwing handler with DLQ), and composition with delivery shaping
  (budget-bounded batches), the health probe (one batch call = one
  failure), and content filters.
- `test/delayed.test.ts` — delayed delivery: not delivered before the due
  time, due-time ordering across schedules, immediate fan-out for
  `delayMs: 0` / past `deliverAt`, `cancelDelayed` (pending cancel, double
  cancel and unknown ids as no-op `false`), injected-clock determinism of
  `deliverAt` (relative and absolute forms, on-disk JSONL format), TTL
  expiry while delayed (dropped as expired, never delivered), schema
  fail-fast at schedule time (no id, counted rejection, throwing validator
  propagates), no sequence number consumed until fan-out, rate-limit budget
  burned at fan-out (not at schedule), wall-clock timer liveness with no
  other activity, durable-log persistence with restart rebuild (pending,
  already-due, cancelled, and expired-while-down schedules; no id reuse, no
  double delivery, no recount), unserializable payloads throwing with a
  durable log and scheduling in memory without one, timing-option
  validation, and `getStats().pendingDelayed`.
- `test/filter.test.ts` — subscriber content filtering: reject/accept
  semantics, filtered messages consuming no backpressure budget, no
  sequence gaps from skipped messages (while a genuinely dropped message
  still counts), `TypeError` on a non-function filter, throwing filters
  propagating to the publish call, group-member filtering on the assigned
  member, filters seeing the raw payload under compression, durable-log
  replay honoring the filter (no queue churn, no gaps), and
  `getStats().filteredMessages`.
- `test/compaction.test.ts` — durable-log keyed compaction: repeated key
  updates collapsing to the latest on disk, multi-key latest-each plus
  keyless budget, `readSince` dedupe before any rewrite, restart recovery
  rebuilding the key index, seq-0 schedule records surviving compaction
  undeduped, flag-off preserving old behavior, malformed key lines counted
  corrupt, bus end-to-end keyed publish with replay-only-latest, keys on
  batch/atomic publishes, `RangeError` on invalid keys with zero state
  change, delayed messages carrying their key through restart, and keys
  working without a durable log.
- `test/metrics.test.ts` — bus-level `delivered`/`dropped`/`throttled`/
  `expired`/`rejected` counters (drop-oldest sheds, throttle sheds,
  TTL expiry, schema rejection), `renderPrometheus` exposition output
  (HELP/TYPE lines, per-topic series, label escaping), and the
  `PROMETHEUS_CONTENT_TYPE` media type.
- `test/rates.test.ts` — per-topic sliding-window publish rates:
  deterministic 1s/1m/5m counts and window-edge decay with an injected
  clock, admission-only sampling (schema rejections, rate-limit sheds and
  idempotency duplicates never count), delayed messages sampled at fan-out
  time rather than schedule time, ring wrap-around eviction, `hotTopics`
  ranking by 1m rate with the 10-topic cap, the `getStats` shape, the new
  Prometheus rate gauges, and renderer tolerance for a stats object
  without the rate fields.
- `test/trace.test.ts` — opt-in delivery tracing: `trace.ts` unit tests
  (32-hex trace ids, 16-hex span ids, traceparent parse/format round-trip,
  option validation), disabled-by-default with an empty `traceSpans`
  buffer, `sampleRate: 0` silence, rejected publishes never starting a
  trace, the full publish→admission→fanout→enqueue→deliver span chain with
  parent linkage and attribute shape, per-subscriber enqueue/deliver spans,
  `onTraceSpan` receiving the same spans as the ring buffer (and a throwing
  callback never disturbing delivery), head-based `sampleRate` + custom
  `sampler` override, `traceparent` continuation vs. fresh-id minting on
  malformed values (`RangeError` on non-strings), `bus.ack` spans on ack
  (nack/timeout abandon, stale-handle ack emits nothing, redelivery opens a
  fresh span), ring-buffer oldest-first eviction, batch/delayed publish
  paths, and snapshot isolation of `getStats().traceSpans`.

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

## Batch delivery benchmark

`bench/batch.bench.ts` measures end-to-end delivery throughput with and
without `SubscribeOptions.batch` (`maxSize: 100`, `maxWaitMs: 0`) under a
streaming pattern — 20,000 messages in 200 rounds of 100, one flush per
round — in two handler profiles:

- **lean handler**: tiny per-message work only (field access + accumulate).
- **framed handler**: the same plus a fixed ~2.6 µs per-invocation cost
  (framing a bulk write / opening a transaction — the real reason to
  batch).

Results measured 2026-10-08 on Node v24.20.0, 2-vCPU AMD EPYC 9D25 VM
(median of 5 trials per path):

| scenario | plain (20,000 handler calls) | batch (200 handler calls) |
| -------- | ---------------------------- | ------------------------- |
| lean handler | 2,070,458 msgs/sec | 1,900,746 msgs/sec (~0.92×) |
| framed handler | 351,783 msgs/sec | 2,075,355 msgs/sec (~5.9×) |

The honest read: with a trivial handler, batching is throughput-neutral
(it collapses 20,000 invocations to 200 at no throughput cost); the win
appears exactly where batching is meant to be used — when the handler
pays a fixed per-call cost, amortizing it over 100 messages yields ~6×.
Rerun the script to reproduce.

```bash
node bench/batch.bench.ts
```

## Prometheus metrics

`src/metrics.ts` renders a `getStats()` snapshot as Prometheus text
exposition (0.0.4), hand-written with zero dependencies:

```ts
import { EventBus } from './src/bus.ts';
import { renderPrometheus, PROMETHEUS_CONTENT_TYPE } from './src/metrics.ts';

const bus = new EventBus();
// ... in your HTTP handler:
res.writeHead(200, { 'content-type': PROMETHEUS_CONTENT_TYPE });
res.end(renderPrometheus(bus.getStats()));
```

Exported series:

| Metric | Type | Source |
|---|---|---|
| `eventbus_published_messages_total` | counter | `totalPublished` |
| `eventbus_delivered_messages_total` | counter | `deliveredMessages` — each handler invocation; at-least-once redeliveries count again |
| `eventbus_dropped_messages_total` | counter | `droppedMessages` — backpressure queue sheds |
| `eventbus_expired_messages_total` | counter | `expiredMessages` — TTL discards |
| `eventbus_throttled_messages_total` | counter | `throttledMessages` — publish-side adaptive throttle sheds |
| `eventbus_rejected_messages_total` | counter | `rejectedMessages` — schema rejections |
| `eventbus_rate_limited_messages_total` | counter | `rateLimitedMessages` — per-topic rate-limit sheds |
| `eventbus_duplicate_messages_total` | counter | `duplicateMessages` — idempotent-publish suppressions |
| `eventbus_filtered_messages_total` | counter | `filteredMessages` — subscriber content-filter skips |
| `eventbus_dedup_dropped_messages_total` | counter | `dedupDropped` — subscriber exactly-once dedup suppressions |
| `eventbus_dead_lettered_messages_total` | counter | `deadLetteredMessages` |
| `eventbus_sequence_gaps_total` | counter | `sequenceGaps` |
| `eventbus_keyed_reordered_messages_total` | counter | `keyedReorderedMessages` — keyed messages held in per-(subscriber, key) reorder buffers |
| `eventbus_topic_published_messages_total{topic}` | counter | per-topic `publishedMessages` |
| `eventbus_subscribers` | gauge | `totalSubscribers` |
| `eventbus_unacked_deliveries` | gauge | `unackedDeliveries` |
| `eventbus_throttled_subscribers` | gauge | `throttledSubscribers` |
| `eventbus_degraded_subscribers` | gauge | `degradedSubscribers` |
| `eventbus_shaped_subscribers` | gauge | `shapedSubscribers` |
| `eventbus_pending_delayed` | gauge | `pendingDelayed` |
| `eventbus_topic_subscribers{topic}` | gauge | per-topic fan-out width (subscribers matched by the most recent publish) |
| `eventbus_delivery_latency_ms{quantile,subscriber,pattern}` | gauge | per-subscriber enqueue→delivery queue-dwell p50/p95/p99 (`quantile` = "0.5"/"0.95"/"0.99"); only `deliveryLatency`-tracked subscriptions |
| `eventbus_delivery_latency_samples{subscriber,pattern}` | gauge | samples in the subscriber's latency window |
| `eventbus_ack_latency_ms{quantile,subscriber,pattern}` | gauge | per-subscriber accepted→ack end-to-end latency p50/p95/p99 (`quantile` = "0.5"/"0.95"/"0.99"); only `ackLatency`-enabled reliable subscriptions |
| `eventbus_ack_latency_samples{subscriber,pattern}` | gauge | samples in the subscriber's ack-latency window |
| `eventbus_subscriber_processing_latency_p99{subscriber,pattern}` | gauge | per-subscriber handler processing-latency windowed p99 — the `latencySlo` alerting signal; only SLO-tracked subscriptions |
| `eventbus_key_hotspot_buffer_depth{subscriber,pattern,key}` | gauge | current per-(subscriber, key) reorder-buffer depth, hottest 10; only `keyHotspot`-enabled subscriptions |
| `eventbus_lag_ms{quantile,subscriber,pattern}` | gauge | per-subscriber enqueue→drain dwell p50/p99 (`quantile` = "0.5"/"0.99"); only `lagMonitor`-enabled subscriptions |
| `eventbus_lag_samples{subscriber,pattern}` | gauge | samples in the subscriber's lag dwell window |
| `eventbus_lag_watermark_ms{subscriber,pattern}` | gauge | live consumer-lag watermark (oldest queued message dwell, 0 when empty); only `lagMonitor`-enabled subscriptions |
| `eventbus_topic_rate_msg_per_sec{topic,window}` | gauge | per-topic publish rate in messages/sec over the trailing window (`window` = "1s"/"1m"/"5m"), load-average style; hot topics only (top 10 by 1m rate) |
| `eventbus_group_lag_messages{group,topic}` | gauge | consumer-group lag per (group, topic): assigned seq minus consumer checkpoint, excluding live handoff-linger holds |
| `eventbus_group_partition_lag_messages{group,topic,partition}` | gauge | per-partition consumer-group lag for partitioned groups |

The per-topic series grow with the distinct topics ever published to — the
same bound as `BusStats.topics` — so a bus fanning out over millions of
ad-hoc topic names grows the series count. Topic names come from the
publisher, so this is bounded by the application's own topic space. The
per-subscriber latency series (four per latency-tracked subscription),
the per-subscriber ack-latency series (four per ack-tracked reliable
subscription), and the per-subscriber lag series (four per lag-monitored
subscription) are bounded by the opted-in subscriber count — monitoring is
opt-in, so unmonitored subscribers add no series. The per-subscriber
processing-latency p99 series (one per `latencySlo`-tracked subscription)
is bounded the same way. `eventbus_topic_rate_msg_per_sec` is the
deliberate exception to the per-topic rule: only the hot-topics set
carries it (10 topics × 3 windows = 30 series max), because
rate-limit/scaling decisions need the busiest topics, not the full topic
space.
