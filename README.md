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
  signalling the consumer may resume full speed. Opt-in `queueMaxBytes`
  adds a byte budget on top of the count bound: payloads are estimated as
  JSON UTF-8 bytes, and a message whose admission would overflow the budget
  sheds entries per the subscriber's `dropPolicy` exactly like count
  evictions — the shed counts into `droppedCount` and surfaces as a
  sequence gap. A count-only queue cannot see memory pressure from a few
  huge messages among many small ones; `getStats().queueBytes` exposes the
  live buffered byte total across subscribers. `subscribeReliable()` opts a
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
  every dead-lettering. Opt-in `deadLetter.diagnosticTopic` fans each
  dead-lettering out as one diagnostic event on the configured topic — the
  original payload plus `{ subscriberId, pattern, seq, lastError,
  redeliveries, traceId, deadLetteredAt }` metadata — so operators consume
  and alert on every poison message from a single topic instead of polling
  per-subscriber DLQs. Opt-in `deadLetter.autoReplay` (EB-134) re-enqueues
  dead-lettered messages automatically after an exponential backoff
  (`initialDelayMs` default 1000, `maxDelayMs` default 60000,
  `backoffMultiplier` default 2) instead of waiting for a manual
  `replayDeadLetter`: the replay preserves the original `messageId` and
  bypasses the dedup window, cooperating with `deduplicateMessages`, and
  each replay gets a fresh redelivery budget. After
  `maxAutoReplayAttempts` (default 3) the message is marked `poisoned`
  and `onPoisoned` fires — permanently isolated, no further automatic
  replays. Unsubscribing cancels pending replays; a manual
  `replayDeadLetter` supersedes the scheduled one. `getStats()` exposes
  `autoReplayed` / `autoReplayExhausted` / `poisoned`. Disabled by
  default. High-throughput in-order consumers can call
  `ackUpTo(subId, seq)` (EB-76) to cumulatively acknowledge every
  outstanding delivery with `seq <= seq` in one call instead of acking
  each `Delivery` handle: it returns the number actually acked, skips
  non-outstanding seqs silently (never delivered, already settled, or
  dead-lettered), and throws `RangeError` for a `seq` outside the
  subscriber's delivery history or a non-reliable subscriber (an unknown
  subscriber id returns 0). A range contributes a single ack-latency
  sample — the last acked message's accepted→ack latency — while each
  delivery still runs the tracker's uniform settle path (so
  `maxInflight` slots release and idle-ack activity records exactly as
  with individual acks). `getStats().ackedRanges` counts the calls. Consumers that process out of
  order or in subsets call `ackBatch(subId, deliveries)` / `nackBatch(subId,
  deliveries)` (EB-107) to settle an arbitrary set of `Delivery` handles
  in one call — the complement of `ackUpTo`'s contiguous range, amortizing
  per-message bookkeeping. Each handle settles exactly like an individual
  `ack()` / `nack()` (per-message ack-timeout/DLQ budget, per-message
  ack-latency/trace/SLO sampling under the live-only guard; a batch nack
  requeues each message at the tail in batch order, EB-11 FIFO), stale
  handles are silent no-ops, and handles that never came from this
  subscriber throw after the valid ones settle (one bad handle never
  pollutes the rest of the batch). Returns the number actually settled;
  unknown subscriber returns 0, non-reliable subscriber throws
  `RangeError`. `getStats().ackBatches` / `getStats().nackBatches` count
  the calls. The
  diagnostic goes through the normal publish
  pipeline (ACL, schema validation, rate limiting); an admission rejection
  suppresses it without affecting the DLQ move, and a dead-lettered
  diagnostic message never emits a second diagnostic (no recursion).
  `getStats().diagnosticEvents` counts admitted diagnostics. Every message carries a per-topic `seq`
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
  lists live groups. Group-level checkpoints are mirrored into a pluggable
  `OffsetStore` (`EventBusOptions.offsetStore`, in `src/offsetstore.ts`;
  default in-memory): inject a shared store (e.g. a Redis/Postgres adapter)
  so checkpoints survive restarts and are visible across bus instances,
  and reseed a fresh bus with `await bus.recoverFromOffsetStore()`
  (highest-seq-wins, never regressing live commits). Opt-in
  `autoCommit: { intervalMs }` on `subscribeToGroup` (EB-127) is the
  Kafka-style `enable.auto.commit` analogue: every `intervalMs` each
  member's commit timer checkpoints the highest per-topic `seq` its group
  has been handed since the last commit, through the same `commitOffset`
  path — so the offset journal and the `OffsetStore` write-through see
  exactly the same writes as manual commits. Pending checkpoints also
  flush early — before any handoff-linger window is computed — on every
  rebalance (member join or leave), on member leave/unsubscribe, and on
  static session expiry: a revoked member's in-flight work is checkpointed
  before ownership moves. Manual `commitOffset` and auto-commit are
  idempotent together, highest-seq-wins: an auto-commit never regresses a
  checkpoint a manual commit moved further. The timer is an unref'd
  wall-clock interval — it never keeps the process alive. Default off:
  absent means no timer, no tracking, zero behavior change.
  `getStats().autoCommittedOffsets` counts the auto-committed checkpoints
  (`eventbus_autocommitted_offsets_total` in `src/metrics.ts`). Opt-in `partitions` on `subscribeToGroup` switches a
  competing set from per-message round-robin to Kafka-style partition
  assignment: the key space is divided into N logical partitions, each owned
  exclusively by one member. A keyed message's partition is
  `sha256("k\0" + key) mod N` (one key always lands on one partition, so
  per-key order is preserved within its exclusive consumer); a keyless
  message hashes `(topic, seq)` for a deterministic spread.
  Publish-side alignment (EB-93): a message published with an explicit
  publish-side partition (EB-90 — i.e. on a topic with more than one
  partition) is consumed on that partition instead of being re-hashed, so
  the producer's partition-local ordering survives end to end. When the
  group's partition count differs from the topic's, the publish partition
  folds into the group's space with a stable modulo (`publishPartition %
  N`); keyed-baseline skips, causal-clock skips and per-partition
  watermarks are all recorded against the aligned partition. Messages
  with no publish-side partition (single-partition topics, pre-partition
  log records) keep the hash routing above. Assignment is
  rendezvous (highest-random-weight) hashing over the member roster —
  deterministic from the roster alone, so joins/leaves only migrate the
  partitions whose winner actually changed — or, with opt-in
  `assignment: 'sticky'` on `subscribeToGroup`, the sticky balanced
  assignor (`src/sticky.ts`, EB-54): every member owns `floor(P/N)` or
  `ceil(P/N)` partitions and each rebalance migrates exactly the minimal
  diff the balance requires, so unaffected partitions keep consuming
  through the rebalance with no pause and no replay (cooperative
  protocol, no stop-the-world reassignment). For anything else, pass a
  custom `assignor` (`src/assignor.ts`, EB-106): any `{ name, assign }`
  object whose `assign(partitions, members, previous)` computes
  `partition -> owner` purely and deterministically (rack-aware,
  weighted, …). The assignor is fixed by the
  group's first member (like the partition count); `onRebalance` events
  report it as `partitionRebalance.strategy`. The bus tracks per-partition
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
  Opt-in composite `patterns` (`{ patterns, mode, exclude }`, EB-70) add
  multi-pattern include/exclude matching per message, evaluated after the
  content filter: `mode: 'any'` (default) delivers when any include pattern
  matches the topic, `mode: 'all'` only when every include pattern does, and
  any `exclude` match drops the message — e.g. include `market.**` with
  `exclude: ['market.btc.*']` to take the market feed while dropping the noisy
  BTC leg. Composite-rejected messages are skipped exactly like filter
  rejections (never queued, no backpressure budget, no throttle token, no
  sequence gap) and count into `filteredMessages`; the same narrowing applies
  to durable-log replay and to shadow mirrors. Patterns compile through a
  dedicated composite cache — separate from the single-pattern
  `patternCache`, so `getStats().patternCacheSize` keeps its meaning — and
  `getStats().compositeSubscriptions` reports the live composite count.
  Illegal combinations throw `RangeError` at subscribe time: an empty
  `patterns` array, a bad `mode`, or an `exclude` set covering the include
  space so no message could ever be delivered.
- **`src/cluster.ts` — `ClusterHub` / `ClusterLink`**: cross-process
  cluster federation. Multiple Node processes form a cluster through a
  central TCP hub (`new ClusterHub({ port })`, TLS via `tls: { key, cert }`);
  each node joins with `bus.connectToHub({ url: 'tcp://host:port' })`,
  advertises its subscribed topic patterns, and receives the hub's route
  table. The hub is authoritative for routing: every membership or pattern
  change bumps a monotonic route version and propagates it; nodes apply only
  newer versions (a hub restart is detected via a new hub epoch,
  which supersedes the version — the version-conflict merge). Steady-state
  changes travel as incremental `routes-diff` frames
  (`{ type: 'routes-diff', baseVersion, version, hubEpoch, added, removed }`,
  `added`/`removed` are `{ nodeId, patterns }` entries) that chain onto the
  member's cached table: a diff applies only when `baseVersion` equals the
  applied version, the hub epoch is unchanged, and the version is strictly
  increasing — anything else (gap, epoch mismatch, corrupt frame) makes the
  member request the authoritative full `routes` snapshot via `routes-sync`
  instead of ever applying a diff out of order. Joins and reconnects always
  receive the full snapshot; the hub retains a bounded window of recent
  diffs (`new ClusterHub({ routeDiffRetention })`, default 64, `0` disables
  diffs) so a member that fell behind the window also rebuilds from the
  snapshot. Wire compatibility: members advertise the `routesDiff`
  capability in `hello`; members without it (pre-diff nodes) keep receiving
  full snapshots on every change — upgrade old nodes rather than mixing
  versions long-term, since they miss the bandwidth win, not correctness. A publish
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
  amortized rewrite at 2x). Opt-in time-based retention
  (`durableLogMaxAgeMs`, EB-85): records older than `now - maxAgeMs` (bus
  clock) are evicted when their topic compacts, dual-bounded with the
  count limit — a compact keeps records satisfying both. Delayed-delivery
  schedule records (`seq: 0`) are timer intents and never age out, so
  pending timers survive; evictions are counted in
  `getStats().durableLog.retentionEvicted`. Change the bound at runtime
  with `setDurableLogMaxAgeMs(ms, actor?)` (applies to the root log and
  every namespace child log); the change is recorded in the subscription
  audit log as `retention_updated` with `{ from, to }`. Corrupt log lines
  are skipped and counted
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
  the linger only covers what the leaver did not commit. Every resume
  replay is observable per subscriber: `getStats().replayProgress` carries
  one row per subscription that replayed — `{ subscriberId, pattern,
  total, replayed, skipped, done, failed, error? }` — counting is
  independent per subscriber, so consumer-group members and partitioned
  members each get their own row. `skipped` counts deliberately
  unqueued records (content-filter / composite-pattern rejections,
  handoff-linger skips, already-expired TTLs — never failures);
  `failed` with `error` marks a pass that aborted on an error (replay
  stays fail-closed: the error still propagates to the `subscribe`
  caller). For a live feed of the same numbers, pass
  `onReplayProgress` (requires `resumeFromSeq` / `resumeFromTime`):
  it fires at most every `progressInterval` processed entries (default
  100) plus exactly once on completion (`done: true`) or abort
  (`failed: true`), and is error-isolated — a throwing observer can
  never disturb replay. Opt-in segment
  rotation (`durableLogSegmentRotation: { maxEntriesPerSegment?,
  maxSegmentAgeMs? }`, validated `RangeError`): the per-topic live file
  holds the current segment, and when an append reaches the entry budget
  or the segment's age (on the record timestamps, i.e. the bus clock)
  reaches the age budget, the live file is atomically renamed aside and a
  fresh live segment starts — the publish path only does the O(1) rename.
  An unref'd background pass then gzips the closed segment to
  `<topic>.seg-<seqStart>-<seqEnd>.jsonl.gz` and appends one line to the
  append-only archive index (`__archive_index.jsonl`):
  `{topic, segmentId, path, seqStart, seqEnd, atStart, atEnd, entries,
  messages}` plus the segment's per-key seq highs — so archiving never
  blocks publishing and never keeps the process alive. Replay
  (`resumeFromSeq` / `resumeFromTime`) reads the live segment by default;
  `DurableTopicLog.readSince` / `readSinceTime` take
  `{ includeArchived: true }` to decompress archived segments on demand
  and merge them in seq order ahead of the live records, under the same
  TTL and keyed-compaction rules (an archived value superseded by a live
  record is never resurrected). Archive files that fail to decompress are
  skipped and counted (`getStats().durableLog.corruptArchives`), never
  fatal; restart recovery rebuilds the archive view from the index, adopts
  orphan archives, and folds archived seq/keySeq bounds into the cursors —
  numbering never reuses an archived number. Segment checksums (EB-94):
  every archived segment gets a `<name>.sha256` sidecar (SHA-256 of the
  gzipped bytes, `node:crypto`, zero new dependencies) written after the
  `.gz` and before the index line; recovery verifies every indexed segment
  and every adopted orphan, and archived reads re-verify on demand. A
  mismatch isolates the segment — skipped, never fatal, counted in
  `getStats().durableLog.corruptSegments` — and fires one
  `segment_checksum_failed` subscription-audit event
  (`getSubscriptionAudit()`). Segments archived before checksums (no
  sidecar) read exactly as before. Segment hash chaining (EB-118): every
  archived segment additionally appends one line to the versioned chain
  manifest (`__segment_chain.jsonl`, line format `v: 2`) —
  `{topic, segmentId, path, sha256, prevHash, entries, bytes}` — linking
  each topic's segments in landing order (`prevHash` is the previous
  segment's SHA-256; the first segment's is the deterministic genesis
  marker `'genesis'`, reproducible by construction). The digest covers
  the gzipped archive bytes — the same byte range the EB-94 sidecar
  covers; the sidecar file itself is metadata and is not hashed, so a
  crash between the `.gz` write and the sidecar write cannot break the
  chain. This is the inter-segment ordering integrity that per-segment
  checksums cannot see: EB-94 proves each segment's bytes are intact,
  the chain proves the sequence of segments is the one that landed.
  `DurableTopicLog.verifySegmentChain(dir)` replays the manifest offline
  and reports reordered lines, missing / replaced / unreadable segments,
  and rogue (unchained) segment files, each located by topic/segmentId/
  path — O(segments) plus one full byte read per segment, an operator
  tool, not a hot-path check. A damaged segment is still isolated
  in-process per the EB-94 convention (skipped, counted, audited);
  offline, the chain keeps naming its original digest, so verify reports
  the tamper instead of silently dropping the link. Rotation and
  compaction never break the chain: rotation appends new links, and
  compaction only rewrites the live file. Byte-based retention (EB-119):
  opt-in per-topic live-byte budget (`durableLogMaxBytesPerTopic`, a
  positive finite number of bytes; `RangeError` otherwise, and it
  requires `durableLogDir` like `durableLogMaxAgeMs`) — the count bound
  counts records, this one counts bytes. When a topic's live bytes grow
  past twice the budget the topic compacts, keeping the newest records
  whose sealed on-disk bytes fit the budget: the count, age, and byte
  bounds are dual constraints, enforced at compact time with the same
  amortized semantics (live bytes never exceed twice the budget between
  compacts). With segment rotation on, crossing the budget archives the
  live segment instead of evicting it — the oldest data lands in a
  `.jsonl.gz` segment, still replayable via `readSince`/`readSinceTime`
  with `{ includeArchived: true }` (time-based resume keeps working from
  the archive index). Byte cost is the actual on-disk line bytes, so
  compressed records count their compressed wire bytes and binary
  records their base64 line bytes. Two deliberate exemptions: seq-0
  records (delayed-delivery schedules, retained snapshots, pause
  buffers) are never evicted by the byte bound — the same rationale as
  `maxAgeMs` — and under keyed compaction the latest record per key is
  exempt too (the key space itself bounds retention, Kafka-style);
  exempt state can push live bytes over the budget. `getStats().
  durableLog` reports `bytes` (live-segment bytes across topics, the
  budget's scope — archived segments are history and are not counted)
  and `bytesLimit` (absent when no byte bound is configured). Pluggable
  storage backends (EB-96):
  every object the log keeps — the per-topic live log, archived segments
  and their sidecars, the group-offset journal (`__group_offsets.jsonl`),
  the subscriber-dedup journal (`__dedup.jsonl`), and the publish-dedup
  journal (`__publish_dedup.jsonl`) — is read and written through the
  `DurableStorageBackend` interface (`src/storage.ts`: atomic
  write/replace with optional exclusivity, append, read, prefix list,
  delete, rename, and `scoped()` views; all synchronous, because the
  log's durability contract is that an append is on stable storage before
  it returns). The default is the local filesystem rooted at
  `durableLogDir` — the historical on-disk layout, byte for byte — and
  recovery enumerates the backend (`list()`) instead of reading a
  directory. Pass `new EventBus({ durableLogDir, durableLogStorageBackend })`
  for anything else; namespace child logs share the root backend through
  `scoped('namespaces/<prefix>')` (the `namespaces/` key prefix is
  reserved — the root log's enumeration skips it). At-rest encryption
  (EB-80) and segment checksums (EB-94) are computed above the backend
  layer, so they hold identically on every backend. `src/storage-s3.ts`
  is a worked S3-compatible example: the caller injects SigV4 signing
  and the (synchronous) HTTP transport — the bundled default shells out
  to `curl` — with zero new dependencies; note its documented limits
  (read-modify-write append, best-effort exclusive write) before using it
  for real.
  `getStats().durableLog` additionally reports `archived` (total archived
  entries), `corruptArchives`, and per-topic `segments`
  (`{ live, segments, archived }`). Opt-in at-rest encryption
  (`new EventBus({ durableLogDir, durableLogEncryptor })`, `src/encrypt.ts`):
  every per-topic JSONL data line, every archived segment's record payload,
  and the publish-dedup journal (`__publish_dedup.jsonl`) are sealed with a
  `PayloadEncryptor` before they touch disk — AES-256-GCM via `node:crypto`
  (`AesGcmEncryptor`, the WR-11 convention: fresh random 12-byte IV per
  line, 16-byte auth tag, tampering fails decryption loudly), zero new
  dependencies. Each line is its own sealed envelope (`{v: 2, enc}`), so
  replay and recovery decrypt transparently; a sealed line that cannot be
  decrypted (wrong key, tampered bytes) is skipped and counted in
  `corruptLines`, never returned as garbage. Plaintext lines written before
  the encryptor was configured still read — mixed old/new files recover
  fine — but opening a sealed directory WITHOUT an encryptor throws loudly
  at recovery (fail-closed: a sealed journal is never silently treated as
  empty or corrupt). `durableLogEncryptor` without `durableLogDir` is a
  `RangeError`; namespace child logs seal with the same encryptor as the
  root log. `getStats().durableLog.encrypted` reports whether the log seals
  at rest.
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
  **Slow-consumer backpressure tiers** (`SubscribeOptions.backpressure`,
  default `'block'`): what the bus does when a subscriber's queue is full at
  enqueue time. `'drop-oldest'` sheds the oldest entry to admit the newcomer;
  `'drop-newest'` refuses the newcomer; `'block'` admits-with-shed on first
  saturation (exactly the historical default) then refuses further arrivals
  while the subscriber stays saturated — the already-queued backlog is
  preserved instead of churned, like a closed TCP receive window — until the
  queue recedes below the high-water mark. The tier composes orthogonally
  with `maxInflight` (delivery-side cap, still enforced on the preserved
  backlog) and publish-side admission sheds (which never reach the queue).
  Every queue-level drop is recorded as a `backpressure_dropped` audit event
  (queryable via `getSubscriptionAudit({ action: 'backpressure_dropped' })`);
  `getStats()` exposes `blockedMessages` (refusals under the `'block'` tier,
  included in `droppedMessages`) and `blockedSubscribers`, plus
  `blockedCount(subId)` per subscriber. Anything else throws `RangeError` at
  subscribe time; an explicit `dropPolicy` contradicting an explicit drop tier
  also throws.
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
- **Nack-reason aggregation** (in `src/bus.ts` + `src/metrics.ts`, always
  on for reliable subscriptions): every redelivery is classified into
  exactly one of three mutually-exclusive triggers — `handler_throw` (the
  handler threw synchronously, driving the redelivery),
  `explicit_nack` (the consumer called `nack()` / `nackBatch()` with no
  throw recorded), or `ack_timeout` (the ack-timeout timer requeued the
  delivery). The per-subscriber counters live on the reliable
  subscription's record, are cumulative and never decremented, and never
  appear on plain subscriptions. `getStats().nackReasons` emits one row
  per reliable subscriber with a non-zero count
  (`{ subscriberId, pattern, handlerThrow, explicitNack, ackTimeout }` —
  zero rows omitted to bound cardinality), and `src/metrics.ts` renders
  `eventbus_nack_reasons_total{subscriber,reason}` (one series per
  non-zero class). Diagnostic only: it is orthogonal to subscriber health
  probing above — counting never degrades, pauses, or fires callbacks,
  and redeliveries during an unsubscribe drain count normally.
- **Subscription leases** (in `src/lease.ts`, opt-in via
  `subscribe(..., { leaseMs })`): a lifetime bound on a subscription — when
  the lease expires, the bus unsubscribes the subscriber automatically,
  through the exact same unsubscribe path a manual `unsubscribe()` takes
  (consumer-group leave handling, reliable-delivery ack-tracker teardown
  and queue disposal all run identically; messages still queued at expiry
  are discarded with the queue, per the subscriber's own `dropPolicy`
  semantics — no new drop semantic is invented). `renewLease(subId, ms)`
  restarts the timer from the renewal moment and returns `false` for an
  unknown id or a lease-less subscription; `unsubscribe()` clears the
  timer, so no phantom expiry can fire afterwards. The expiry timer is
  `unref()`'d, so a pending lease never keeps the process alive.
  `EventBusOptions.onLeaseExpired` fires once per expiry (error-isolated —
  a throwing hook is swallowed) and `getStats().expiredLeases` counts
  expiries. Works for consumer-group members too (`subscribeToGroup` passes
  the option through). Non-positive-integer `leaseMs` (including `NaN`)
  throws `RangeError` at subscribe time. Disabled by default — fully
  backward compatible.
- **Idle subscriber auto-reclamation** (in `src/idle.ts`, opt-in via
  `subscribe(..., { maxIdleMs, idleActivity })`): the complement of the
  fixed-duration lease — an inactivity bound instead of a lifetime bound.
  Every qualifying activity re-arms the window from the activity moment,
  so a busy subscriber never expires while it is active; when the time
  since the last qualifying activity exceeds `maxIdleMs`, the bus
  unsubscribes the subscriber automatically, through the exact same
  unsubscribe path a manual `unsubscribe()` takes (consumer-group leave
  handling — including the rebalance a member's departure triggers —,
  reliable-delivery ack-tracker teardown and queue disposal all run
  identically; messages still queued at expiry are discarded with the
  queue, per the subscriber's own `dropPolicy` semantics). Which events
  count is configurable per subscription via `idleActivity` (default
  `'any'`): `'deliver'` = the handler returned without throwing
  (dropped/expired/shed messages never reach the handler, and a throwing
  handler is a failed delivery — neither counts); `'ack'` = `ack()`
  settled a live delivery (reliable subscriptions only — `nack()`, ack
  timeouts and no-op acks do not count). The timer is wall-clock and
  `unref()`'d, so a pending idle window fires even while the bus is idle
  and never keeps the process alive; `unsubscribe()` clears it, so no
  phantom expiry can fire afterwards. `EventBusOptions.onIdleExpired`
  fires once per expiry (error-isolated — a throwing hook is swallowed)
  and `getStats().idleExpired` counts expiries. A subscriber may hold
  both `leaseMs` and `maxIdleMs`: the two are independent opt-ins and
  whichever fires first wins, the other being cleared by the shared
  unsubscribe path. Works per member for consumer-group members
  (`subscribeToGroup` passes the option through). Non-positive-integer
  `maxIdleMs` (including `NaN`), an `idleActivity` without `maxIdleMs`,
  or an unknown activity mode throws `RangeError` at subscribe time.
  Disabled by default — fully backward compatible.
- **Graceful drain-on-unsubscribe** (opt-in via
  `subscription.unsubscribe({ drain: true, drainTimeoutMs })`): instead of
  tearing the subscription down immediately, the bus stops admitting new
  messages for it and removes the subscription only after its queued
  backlog has been fully delivered — for reliable subscriptions, only
  after every outstanding delivery has settled, with `nack()` and ack
  timeouts keeping their normal redelivery and dead-letter semantics while
  draining. Publishes arriving during the drain are skipped: never queued,
  never delivered, and deliberately *not* counted as drops (the consumer
  asked to leave, so fresh work must not be enqueued behind it nor pollute
  backpressure statistics). A second `unsubscribe()` call while draining
  abandons the drain and tears down immediately; `drainTimeoutMs` (default
  30000, must be a positive finite number) bounds the wait — on expiry the
  subscription is torn down through the normal path, with outstanding
  reliable deliveries forgotten exactly as an immediate unsubscribe would
  (no requeue, no further redelivery). `getStats().drainingSubscribers`
  reports how many subscriptions are mid-drain. A non-boolean `drain` or a
  non-positive/non-finite `drainTimeoutMs` throws `RangeError` before any
  state changes. Works through `subscribeReliable` and `subscribeToGroup`
  (a draining group member stays in the competing set until its backlog
  drains, so quiesce publishers first); lease/idle expiry, migration
  completion and topic deletion keep the immediate semantics. Disabled by
  default — a plain `unsubscribe()` behaves exactly as before.
- **Subscription pattern hot update** (`bus.updateSubscriptionPattern(subId, newPattern)`):
  atomically replaces a live subscription's topic pattern — from the next
  publish on, the subscriber matches the new pattern instead of its
  registration pattern, with no unsubscribe/resubscribe round-trip and no
  delivery gap. The subscription is *not* rebuilt: its queue (queued
  messages intact), per-topic sequence baselines, lease, idle window,
  health-probe state, reliable ACK state and every other opt-in survive the
  swap; only the match surface changes — the compiled matcher is re-fetched
  from the shared `patternCache` (a pattern whose last subscriber moved
  away is evicted, the same lifecycle as unsubscribe), the subscriber is
  re-filed in the publish-side prefix index, and the cluster pattern
  advertisement is refreshed. Consumer-group members keep their identity
  and move competing sets — `(groupId, oldPattern)` → `(groupId,
  newPattern)` — with a `'leave'` rebalance on the old set (partition
  redistribution and linger handoff included) and a `'join'` rebalance on
  the new one; composite subscriptions keep their include/exclude rules
  (only the base pattern swaps); namespaced subscriptions take the new
  pattern in their namespace scope and can never cross the namespace
  boundary (`/` in a namespaced pattern throws `NamespaceDeniedError`).
  Like `subscribe`, the new pattern passes the broker-level ACL on its
  registered form (`AclDeniedError`, audited via `onAuthzDenied`), so a hot
  update cannot bypass subscribe-time authorization. All validation runs
  before anything is mutated: an invalid new pattern throws `RangeError`
  and the old pattern stays in force. Returns `false` for an unknown
  subscription id (before validation, so it never throws for one).
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
- **Per-topic adaptive sampling** (in `src/bus.ts`, via
  `setTopicSampleRate(pattern, rate)`): a probabilistic load-shedding gate
  that complements the hard rate limit above. The hard limit caps the
  admitted rate with a token bucket — it protects the ceiling. Sampling
  protects the tail: each non-critical publish to a matching topic is
  dropped with probability `rate` (`0` keeps everything, `1` drops every
  non-critical publish) *before* sequence-number assignment, fan-out, and
  the durable log, so a hot topic sheds load before it ever pays delivery
  cost. A sampled publish is never fanned out, never written to the durable
  log, never queued; `publish` returns 0 for it. The shed consumes no
  sequence number (subscribers see no gap — unlike a rate-limit shed) and
  burns no rate-limit token (the gate runs before the hard rate-limit
  gate). It is counted in `TopicStats.sampledMessages` (and the global
  total, also rendered as `eventbus_sampled_messages_total`) and surfaces
  on `onAdmissionRejected` with reason `'sampled'`.
  Critical publishes (`PublishOptions.critical: true`) are exempt — they
  always pass the sampling gate (but still pass every other admission
  gate, including the hard rate limit). "Non-critical" is
  producer-declared and opt-out: the bus cannot know a payload's business
  value, so producers mark must-deliver traffic (control signals,
  financial transactions) critical and leave expendable traffic
  (telemetry, heartbeats, bulk analytics) non-critical.
  Admission order: schema validation → idempotency-dedup (EB-27) →
  **sampling** → hard rate limit (EB-19). Sampling after schema means an
  invalid payload is rejected with reason `'schema'`, never silently
  sampled; after dedup means a duplicate retry is reported as
  `'duplicate'` and only an admitted first attempt claims a dedup slot;
  before the rate limit means a shed never consumes the budget it
  protects. The random draw comes from the injectable
  `EventBusOptions.samplingRandom` (default `Math.random`) — inject a
  seeded PRNG in tests for deterministic, reproducible sampling
  decisions. Rule matching mirrors `setTopicTtl`: an exact-topic rule
  wins over patterns, the earliest-registered matching pattern wins, and
  re-setting a rule replaces it. `clearTopicSampleRate` removes a rule.
  `rate` must be a finite number in `[0, 1]`; anything else throws
  `RangeError`. Sampling applies to `publish`, `publishIdempotent`, and
  `publishBatch` (per entry — batch entries carry no `critical` flag);
  `publishAtomic` and delayed publishes skip it (documented in the API
  docs: all-or-nothing batches are incompatible with a probabilistic
  shed), and `previewPublish` intentionally does not preview it (a
  preview cannot draw from the random stream without disturbing real
  publish decisions). No performance claims are made for sampling — its
  effect is workload-dependent and unmeasured here.
- **Per-publisher publish rate limiting** (in `src/bus.ts`, via
  `setPublisherRateLimit(publisherId, { messagesPerSec, burst })`): a token
  bucket per publisher identity (refilled from the bus clock, so tests stay
  deterministic) caps how many messages per second one producer may publish —
  so a single hot publisher cannot saturate the bus.
  The first `burst` messages (default: one second of budget) publish
  instantly. A publish that finds the publisher's bucket empty is shed in the
  admission phase, ahead of every other admission gate: it consumes no
  sequence number (subscribers see no gap), is never fanned out, never
  written to the durable log, never queued, and burns no topic rate-limit
  budget; `publish` returns 0 for it. The shed is counted in
  `TopicStats.publisherRateLimited` (and the global total, also rendered as
  `eventbus_publisher_rate_limited_messages_total`) and surfaces on
  `onAdmissionRejected` with reason `'publisher-rate-limit'`. Publishes
  without a `publisherId` — and publishers with no configured limit — are
  unaffected. `publishBatch` sheds over-budget entries one by one;
  `publishAtomic` checks the batch in shadow admission (an over-budget entry
  aborts the whole batch with reason `'publisher-rate-limit'`);
  `publishDelayed` fails fast at schedule time; a suppressed idempotent
  duplicate never reaches the gate, so retries burn no publisher budget.
  `clearPublisherRateLimit` removes a rule (unknown id is a no-op returning
  `false`). Invalid publisher identities and invalid rate values throw
  `RangeError`.
- **Per-topic subscriber caps** (in `src/bus.ts`, via
  `setTopicMaxSubscribers(pattern, maxSubscribers)`): a fail-fast guard
  against subscription storms — a runaway service subscribing in a loop, a
  misconfigured fan-out. Once the cap is reached, `subscribe` (and the
  subscribe-family wrappers: `subscribeReliable`, `subscribeToGroup`, shadow
  mirrors) throws `SubscriptionLimitError` (carrying `pattern`, `limit`,
  `current`) before anything registers — the caller sees the rejection
  instead of silently piling on. The count is per live subscription against
  the subscription's live pattern: consumer-group members each count one,
  shadow mirrors count one, and unsubscribing frees a slot. Rule matching
  mirrors `setTopicTtl`: an exact-topic rule wins over patterns, the
  earliest-registered matching pattern wins, and resolution is live — adding
  an exact rule re-homes existing matching subscribers onto it (like the
  TTL/rate-limit rules). Lowering a cap never kicks existing subscribers;
  only new registrations are rejected. `getStats().topicSubscriberLimits`
  reports every rule's `{ rule, maxSubscribers, currentSubscribers }`.
  `clearTopicMaxSubscribers` removes a rule. Empty patterns and
  non-positive-integer caps throw `RangeError`.
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
  **Versioned schemas** (`setTopicSchemaVersion(pattern, version,
  validator, { graceMs })`, EB-86): register multiple validators per
  topic/pattern for schema evolution. `publish` (and batch/atomic/
  delayed/idempotent publishes) may carry `PublishOptions.schemaVersion`;
  absent means the latest registered version. When a greater version
  registers, older versions are marked superseded and granted this
  registration's `graceMs` as their migration window (default 0 — strict):
  inside the window (boundary inclusive) an old-version publish is still
  validated and admitted, counted in `deprecatedSchemaVersions`
  (per-topic and global) — the migrate-your-producers signal; past the
  window, or for a never-registered version, the publish is rejected
  exactly like a schema failure (`rejectedMessages`, no seq consumed).
  A topic with versioned registrations ignores the legacy
  `setTopicSchema` validator; `clearTopicSchemaVersion(pattern,
  version?)` removes one version or the whole set. `getStats().topicSchemas`
  exposes the registry (rule, latest, per-version `registeredAt` /
  `graceMs` / `supersededAt`) — validators are functions and never leave
  the process. Duplicate versions, negative/non-integer versions, and
  bad `graceMs` throw `RangeError`.
  **Compatibility regression gate** (opt-in, EB-95): pass
  `{ compatibilityCorpus: [...] }` — the sample set of payloads older
  versions accepted — and the new validator is run against every sample
  at registration time (with the registration pattern as the `topic`
  argument). Any rejected or throwing sample throws `RangeError` and
  registers nothing: no version row appears and no existing version is
  stamped superseded — zero side effects. A backfilled lower version is
  gated identically. Each version's gate result surfaces in
  `getStats().topicSchemas` as `compatGate` (`'passed' | 'not-tested'`)
  with `compatCorpusSize`; omitting the corpus skips the gate
  (`'not-tested'`).
- **Per-topic max message size** (in `src/bus.ts`, via
  `setTopicMaxMessageBytes(pattern, maxBytes)`): an admission gate for
  oversized payloads (backend/real-time systems). A publish whose payload
  serializes to more than `maxBytes` UTF-8 JSON bytes is rejected before
  admission — the same zero-side-effect semantics as a schema rejection: it
  consumes no sequence number (subscribers see no gap), is never written to
  the durable log, never reaches a queue, and does not burn rate-limit
  budget; `publish` returns 0 for it. The gate runs after schema validation
  and before sequence-number assignment. Rejections share
  `TopicStats.rejectedMessages` (and the global total) with the other
  pre-admission rejections and surface on `onAdmissionRejected` with reason
  `'message-too-big'`. Rule matching mirrors `setTopicTtl`: an exact-topic
  rule wins over patterns, the earliest-registered matching pattern wins,
  and re-setting a rule replaces it. `clearTopicMaxMessageBytes` removes a
  rule. Empty patterns and non-positive-integer sizes throw `RangeError`.
  `publishAtomic` runs the cap in shadow admission (an oversized entry aborts
  the batch with reason `'message-too-big'`), and `publishDelayed`
  fail-fasts on it at schedule time, exactly like schema validation.
- **Per-topic payload-size cap** (in `src/bus.ts`, via
  `setTopicMaxPayloadBytes(pattern, maxPayloadBytes)`, EB-120): an
  admission gate for oversized payloads positioned after schema validation
  (and after the message-size cap) and before any rate-limit budget is
  touched (backend engineering). A publish whose payload serializes to more
  than `maxPayloadBytes` UTF-8 JSON bytes is rejected before admission —
  the same zero-side-effect semantics as a schema rejection: it consumes
  no sequence number (subscribers see no gap), is never written to the
  durable log, never reaches a queue, and does not burn rate-limit budget;
  `publish` returns 0 for it. The byte measure is `payloadByteSize` — JSON
  UTF-8 bytes, `Uint8Array` binary payloads measured as their wire bytes,
  and payloads with no JSON encoding (`undefined`, functions, circular
  structures, BigInt) measuring 0 bytes and always passing (the compress
  path's passthrough semantics). Rejections share
  `TopicStats.rejectedMessages` (and the global total) with the other
  pre-admission rejections, additionally sum the rejected bytes into
  `TopicStats.payloadRejectedBytes` / `BusStats.payloadRejectedBytes`, and
  surface on `onAdmissionRejected` with reason `'payload-too-large'` (the
  event carries `payloadBytes`, never the payload). The gate applies
  uniformly: `publish`, `publishBatch` (per entry, through the shared
  `fanOut` admission path — a batch envelope never bypasses it),
  `publishAtomic` (any oversized entry rejects the whole batch, surfaced as
  `'payload-too-large'`), `publishDelayed` (fail-fast at schedule time),
  `publishIdempotent`, and `previewPublish` (gate name
  `'max-payload-bytes'`). Rule matching mirrors `setTopicTtl`: an
  exact-topic rule wins over patterns, the earliest-registered matching
  pattern wins (independent matcher cache — never inflates
  `patternCacheSize`), and re-setting a rule replaces it.
  `clearTopicMaxPayloadBytes` removes a rule. When both this cap and
  `setTopicMaxMessageBytes` are configured for a topic, the message-size
  cap is evaluated first. Empty patterns and non-positive-integer sizes
  throw `RangeError`. Unset by default (fully backward compatible).
- **Per-topic sensitive-field redaction** (in `src/bus.ts`, via
  `setTopicRedaction(pattern, fields)`): admission-time masking of
  sensitive payload fields for PII/secret hygiene (backend/real-time
  systems). Each registered field path is a dot-separated object path
  (e.g. `"user.email"`, `"payment.card.number"`); every admitted publish
  to a matching topic has those fields replaced with `"***"` **before**
  schema validation — so validators, the size cap, the rate limiter,
  compression, the bridge mirror, the durable log, and fan-out all see
  only the redacted bytes. The original values are never persisted and
  can never be resurrected by replay; the caller's payload object is never
  mutated. Redaction is idempotent: a field already holding `"***"` is
  left untouched and uncounted, so a payload crossing a route/bridge hop
  is never double-counted. Array element paths are not supported (arrays
  are opaque to redaction), and non-object payloads pass through untouched
  and uncounted. `getStats()` exposes per-topic and global
  `redactedFields` (fields actually masked). Rule matching mirrors
  `setTopicTtl`: an exact-topic rule wins over patterns, the
  earliest-registered matching pattern wins, and re-setting a rule replaces
  it; `clearTopicRedaction` removes a rule. Unset by default (fully backward
  compatible). Empty patterns, non-array `fields`, and illegal paths (empty,
  non-string, empty segments, or `__proto__`/`constructor`/`prototype`
  segments) throw `RangeError` at registration. `publishAtomic` redacts in
  shadow admission (the schema gate and the commit phase see redacted
  bytes), and `publishDelayed` redacts at schedule time — the persisted
  schedule record is already redacted, so the due-time fan-out does not
  redact again.
- **Unified admission-rejection hook** (in `src/bus.ts`, via
  `EventBusOptions.onAdmissionRejected`): every publish-side admission
  rejection — schema-validation rejections, per-topic message-size
  rejections, per-topic rate-limit sheds,
  idempotency-duplicate suppressions, and `publishAtomic` batch rejections
  (surfaced with the failing entry's gate reason) — fires one
  `AdmissionRejectionEvent { topic, reason, payloadBytes, at }`. `reason`
  (`'acl' | 'schema' | 'message-too-big' | 'rate-limit' | 'duplicate' | 'alias-retired'`) maps onto the stats counters
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
- **Cross-bus forward rules** (in `src/bus.ts`, via `forward(dstBus,
  srcPattern, { dstTopic })`): fan-out across multiple `EventBus`
  instances in the same process. `setTopicRoute` only routes within one
  bus; the EB-51 bridge crosses processes via an external broker —
  forward rules fill the middle: every admitted, non-routed message on
  the source bus whose alias-resolved topic matches `srcPattern` (the
  usual wildcard semantics, `compilePattern`) is additionally offered to
  `dstBus`, on `dstTopic` or the published topic when omitted. The
  forwarded message runs the destination bus's full admission pipeline
  (`receiveForward`, internal use only — not part of the public publish
  API) — ACL, schema, rate-limit, TTL — with destination-side sequence
  numbers and budgets, exactly as if a producer published it there
  directly. Identity is preserved across the hop: the forward continues
  the source message's end-to-end trace (same `traceId`, continued as a
  W3C `traceparent`), keeps its TTL deadline verbatim (a forward never
  resets it — when the source message had no deadline, the destination's
  TTL rules apply normally), and carries the application `key` /
  `messageId` along. The forwarded message is marked `routed`, so it
  never triggers the destination's routes or forwards again — one hop
  per message, no amplification. Registration fails fast before anything
  is mutated: `TypeError` when `dstBus` is not an `EventBus`,
  `RangeError` on a self-forward, an empty/invalid `srcPattern`, an
  empty `dstTopic`, or a registration that would close a forwarding
  cycle — the forward graph is walked bus-level and pattern-agnostic, so
  transitive `A→B→C→A` cycles are caught too. Re-registering an
  identical (`dstBus`, `srcPattern`) rule replaces it (and resets its
  count); `clearForward(dstBus, srcPattern)` removes it.
  `getStats().forwards` exposes the table (`{ srcPattern, dstTopic?,
  forwarded }`) as a snapshot — mutating it does not affect the bus —
  with `forwarded` counting every forward attempt, including attempts the
  destination's admission gates then rejected.
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
  - **Auto-trained dictionaries** (`autoTrain` option): instead of
    hand-building the dictionary, the bus can derive it from the topic's
    own recent publishes — zero new dependencies. While enabled, every
    admitted publish contributes its serialized payload to a sliding
    sample window (bounded by both `sampleCount` and `sampleMaxBytes`,
    oldest evicted first), and a background training pass re-derives the
    dictionary whenever `retrainEveryMessages` publishes have been sampled
    or every `retrainEveryMs` (default 60s, only when new samples
    arrived). The derivation is a documented trailing-window heuristic —
    the window's samples concatenated, last 32 KiB kept — not a
    coverage-based trainer. Training never runs inline in the publish
    path: it is scheduled on an unref'd timer, publishes admitted while a
    training is pending keep compressing with the old dictionary, and a
    completed training atomically swaps the rule to the new dictionary
    (new SHA-256 `dictId`, version incremented) while old dictionaries
    stay registered — durable-log replay fail-closed semantics are
    unchanged, and each log record keeps carrying the `dictId` in force
    when it was written. Sampling costs one serialization per publish on
    auto-trained topics; `getStats()` exposes per-topic `autoTrain`
    (`version`, `dictionaryId`, `lastTrainedAt`, `sampledMessages`,
    `sampledBytes`). `clearTopicCompression` (or replacing the rule)
    retires the trainer — queued trainings are dropped and timers
    cleared. Invalid options throw `RangeError`; an `onTrained` callback
    fires after each completed training (a throwing callback is
    swallowed). Disabled by default. Measured on
    `bench/dict-train.bench.ts` (2,000 market-tick messages, mean 142
    bytes serialized; dictionary auto-trained from the first 300 samples,
    32 KiB cap, then applied to the full corpus including 1,700 unseen
    messages; Node v24.20.0, AMD EPYC 9D25):

    | mode | wire ratio (after/before) | deflate p50 | deflate p99 |
    | ---- | ------------------------- | ----------- | ----------- |
    | no dictionary | 0.909 | ~20µs | ~160µs |
    | auto-trained dictionary | 0.192 | ~89µs | ~380µs |

    The auto-trained dictionary matches the hand-built one (0.192 in both
    benches) — the trailing-window heuristic captures this payload
    family's recurring patterns as well as manual curation. Derivation
    itself took 0.11ms, one-time, off the publish path. Same CPU trade as
    above: dictionary deflate costs ~4x the p50 time.
- **Per-topic HMAC message signing** (in `src/signing.ts`, opt-in via
  `setTopicSigningKey(pattern, { keys: [{ kid, secret }], graceMs })`):
  publish-side authenticity with zero new dependencies (`node:crypto`
  only). Every admitted publish to a matching topic is signed with
  HMAC-SHA256 over `(topic, seq, wire bytes)` — the wire bytes are the
  post-compression form, so a compressed envelope's signature covers the
  ciphertext — and the `{kid, sig}` rides the `BusMessage` envelope, the
  durable log, and the bridge mirror. Subscribers opt into verification
  with `subscribe(..., { verifySignature: true })`: the envelope is
  checked before any content processing (before inflation for compressed
  messages), and a failed check drops the message — never queued, never
  delivered — counted in per-topic and global `signatureFailures` with a
  `signature_verification_failed` audit event. Fail-closed: a topic with
  no signing rule, or a message carrying no signature, never verifies.
  Keys rotate without a verification outage: `keys` lists them
  newest-first, the newest signs, and retired keys keep verifying inside
  `graceMs` (inclusive, the WR-20 convention); re-setting the rule stamps
  rotation automatically. Rule matching mirrors `setTopicTtl`: an
  exact-topic rule wins over patterns, the earliest-registered matching
  pattern wins; `clearTopicSigningKey` removes a rule. Invalid options
  throw `RangeError`. Disabled by default: a non-verifying subscriber pays
  a single boolean check per delivery. Cross-bus semantics: a bridge hop
  is a new admission — the destination verifies the source signature
  against its own signing rules for the topic (rejecting the envelope as
  `signature-verification-failed` when the check fails) and re-signs under
  its own rules; signatures are per-bus, never trusted across the hop.
  See "Message signing (HMAC)" below.
- **Per-publisher identity signing** (in `src/signing.ts`, opt-in via
  `setPublisherSigningKey(publisherId, { keys: [{ kid, secret }], graceMs })`,
  EB-97): binds a message to the identity that published it. Every
  admitted publish carrying a `publisherId` with a registered key set is
  signed with HMAC-SHA256 over
  `(topic, seq, publisherId, wire bytes)` — every field NUL-separated so
  no two triples share a base — orthogonal to the per-topic signature
  above: a message can carry both, integrity (topic key) and identity
  (publisher key) independently. The `{kid, sig}` rides the `BusMessage`
  envelope (`publisherSignature`), the durable log, replay, and the
  retained snapshot. Subscribers opt into verification with
  `subscribe(..., { verifyPublisher: true })`: the envelope is checked
  before any content processing, and a failed check drops the message —
  never queued, never delivered — counted in per-topic and global
  `signatureFailures` with a `signature_verification_failed` audit event
  (`detail.kind: 'publisher'`). Fail-closed: a message with no
  `publisherId`, or from a publisher with no registered key set, never
  verifies — an impersonated identity is dropped, not delivered. Key
  rotation follows the WR-20/EB-84 convention verbatim (newest signs,
  retired keys verify inside `graceMs`, re-setting stamps rotation
  monotonically); `clearPublisherSigningKey` removes a key set. Ordering
  note: sensitive-field redaction (`setTopicRedaction`, EB-68) runs at
  admission, BEFORE signing at fan-out — both signatures cover the
  redacted wire bytes, so redaction never breaks verification. Cross-bus
  semantics: a bridge hop is a new admission — the destination re-admits
  and re-signs under its own publisher keys; signatures are per-bus.
  Disabled by default: a non-verifying subscriber pays a single boolean
  check per delivery.
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
- **Subscriber slow-start** (in `src/slowstart.ts`, opt-in via
  `subscribe(..., { slowStart: true })`): cold-start protection for a fresh
  subscription. After the subscription is created, delivery is paced by a
  ramping gate — the allowed rate climbs from `initialPerSec` (default 10)
  up to the subscriber's full speed over `rampMs` (default 1000ms),
  `'linear'` interpolating the rate and `'exponential'` growing it
  geometrically, so a new downstream is never flooded before it has warmed
  up. When `deliveryShaping` is configured the ramp targets its cap;
  without shaping the ramp has no finite ceiling and opens fully when the
  window elapses. Once the ramp completes the gate is dropped — zero
  overhead afterwards. Composes orthogonally with `deliveryShaping` and
  `maxInflight` (a message is delivered only when every enabled gate allows
  it); messages held back stay queued in FIFO order, never dropped, and a
  re-flush timer (unref'd, derived from the bus clock) keeps draining the
  backlog when no new publishes arrive. `getStats().rampingSubscribers`
  counts subscriptions still ramping. Invalid options throw `RangeError`
  from `subscribe`; cannot be combined with `shadow`. Disabled by default.
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
- **Per-subscriber max in-flight deliveries** (in `src/bus.ts`, opt-in via
  `subscribe(..., { maxInflight: N })`): caps how many handler invocations
  may be unsettled at once per subscriber. At the cap the drain stops
  pulling new messages — they wait queued in FIFO order, never dropped —
  and pulls more once a handler settles. For reliable subscriptions a
  delivery settles on `ack()`, `nack()`, or the ack timeout (all three
  release through the tracker's single settle path); for plain
  subscriptions the invocation settles when the handler returns, or when a
  returned promise settles. Composes orthogonally with delivery-side
  shaping (shaping paces the round, the cap bounds unsettled invocations).
  `getStats()` exposes per-subscriber usage via `inFlightUsage`. Must be an
  integer >= 1 — anything else throws `RangeError` from `subscribe`, as do
  combinations with `batch` or `shadow`. Disabled by default.
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
- **SLO-miss phase attribution** (in `src/bus.ts`, opt-in via
  `new EventBus({ processingLatencySloAlert: true })`): when a
  `latencySlo` subscriber's p99 breaches, the miss event carries a
  `phaseAttribution` — windowed p50/p99 for the four delivery phases —
  so you can tell *where* the latency came from without guessing:
  `queueWaitMs` (enqueue→hand-off), `backpressureWaitMs` (time behind a
  fully-gated drain: shaping / rate window / max-inflight / slow-start /
  credit, clipped to the message's own queue lifetime; plain drain path
  only — batched subscribers report 0), `handlerMs` (the SLO tracker's
  own window, always agreeing with the event's `p99Ms`), and
  `redeliveryWaitMs` (reliable nack/timeout requeue→re-hand-off). The
  breach also emits a `processing-latency-slo-breach` alert on the ops
  outlet (when configured) and is counted in `getStats().sloMisses` /
  `sloMissesByPhase`. Default off: with the flag off, SLO tracking
  behaves exactly like the bullet above — no phase stamping, no extra
  clock reads, no ops alert.
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
  expectation, or delivery order. Opt-in `autoMitigate` turns detection into
  action: when a stream's buffer reaches the threshold the stream is
  mitigated — new out-of-order arrivals for that key are shed at fan-out
  instead of piling onto the buffer (their keySeqs are marked skipped so the
  stream never hangs on the gap), `onHotKeyMitigated` fires once per
  engagement, and mitigation releases automatically once the buffer drains
  below `recoverDepth` (default: the alert threshold). Other keys are
  untouched; `getStats().hotKeyMitigations` / `.hotKeyShedMessages` count
  engagements and sheds, and each `hotKeys` entry reports `mitigated`. Only monitored subscriptions are sampled;
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
- **Event-time watermarks and late-message detection** (in
  `src/watermark.ts` + `src/bus.ts`, via `PublishOptions.eventTime`):
  stream-processing semantics, Kafka/Flink-style. A publish may carry its
  business event time (epoch ms — when the event happened, not when the bus
  sees it); the bus tracks a per-topic watermark — `max(eventTime) −
  allowedLatenessMs` — and treats a message with `eventTime < watermark` as
  late. Late messages are still delivered normally (lateness never blocks
  delivery); they are counted (`TopicStats.lateMessages`,
  `BusStats.lateMessages`) and reported once each on the bus-level
  `EventBusOptions.onLate` hook (error-isolated, like `onAdmissionRejected`).
  The watermark is orthogonal to the publish-order `seq` (EB-13): `seq`
  orders arrivals, the watermark orders business time — out-of-order
  arrivals with on-time business times are not late. `allowedLatenessMs`
  is configurable bus-wide (`EventBusOptions.allowedLatenessMs`, default 0)
  and per topic/pattern (`setTopicAllowedLateness` /
  `clearTopicAllowedLateness`, exact-topic rule wins over patterns, invalid
  values throw `RangeError`); only admitted publishes move the watermark —
  schema rejections and rate-limit sheds never do. Publishes without an
  event time never participate (the watermark stays unknown), and the event
  time rides the envelope end to end — durable-log record and replay,
  topic routes, and cross-bus forwards carry it verbatim — so replayed and
  forwarded messages keep their business-time metadata. `getStats()`
  exposes per-topic `watermark` (epoch ms, `undefined` until the first
  event-time publish) and `lateMessages`, and `src/metrics.ts` renders
  `eventbus_topic_late_messages_total{topic}` and
  `eventbus_topic_event_time_watermark{topic}`. Natural fit for
  fintech/settlement feeds: a retried settlement that arrives after the
  stream moved on is flagged late instead of silently double-applied.
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
- **Subscriber tumbling-window aggregation** (in `src/bus.ts`, opt-in via
  `subscribe(..., { window: { windowMs, aggregate, field? } })`, EB-73):
  downstream QPS smoothing for hot topics. The first admitted message
  opens a window `[windowStart, windowStart + windowMs)` on the bus
  clock; every admitted message folds into the window's O(1)
  accumulators instead of being queued; when the window closes the
  subscriber receives a single aggregate message whose payload is
  `{ windowStart, windowEnd, count, value }`. `aggregate` is one of
  `'count' | 'sum' | 'avg' | 'min' | 'max' | 'last' | 'concat'`, with an
  optional dot-path `field` selecting the folded value (non-finite
  values are ignored by the numeric aggregates; a window with no
  contributing messages never delivers). Aggregation happens at fan-out,
  before the queue — the content `filter` still applies to each raw
  message, publish-side compression and the durable log see the raw
  publishes untouched, and the aggregate itself is synthetic (not
  re-filtered, no `messageId`, no TTL). The aggregate then travels the
  subscriber's normal delivery path: backpressure queue, reliable
  `Delivery` (one ack per window), drain-time shaping; its `seq` is the
  window's last raw message `seq`, so sequence-gap detection keeps
  working across windows. The close timer is `unref()`'d; unsubscribing
  discards the open window. `getStats().aggregatedWindows` counts
  completed windows. Invalid options throw `RangeError` from
  `subscribe`. Disabled by default.
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
- **Publish-side idempotent dedup window** (in `src/bus.ts`, via
  `publishIdempotent(topic, payload, { messageId })`): within
  `EventBusOptions.idempotencyWindowMs` (default 60 s), a `(topic,
  messageId)` pair is admitted at most once — a retry carrying the same
  `messageId` returns `{ duplicate: true, accepted: 0 }` instead of
  publishing again, so the downstream sees the message exactly once (the
  payment/fintech retry pattern: a client retrying after a timeout can
  never double-execute). The dedup gate runs before admission: a duplicate
  consumes no sequence number (subscribers see no phantom gap), is never
  written to the durable log, and burns no rate-limit budget. Only an
  *admitted* publish claims a window slot — a schema rejection or
  rate-limit shed leaves the retry free. The window is bounded by
  `EventBusOptions.idempotencyMaxEntries` (oldest evicted) and measured on
  the bus clock, so it is deterministic in tests. With
  `EventBusOptions.durableLogDir` the window survives restarts: every
  admitted claim is journaled to `__publish_dedup.jsonl` (best-effort — the
  in-memory table stays the source of truth, so a failed journal write
  never fails the publish), and a bus opened over the same directory
  rehydrates the unexpired claims at construction; the journal compacts
  itself to the most recently claimed `idempotencyMaxEntries` identities
  when it grows past twice that many lines. `getStats().durableLog`
  exposes the journal line count as `publishDedupEntries`.
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
- **Causal (happens-before) delivery** (in `src/bus.ts`, via
  `publish(topic, payload, { causal: { source?, clock } })` and opt-in
  `subscribe(pattern, handler, { causal: true | { maxBufferPerSource? } })`):
  the producer numbers each causal source with monotonically increasing
  integer clocks starting at 0 (`source` defaults to the alias-resolved
  topic; an explicit source orders messages across topics — a saga's
  steps, a multi-topic transaction), and a causal subscriber delivers them
  in happens-before order per source: a message is delivered if and only
  if its clock equals the source's next expected clock; an early arrival
  waits in a per-source reorder buffer — pre-queue, no backpressure budget
  consumed, no filter or throttle evaluated yet — until its dependencies
  are admitted, and each admission advances the expectation and releases
  buffered successors in clock order. A regressed clock (below the
  expectation — a duplicate or a late arrival from before the horizon) is
  delivered immediately without moving the expectation backwards. Messages
  without a causal clock bypass the gate entirely and never disturb any
  source's expectation. Composes with per-key publish-order delivery: for
  a keyed causal message the causal gate runs first — happens-before order
  takes precedence over publish (keySeq) order — and an admitted message
  then goes through the keyed gate as usual; keyed messages without a
  clock keep their exact keyed behavior. Every wait terminates: each
  per-source buffer is bounded (`maxBufferPerSource`, default 1000) — on
  overflow the oldest buffered message is dropped (drop-oldest) and the
  expectation advances past it, so a dependency that never arrives cannot
  wedge the stream; clocks that will never be fanned out (non-matching
  topics, group assignment to another member, filtered replay) are marked
  skipped so the expectation cascades past them; buffered messages keep
  their TTL deadline and one that expires while waiting is dropped as
  expired at release — never resurrected. The clock rides the durable-log
  record, so replay restores happens-before order across restarts.
  `getStats().causalBuffer` reports `{ depth, droppedMessages,
  regressedMessages }` (plus per-subscriber `causalBufferDepth(subId)`),
  and `src/metrics.ts` renders `eventbus_causal_buffer_depth`,
  `eventbus_causal_buffer_dropped_messages_total`, and
  `eventbus_causal_buffer_regressed_messages_total`. Disabled by default —
  without the opt-in the subscriber builds no buffers and tracks no
  clocks: zero overhead, zero behavior change. Invalid clocks
  (non-integer, negative) and buffer bounds throw `RangeError`.
- **Barrier cut markers** (EB-98, in `src/bus.ts`, via
  `publish(topic, payload, { barrier: { epoch } })` and opt-in
  `subscribe(pattern, handler, { barrier: true | { maxHeldMessages? } })`):
  a consistency cut point across the subscriber's whole delivery stream —
  the cross-topic barrier per-source causal clocks cannot express
  (config switches, snapshot boundaries). A barrier publish is
  control-plane: it consumes no per-topic sequence number (`seq` 0, the
  EB-13 counter never advances, no gap), is never written to the durable
  log, never becomes a retained message, and skips schema / rate-limit /
  compression / signing — but still passes the ACL, so unauthorized
  publishers cannot inject cut markers. Each opted-in subscriber arms on
  the first barrier with a higher epoch than any seen (epochs must be
  strictly increasing per subscriber; an out-of-order barrier is
  discarded, counted, and audited as `barrier_dropped`): the barrier
  itself is delivered as a control message (`msg.barrier.epoch`, user cut
  metadata in `msg.payload` verbatim), and every message arriving after
  it in delivery order is held pre-queue until the barrier is handed to
  the handler — then released at once, re-injected through the normal
  pipeline. The barrier gate runs before the causal gate, so
  happens-before order is preserved on both sides of the cut.
  Subscribers that did not opt in never see barrier messages. Every hold
  terminates: the hold buffer is bounded (`maxHeldMessages`, default
  1000) — on overflow the gate fails open (releases everything held,
  disarms, counted in `getStats().barrier.overflowedHolds`); a barrier
  shed by backpressure before delivery also releases fail-open.
  `getStats().barrier` reports `{ publishedBarriers, droppedBarriers,
  heldMessages, releasedMessages, overflowedHolds }`. See "Barrier cut
  markers" below for the delayed / retained / replay combination matrix.
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
  something that could not survive a restart. Opt-in schedule-skew
  observability (EB-136, in `src/scheduleskew.ts`, via `new EventBus({
  scheduleSkew: {} })`): samples every fanned-out schedule's skew — the
  actual fan-out time minus its due time (`deliverAt`, the expected
  fan-out moment; no redundant timestamp is stamped) — into a bounded
  rolling window (default 1024, oldest evicted) with nearest-rank p50/p99.
  A clock that moved backwards clamps the sample at 0. `getStats()
  .delayedScheduleSkew` reports `{ samples, p50Ms, p99Ms, minMs, maxMs,
  meanMs }`, and `src/metrics.ts` renders
  `eventbus_delayed_schedule_skew_ms{quantile="0.5"|"0.99"}`. With
  `scheduleSkew: { alertThresholdMs, onScheduleSkew }`, every sample whose
  skew exceeds the threshold fires `onScheduleSkew` with `{
  scheduledFor, fannedOutAtMs, skewMs, topic, delayId }` — advisory only:
  a throwing callback is isolated and never breaks fan-out. Only
  schedules that actually fan out are sampled; cancelled, expired, and
  pause-shed/pause-buffered schedules never fan out and stay out of the
  distribution. Schedules rebuilt from the durable log sample exactly like
  live ones, and invalid options throw `RangeError`/`TypeError` from the
  constructor.
- **Shadow subscriber traffic mirroring** (in `src/bus.ts`, via
  `subscribe(pattern, handler, { shadow: true | { queueSize?,
  dropPolicy? } })`): the subscription becomes a shadow subscriber — it
  receives a full copy of production traffic for regression / load-test
  validation, in its own independent queue with its own backpressure
  policy, and it can never disturb production. The mirror is excluded from
  the `publish` fan-out width (`matched`/`accepted`), from the topic's
  `subscriberCount`, and from `deliveredMessages`/`droppedMessages`/
  `sequenceGaps` and every topic/group stat; shadow deliveries are tracked
  in independent counters only — `shadowed`/`dropped` per subscriber in
  `getStats().shadows`, `shadowedMessages`/`shadowDroppedMessages`
  bus-wide, and `eventbus_shadowed_messages_total` /
  `eventbus_shadow_dropped_messages_total` in `src/metrics.ts`. A shadow
  subscriber never joins group assignment: it cannot steal a consumer
  group's copy, advance the assignment watermark, or touch committed
  offsets (`commitOffset`). A throwing shadow handler is swallowed — it
  never breaks the flush loop or affects production delivery, and the
  message still counts as shadowed, exactly like production counts a
  throwing handler's message as delivered. The shadow queue is sized by
  `ShadowSubscribeOptions` (falling back to the subscription's own
  `queueSize`/`dropPolicy`); a saturated shadow sheds only its own copy.
  The shadow copy bypasses the per-subscriber ordering gates (keyed /
  causal) — it mirrors fan-out order — and skips sequence-gap tracking
  (loss is visible in the shadow `dropped` counter); TTL expiry still
  discards an unexpired-in-time copy, uncounted. Incompatible options fail
  fast with `RangeError`: `subscribeReliable` and `subscribeToGroup`
  reject `shadow` outright, and within `subscribe` so do
  `resumeFromSeq`/`resumeFromTime`, `throttle`, `deliveryShaping`,
  `rateLimit`, `healthProbe`, `batch`, `causal`, `deduplicateMessages`,
  `keyHotspot`, and `ackLatency` — a shadow mirror is a plain
  live-traffic tap. The passive observability knobs compose normally:
  `filter` (a throwing filter still propagates to the publish call, like
  production), `deliveryLatency`, `lagMonitor`, `latencySlo`,
  `onBackpressure`, `onDrained`. Disabled by default — without the opt-in
  the subscriber builds no shadow state: zero overhead, zero behavior
  change. Invalid shadow tuning throws `RangeError` (`TypeError` for a
  non-object value).

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

## Subscription migration

`exportSubscription` / `importSubscription` / `completeMigration` /
`cancelMigration` move one subscription — backlog, options, ack position
and lease — from one `EventBus` instance to another without losing or
duplicating messages (EB-82). The flow is:

```ts
// 1. Source: pause delivery and take the snapshot.
const snapshot = source.exportSubscription(sub.id);
if (snapshot === null) throw new Error('nothing to export');
// 2. Target: rebuild the subscription. The handler cannot travel in the
//    snapshot (functions don't cross instances) — you supply it here:
//    a reliable (Delivery) handler for reliable snapshots, a batch
//    handler for batched ones.
const { subId } = target.importSubscription(snapshot, (msg) => { /* ... */ });
// 3a. Source: the target has taken over — unsubscribe the source copy.
source.completeMigration(snapshot.handoffToken);
// ...or 3b. Source: abort the migration — resume delivery where it paused.
source.cancelMigration(snapshot.handoffToken);
```

The snapshot carries: the pattern as registered (pre-namespace
translation) plus the namespace name; the serializable subscribe options;
the committed ack position (`ackCommittedSeq`); the remaining lease time;
a backlog stats reading; and every undelivered message in delivery order —
in-flight reliable deliveries first (ascending delivery seq), then the
queued backlog (FIFO). The message envelopes travel by reference (no deep
clone); moving a snapshot across processes (e.g. as JSON) needs JSON-safe
payloads and option values, which is the caller's responsibility.

Guarantees: acked messages are absent from the snapshot by construction
and are never re-delivered; unacked messages are taken out of the source
ack tracker *without* requeueing, so the source never redelivers them and
the target delivers each exactly once; the handoff token is single-use
per importing instance, so one snapshot can never double-subscribe the
target; the target's `Delivery.seq` numbering continues the source's
committed position. Export pauses delivery on the source (the drain skips
the migrating subscriber; the backlog stays queued, nothing is dropped by
the pause) while the subscription keeps counting as live in `getStats()`
(`migratingSubscribers`). Import re-runs the normal subscribe path, so
unknown namespaces, ACL denials and invalid options fail fast exactly like
a fresh subscribe; a failed import consumes no token, so you can fix the
cause and retry.

Honest boundaries — read before relying on them:

- **Non-serializable options don't migrate.** Content filters and
  lifecycle callbacks (`filter`, `onBackpressure`, …) are functions; they
  are listed openly in `snapshot.unsupportedOptions`, never dropped
  silently. The target must re-attach its own.
- **In-flight handler executions at export time are the caller's
  responsibility.** A source handler still running when its delivery is
  taken into the snapshot may process the message; the target will deliver
  it again — at-least-once, so consumers must be idempotent. Stale source
  `Delivery` handles settle to silent no-ops (no phantom requeues).
- **Per-message TTL deadlines don't travel** (migrated messages are
  admitted without a deadline), nor do dead-letter-queue contents, open
  tumbling-window aggregates, per-key reorder-buffer holdings, or
  per-message redelivery counters (they restart on the target).
- **Publishes between export and complete are not in the snapshot.**
  They queue on the (paused) source; `completeMigration` reports them as
  `stragglers` and discards them with the source queue — quiesce producers
  first, or accept the count.
- **Durable-log resume cursors don't transfer** (`resumeFromSeq` /
  `resumeFromTime`): the snapshot already carries the full undelivered
  backlog, and replaying from the cursor would re-deliver acked messages.
- **Consumer-group members can't migrate** (`exportSubscription` returns
  `null`): the competing-consumer roster is bus-local. Unknown
  subscription ids, already-migrating subscriptions and unknown handoff
  tokens likewise return `null` / `false` instead of throwing (the
  `renewLease` convention); malformed snapshots and double imports throw
  `RangeError`.

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
- `test/offsetstore.test.ts` — pluggable `OffsetStore` (EB-114):
  group-level commits mirrored to an injected store, per-partition and
  per-instance checkpoints staying in memory, default in-memory zero
  behavior change, `TypeError` on malformed stores, throwing stores never
  breaking the commit (counted in `offsetStoreWriteErrors`),
  `recoverFromOffsetStore()` reseeding highest-seq-wins with malformed
  entries skipped.
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
- `test/cooperative-rebalance.test.ts` — sticky balanced partition
  assignment (EB-54): `src/sticky.ts` unit tests (2/2/2 balance vs
  rendezvous 3/2/1 skew, determinism, maximal stickiness under the
  balance target over seeded roster churn, fewer-partitions-than-members,
  empty roster, invalid input) and bus integration (join migrates exactly
  the minimal diff with one shed partition per incumbent, leave
  redistributes only the departed member's partitions, stable partitions
  keep consuming through a rebalance with no loss/duplication —
  cooperative no-stop-the-world, strategy mismatch rolls back the join,
  invalid value rejected before registration, rendezvous stays the
  default, fully drained groups start fresh without pinning).
- `test/durablelog.test.ts` — durable topic log: append/readSince round-trip,
  reopen recovery of topics/seqs/counts, corrupt-line tolerance, amortized
  compaction, option validation; bus integration: per-publish logging with
  seq and TTL deadline, seq continuity across restart, `resumeFromSeq`
  replay (missed-only, pattern-scoped, `0` = everything), loud failure when
  `durableLogDir` is missing, TTL-expired replays counted as expired, no
  phantom sequence gaps after replay, per-member group replay, unserializable
  payloads delivered live but not logged, and the on-disk JSONL line format.
- `test/archive.test.ts` — durable-log segment archiving: count-based and
  age-based rotation triggers, the archive index format
  (`{topic, segmentId, path, seqStart, seqEnd, atStart, atEnd, entries}`),
  background archiving never blocking the publish path (rotation renames
  synchronously; the gzip lands later), default replay reading only the
  live segment, opt-in `{ includeArchived: true }` reads merging archives
  in seq order, corrupt gzips skipped and counted, corrupt index lines
  tolerated on recovery, restart rebuilding the archive view without seq
  reuse (keySeqs reseeded), keyed-compaction dedup across the
  live/archive boundary, no archiving without the opt-in, and `RangeError`
  on invalid rotation options.
- `test/backpressure.test.ts` — both drop policies, one-shot high-water-mark
  behavior, drain ordering, runtime watermark adjustment (`setHighWaterMarkRatio`,
  validation, mid-excursion lowering/raising semantics) and the `onDrained`
  recovery callback.
- `test/byte-budget.test.ts` — byte-budgeted backpressure queues: count+bytes
  dual-dimension shedding per drop policy, priority-aware byte eviction,
  oversized-item rejection, byte accounting across `drainLiveUpTo`,
  `maxBytes`/`byteSize` validation, the JSON default estimator, the bus-level
  `queueMaxBytes` subscribe option (dropped + sequence-gap parity with count
  evictions, fail-fast validation, `getStats().queueBytes`, the
  `eventbus_queue_bytes` gauge), and zero overhead when the budget is off.
- `test/ack.test.ts` — at-least-once delivery: ack suppresses redelivery,
  nack redelivers immediately with a bumped `redeliveries` count, ack timeout
  auto-requeues, double/late settles are no-ops, unsubscribe cancels pending
  timers, redelivered messages keep their original TTL deadline, and
  `ackTimeoutMs` validation.
- `test/ackbatch.test.ts` — batch settles (`ackBatch` / `nackBatch`,
  EB-107): out-of-order subset acking, per-message nack tail-requeue in
  batch order (EB-11 FIFO), duplicates/stale handles as no-ops, invalid
  handles throw after the valid ones settle, empty batch no-op, unknown /
  non-reliable subscriber rules, unwrapped-handle recognition, and
  tracker-level `ackBatch` / `nackBatch` ownership validation.
- `test/ackupto.test.ts` — cumulative ack (`ackUpTo`, EB-76): whole-range
  acking with the per-call count, silent skipping of non-outstanding seqs,
  `RangeError` on out-of-history seqs and non-reliable subscribers, 0 on
  unknown subscriber ids, fresh seqs on nacked redeliveries, DLQ-skipping
  (EB-28), single ack-latency sample at the last ack moment (EB-40), and
  `maxInflight` slot release through the uniform settle path (EB-67).
- `test/deadletter.test.ts` — subscriber dead-letter queues: nack/ack-timeout
  exhaustion and repeatedly throwing handlers move poison messages to the DLQ,
  DLQ-local seqs and arrival order, snapshot reads, replay with original seq
  and TTL deadline (no false sequence gap, expired replays dropped as
  expired), fresh redelivery budget on replay, bounded eviction with
  `onDeadLetter`, option validation, `deadLetter: true` defaults, and the
  unchanged retry-forever behavior without the option.
- `test/deadletter-topic.test.ts` — centralized poison-message diagnostics:
  `deadLetter.diagnosticTopic` publishes one diagnostic event per
  dead-lettering (original payload + subscriber/seq/error/traceId metadata),
  a dead-lettered diagnostic never emits a second diagnostic (recursion
  cut), diagnostics go through normal admission (ACL denial suppresses
  without affecting the DLQ move), admission-time counting, and the
  `eventbus_diagnostic_events_total` exposition series.
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
- `test/dict-train.test.ts` — auto-trained per-topic deflate dictionaries:
  `autoTrain` option validation (`RangeError` fail-fast), disabled by
  default (no stats field, no timers), message-count trigger training with
  atomic dictionary swap and transparent inflation afterwards, training
  never running inline in the publish path (a publish admitted while a
  training is pending carries the old `dictId` on its durable-log
  record), old dictionaries staying registered so pre-train records still
  replay while fail-closed semantics are unchanged for unknown
  dictionaries, `retrainEveryMs` wall-clock cadence firing only on new
  samples, `clearTopicCompression` and rule replacement retiring the
  trainer (timers cleared, no further training, stats gone), the
  dual-bounded sample window (count and bytes, unserializable payloads
  skipped), a throwing `onTrained` callback being swallowed, and
  `getStats()` auto-train state before/after training.
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
- `test/schedule-skew.test.ts` — delayed-delivery schedule-skew observability
  (EB-136): skew sampling (`now - deliverAt`) on the real fan-out path only,
  nearest-rank p50/p99 over the window, oldest-first eviction past
  `windowSize`, negative-sample clamping (backwards clock), per-sample alert
  (`onScheduleSkew` fires when skew exceeds `alertThresholdMs`, with
  `scheduledFor === deliverAt`, `fannedOutAtMs`, `skewMs`, `topic`,
  `delayId`; a throwing callback never breaks fan-out; at-or-below
  threshold stays silent), opt-in semantics (no option: zeroed stats, no
  Prometheus series), durable-log restart recovery sampling through the
  same sweep, constructor option validation, `renderPrometheus` exposition
  of `eventbus_delayed_schedule_skew_ms{quantile="0.5"|"0.99"}`, and
  expired-while-delayed schedules staying out of the distribution.
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
- `test/causal.test.ts` — causal (happens-before) delivery: out-of-order
  arrivals delivered in clock order, independent per-source streams (one
  stalled source never blocks another), source defaulting to the topic
  name, regressed clocks delivered immediately without moving the
  expectation back, non-causal messages bypassing the gate, zero behavior
  change when the opt-in is off, composition with keyed ordering
  (happens-before wins over publish order), keyed-without-clock behavior
  unchanged, buffer depth in `getStats().causalBuffer` and per subscriber,
  over-full buffers dropping the oldest wait and advancing past it
  (anti-deadlock), TTL-expired buffered messages dropped as expired
  without wedging the stream, group members each observing an ordered
  subsequence, `RangeError` on invalid clocks/buffer bounds, the clock
  persisting on the durable log with replay restoring happens-before
  order, and the Prometheus exposition.
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
- `test/watermark.test.ts` — event-time watermarks (EB-59): `src/watermark.ts`
  unit tests (peak tracking, pre-fold lateness judgement, boundary on-time,
  monotone watermark, validation helpers), watermark advancement with the
  maximum event time, plain publishes never moving the watermark, late
  messages still delivered but counted per topic and bus-wide, `onLate`
  event shape and error isolation, bus-level and per-topic/pattern
  `allowedLatenessMs` with exact-wins-over-pattern resolution and
  setter/clearer semantics, invalid config throwing `RangeError`/`TypeError`,
  invalid `eventTime` throwing from every publish entry point without
  mutating state, orthogonality to publish-order `seq`, rejected/shed
  publishes never moving the watermark, `eventTime` on batch/atomic
  entries, durable-log persistence with replay restoring the envelope, and
  the late-message/watermark Prometheus series.
- `test/shadow.test.ts` — shadow subscriber traffic mirroring (EB-63): a
  shadow subscription receiving a full copy of production traffic, the
  mirror excluded from the `publish` fan-out width and the topic's
  `subscriberCount`, the shadow queue shedding independently (drop-oldest
  evictions and drop-newest discards) with zero movement in production
  `droppedMessages`/`deliveredMessages`, a throwing shadow handler isolated
  from production delivery and the flush loop, consumer-group assignment
  and committed offsets undisturbed by a same-pattern shadow, `RangeError`
  on every incompatible option (`subscribeReliable`, `subscribeToGroup`,
  throttle/shaping/rate-limit/health/batch/causal/dedup/keyHotspot/
  ackLatency/resume) plus invalid shadow tuning with zero state change, a
  shadow content filter narrowing the mirror without touching production
  filter stats, composition with `deliveryLatency`/`lagMonitor`, the
  shadowed/shadow-dropped Prometheus counters, shadow sheds never
  surfacing as production sequence gaps, and unsubscribe stopping the
  mirror.
- `test/migration.test.ts` — cross-instance subscription migration
  (EB-82): plain-subscription export/import round-trip (backlog moves in
  order with per-topic seqs preserved, source delivery pauses while
  migrating, `completeMigration` unsubscribes the source and reports
  stragglers), reliable migration (acked messages never redeliver,
  unacked in-flight deliveries redeliver exactly once on the target with
  `Delivery.seq` continuing the committed position; queued-only backlog
  restarts numbering), `null`/`false` on unknown ids, group members and
  double exports/completions, `cancelMigration` resuming delivery without
  loss (in-flight backlog restored ahead of the queue, at-least-once),
  double import rejected by the single-use handoff token, snapshot/handler
  `RangeError` validation with no half-registered state, fail-fast import
  when the target lacks the namespace, composite-pattern migration,
  non-serializable options reported in `unsupportedOptions`, and lease
  migration restoring the remaining time rather than the full duration.
- `test/topic-pause.test.ts` — topic pause/resume (EB-88): shed-mode
  rejection counting (`pausedMessages` per-topic + global, hook reason
  `'topic-paused'`, sequence gap visible downstream), pause as the first
  admission gate (schema never runs, publisher rate-limit budget untouched),
  queue-mode buffering with no seq/budget burned and in-order resume fan-out,
  TTL deadline preservation across the pause (expired-while-buffered counts
  expired, never resurrected), exact-first/earliest-pattern rule matching
  with an independent matcher cache, `publishBatch` per-entry sheds,
  `publishAtomic` all-or-nothing rejection, `publishDelayed` interplay
  (shed fail-fast at schedule, queue defer at due time, due-time shed),
  `publishIdempotent` pause-before-dedup-claim plus resume-time dedup,
  `topic_paused`/`topic_resumed` subaudit events, Prometheus pause series,
  argument validation, and restart recovery of the pause buffer with
  tombstones.
- `test/cancel-delayed-topic.test.ts` — bulk delayed-delivery
  cancellation by topic (EB-89): exact-topic and wildcard-pattern
  expansion, already-fanned-out/expired/cancelled exclusion, empty result
  never throwing, tombstone persistence across restart, keyed-schedule
  per-key release, `delayed_cancelled` subaudit details, and argument
  validation.

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

## Publisher identity

Every publish entry point accepts an optional `publisherId` — a
producer-side label (service name, client id, tenant) that makes publish
traffic attributable:

```ts
bus.publish('orders.created', order, { publisherId: 'checkout-svc' });
bus.publishIdempotent('payments.settled', payment, {
  messageId: payment.id,
  publisherId: 'payments-svc',
});
// per-entry on batch/atomic, and on publishDelayed too
bus.publishBatch([{ topic: 't', payload: x, publisherId: 'etl' }]);
```

The identity is 1–64 ASCII alphanumeric, `_` or `-` characters; anything
else throws `RangeError` before the publish mutates any state. It is a
label, not a credential — the bus never authenticates it.

What it buys:

- **Per-publisher counters** — only publishes that clear admission count
  (a schema rejection or rate-limit shed never claims a slot, and a
  suppressed idempotent duplicate is not a publish). `getStats().publishers`
  lists every identity with its total, most-published first; anonymous
  publishes move no per-publisher counter.
- **Attribution on rejections** — every `onAdmissionRejected` event carries
  `publisher` when the rejected publish carried one, so a poison producer
  is visible at the same place its rejects are counted.
- **Per-publisher rate limiting** (real-time systems / backend engineering)
  — `setPublisherRateLimit(publisherId, { messagesPerSec, burst })` puts a
  token bucket on one producer: an over-budget publish is shed in the
  admission phase, ahead of every other gate — no sequence number consumed
  (subscribers see no gap), never fanned out, never logged, no topic
  rate-limit budget burned. Counted in `getStats().publisherRateLimited`
  (also `eventbus_publisher_rate_limited_messages_total` in the Prometheus
  exposition) and surfaced on `onAdmissionRejected` with reason
  `'publisher-rate-limit'`; `clearPublisherRateLimit` removes the rule.
  Anonymous publishes and unconfigured publishers stay unlimited, so the
  limit is opt-in per producer.
- **End-to-end attribution** — the identity rides the message envelope and
  the durable-log record: `resumeFromSeq` replays restore it, delayed
  schedules keep it across restarts (a rebuilt timer fans out under the
  original publisher), topic routes and cross-bus forwards carry it to the
  destination's admission. Bridge and cluster inbound are the deliberate
  exception: a receiving node treats remote traffic as anonymous inbound —
  the origin node already counted the publish under its publisher.

The identity also renders as
`eventbus_publisher_published_messages_total{publisher}` in the
Prometheus exposition (see above) — emitted only when at least one
publisher has admitted a publish, so a publisher-free bus renders
byte-identical exposition to before.

## Topic registry

The operator-side catalog of known topics. Register the concrete topics
your producers publish to, with ownership metadata:

```ts
bus.registerTopic({
  topic: 'orders.created',
  owner: 'payments-svc',
  description: 'new orders, schema v3',
  schemaVersion: 3,
  tags: ['payments'],
});
bus.deregisterTopic('legacy.events'); // retired, but not forgotten
bus.getTopicInfo('orders.created'); // full record, incl. registeredAt/updatedAt
bus.topicCatalog(); // live registrations, in registration order
bus.topicCatalog({ includeDeregistered: true }); // tombstones appended
```

The registry is advisory, never a gate. Publishing to an unregistered
topic is not blocked — it is counted in
`getStats().unregisteredTopicPublishes` (also rendered as
`eventbus_unregistered_topic_publishes_total` in the Prometheus
exposition) and surfaced per occurrence on the opt-in
`EventBusOptions.onUnregisteredTopic` hook, so a typo'd topic name or a
producer that forgot to register shows up in monitoring instead of
silently flowing. The hook is error-isolated like `onAdmissionRejected`:
a throwing observer is swallowed and can never disturb the publish path.

Details:

- **Exact topics, not patterns** — the check runs on the concrete topic
  after namespace resolution; a publish to `orders.created` consults the
  registration for exactly `orders.created`.
- **Upsert semantics** — re-registering a live topic updates its metadata
  in place (returns `false`); a deregistered topic re-registers fresh
  (returns `true`), keeping its original `registeredAt`.
- **Tombstones** — deregistration keeps the last-known metadata marked
  `registered: false`, so `getTopicInfo` can still tell "never existed"
  apart from "used to exist".
- **Validation** — empty topics, empty owner/description/tags, and
  negative/non-finite `schemaVersion` throw `RangeError` before anything
  mutates.
- **Audit** — every registration change is recorded in the subscription
  lifecycle audit log (below) as `topic_register` / `topic_deregister`,
  with the caller-supplied actor (default `'system'`).

## Message signing (HMAC)

Opt-in publish-side authenticity for tamper-evident topics
(`src/signing.ts`, zero new dependencies — `node:crypto` only):

```ts
bus.setTopicSigningKey('payments.*', {
  keys: [{ kid: '2026-10-a', secret: '…' }],
  graceMs: 60_000,
});
// Rotate: newest key first; the old key keeps verifying for graceMs.
bus.setTopicSigningKey('payments.*', {
  keys: [
    { kid: '2026-10-b', secret: '…' },
    { kid: '2026-10-a', secret: '…' },
  ],
  graceMs: 60_000,
});

bus.subscribe('payments.charges', handler, { verifySignature: true });
```

How it works:

- **Signed base** — `topic + "\0" + seq + wireBytes`, HMAC-SHA256, hex
  digest. The wire bytes are the post-compression form: a compressed
  envelope's signature covers the ciphertext, and a verifying subscriber
  checks those same bytes *before* inflating. A payload with no JSON
  encoding cannot be signed — it travels unsigned and fails closed on
  verifying subscribers.
- **Envelope** — `{kid, sig}` rides the `BusMessage`; the durable log
  persists it and replay restores it, so a `verifySignature` subscriber
  with `resumeFromSeq` checks history against the same bytes the
  publisher signed. Delayed messages are signed at fan-out (due time),
  not at schedule time.
- **Verification** — opt-in per subscription, checked before any content
  processing (filter, throttle, queue). A failed check drops the message
  (never queued, never delivered), counts it in per-topic and global
  `signatureFailures`, and files a `signature_verification_failed`
  subscription audit event (`{ seq, kid? }`). The loss also surfaces as a
  sequence gap — a failed verification is a genuine loss, not a
  deliberate skip. Fail-closed: no signing rule, or no signature on the
  message, never verifies. A `kid` hint verifies against that single key
  with no fallback; without a hint the active keys are tried newest-first.
- **Rotation** — `keys` is newest-first; index 0 signs. Re-setting a rule
  retires displaced keys automatically (`retiredAtMs` stamped at
  rotation; already-retired keys keep their original timestamp —
  rotation is monotonic). A retired key verifies while
  `now - retiredAtMs <= graceMs` (inclusive). Keys whose grace window
  fully elapsed are pruned.
- **Rules** — match resolution mirrors `setTopicTtl`: an exact-topic rule
  wins over patterns, the earliest-registered matching pattern wins
  (independent matcher cache — never inflates `patternCacheSize`).
  `clearTopicSigningKey` removes a rule. Empty patterns, empty key lists,
  empty/duplicate/malformed kids (`1–64` chars of `[A-Za-z0-9_-]`), empty
  secrets, and negative `graceMs` throw `RangeError` at configuration
  time. Disabled by default.
- **Observability** — `getStats()` exposes per-topic `signatureFailures`
  and, when a rule matches, `signing: { keyIds, graceMs }` (key ids only —
  secrets never leave the rule); the global total is
  `BusStats.signatureFailures`. Prometheus:
  `eventbus_signature_failures_total` (global counter) and
  `eventbus_signature_failures_total{topic}` (per-topic).
- **Cross-bus semantics** — a bridge hop is a new admission. The bridge
  envelope carries a signature over `(topic, seq, canonical raw payload
  bytes)` — the raw bytes, because the receiver recomputes them from the
  carried payload (the envelope signature covers post-compression wire
  bytes, which the receiver cannot recompute). `receiveFromBridge`
  verifies it against the destination bus's own signing rules for the
  topic and rejects the envelope as `signature-verification-failed` when
  the check fails; a topic with no destination rule, or an envelope with
  no signature, skips the check. The destination then re-admits and
  re-signs under its own rules — signatures are per-bus and are never
  trusted across the hop. Forwarded messages (`receiveForward`) go through
  the same destination admission and re-signing; a verifying subscriber
  on the destination always checks the destination bus's signature.

### Publisher identity signing (EB-97)

Binds a message to the identity that published it (`src/signing.ts`,
same zero-dependency HMAC machinery):

```ts
bus.setPublisherSigningKey('billing-svc', {
  keys: [{ kid: '2026-10-a', secret: '…' }],
  graceMs: 60_000,
});
bus.subscribe('payments.charges', handler, { verifyPublisher: true });
bus.publish('payments.charges', { amount: 100 }, { publisherId: 'billing-svc' });
```

- **Signed base** — `topic + "\0" + seq + "\0" + publisherId + "\0" +
  wireBytes`: every variable-length field is NUL-terminated (the
  publisher id admits digits, so the separators keep every
  `(seq, publisherId, wireBytes)` triple unambiguous). Orthogonal to the
  per-topic signature — a message can carry both, integrity and identity
  independently.
- **Envelope** — `{kid, sig}` rides `BusMessage.publisherSignature`;
  the durable log persists it, replay and the retained snapshot restore
  it. Fail-closed everywhere: no `publisherId`, no registered key set,
  or no signature never verifies — an impersonated identity is dropped,
  counted in `signatureFailures`, and audited as
  `signature_verification_failed` with `detail.kind: 'publisher'`.
- **Rotation** — the WR-20/EB-84 convention verbatim: newest key signs,
  retired keys verify inside `graceMs` (inclusive), re-setting stamps
  rotation monotonically, fully-elapsed keys are pruned.
- **Redaction ordering** — `setTopicRedaction` (EB-68) runs at admission,
  before signing at fan-out: both signatures cover the redacted wire
  bytes, so redaction never breaks verification.

## Retained messages (MQTT-style)

Opt-in per-topic retained messages (`src/bus.ts`, EB-87 — real-time
systems / messaging): a `publish` carrying `retain: true` on a topic with
an enabled retained rule additionally stores the message as that topic's
retained message — the latest value a late-joining subscriber receives as
a snapshot at subscribe time, before any live message:

```ts
bus.setTopicRetained('config.*', true);
bus.publish('config.pricing', { tier: 'pro' }, { retain: true });
// …later, on another part of the system:
bus.subscribe('config.*', (msg) => {
  // First call: { topic: 'config.pricing', payload: { tier: 'pro' },
  //   seq, retained: true } — then live messages follow.
});
bus.publish('config.pricing', undefined, { retain: true }); // clears it
```

How it works:

- **Rules** — `setTopicRetained(topicPattern, enabled)` /
  `clearTopicRetained(topicPattern)`. The rule key is the exact string
  passed; match resolution mirrors `setTopicTtl`: an exact-topic rule
  wins over patterns, the earliest-registered matching pattern wins
  (independent matcher cache — never inflates `patternCacheSize`). With
  no matching rule a `retain: true` publish is an ordinary publish — the
  flag is silently ignored — so retained storage is strictly opt-in.
  Removing a rule stops future retained publishes from updating the
  table; already-stored retained messages stay deliverable. Empty
  patterns and non-boolean flags throw `RangeError` at configuration
  time.
- **Store** — one entry per exact (alias-resolved) topic; the latest
  retained publish wins, overwriting the previous value. A rejected or
  shed publish never touches the store: the message goes through the
  normal publish pipeline (admission, fan-out, durable log) and only an
  admitted publish updates retained state.
- **Clear** — publishing an empty payload with `retain: true` clears the
  topic's retained message (MQTT convention). "Empty" is measured in
  JSON-serialized bytes — the same yardstick the byte-budgeted queues
  use: `undefined` and values `JSON.stringify` cannot represent
  (functions, symbols, circular structures) serialize to zero bytes;
  `null` serializes to `null` (4 bytes) and is **not** empty.
- **Snapshot delivery** — a late subscriber first receives the latest
  retained message of every matching concrete topic (a wildcard
  subscription gets one per matching topic, in publish-time order),
  each carrying `retained: true` (absent on live messages). The snapshot
  is delivered synchronously inside `subscribe` — before any
  `resumeFromSeq` / `resumeFromTime` replay and before any live message —
  so snapshot-then-live order is guaranteed by construction, and a
  replay never redelivers a snapshotted message (the exact record is
  skipped in the replay pass). Snapshot deliveries bypass the
  backpressure queue entirely: no backpressure budget burned, no queue
  capacity occupied. Content filters, composite include/exclude
  patterns, and `verifySignature` apply exactly as on the live path;
  keyed/causal ordering gates are bypassed (the first live keyed/causal
  message seeds those gates fresh). Shadow mirrors never receive the
  snapshot (like replay, which shadows reject); consumer-group members
  each receive their own snapshot, mirroring per-member replay;
  reliable subscribers receive it as tracked at-least-once deliveries.
  Each snapshot delivery establishes the per-topic gap baseline like a
  first delivery — a late joiner never counts pre-subscription messages
  as gaps — and counts into `deliveredMessages` like replayed messages.
- **Envelope** — the stored envelope is the wire form: the compressed
  bytes when a compression rule applied, plus the HMAC `{kid, sig}` when
  a signing rule matched. Snapshot delivery re-registers the envelope
  for transparent inflation and verifies the signature against the same
  post-compression bytes the publisher signed — the existing
  signature-covers-wire-bytes semantics is unchanged.
- **Durability** — retained state is journaled as seq-0 retained records
  (the delayed-delivery convention: state, not messages — invisible to
  replay, never moving the sequence counter); a clear writes a tombstone
  so a restart never resurrects a cleared value. Restart recovery
  rebuilds the table from the log (latest record per topic wins;
  tombstoned or TTL-expired entries rebuild nothing). Retained records
  are never age-evicted by time-based retention — dropping them would
  lose the snapshot on restart.
- **TTL** — a retained message keeps the TTL deadline stamped at publish
  time: past its deadline it is pruned and never snapshotted (nor shown
  in stats) — the snapshot never resurrects an expired message.
- **Observability** — `getStats().retainedTopics` lists
  `{ topic, seq, at }` per topic holding a retained message (sorted by
  topic; TTL-expired entries filtered out); Prometheus:
  `eventbus_retained_topics` (gauge). Rule changes and publish-driven
  clears are recorded in the subscription audit log as
  `retained_rule_set` (`{ enabled }`), `retained_rule_cleared`, and
  `retained_cleared` (`{ via: 'publish' }`).
- **Scope notes** — retained state is node-local: bridge/cluster hops
  re-admit as ordinary publishes and do not propagate the retain flag.
  Only `publish` carries `retain`; batch/atomic/delayed publishes do
  not retain.

## Topic pause / resume (ops window)

`pauseTopic` / `resumeTopic` (`src/bus.ts`, EB-88 — real-time systems /
operability): an operator-level "stop" on a topic or topic pattern —
pause publishing during a deploy or incident, then resume:

```ts
bus.pauseTopic('orders.*'); // shed mode (default): publishes rejected outright
bus.publish('orders.eu', order); // -> 0, counted, subscribers see a gap
bus.resumeTopic('orders.*'); // -> true; publishing flows again

bus.pauseTopic('billing.*', { mode: 'queue' }); // needs durableLogDir
bus.publish('billing.eu', invoice); // -> 0, held in the durable-side buffer
bus.resumeTopic('billing.*'); // buffered messages fan out in original order
```

How it works:

- **Rules** — `pauseTopic(topicPattern, { mode })` / `resumeTopic(topicPattern)`.
  The rule key is the exact string passed; match resolution mirrors
  `setTopicTtl`: an exact-topic rule wins over patterns, the
  earliest-registered matching pattern wins (independent matcher cache —
  never inflates `patternCacheSize`). Re-setting a rule replaces its mode
  but keeps an already-held queue buffer (it drains on resume).
  `resumeTopic` on an unknown pattern is a no-op returning `false`.
  Empty patterns throw `RangeError`, non-string patterns `TypeError`,
  an unknown mode `RangeError`, and queue mode without
  `EventBusOptions.durableLogDir` `RangeError` — fail-fast at call time.
- **Modes** — `'shed'` (default) rejects publishes outright: counted in
  `TopicStats.pausedMessages` / `BusStats.pausedMessages`, surfaced on
  `onAdmissionRejected` with reason `'topic-paused'`, and consuming a
  sequence number so subscribers observe the loss as a gap (the
  rate-limit shed semantics, not the schema-rejection semantics).
  `'queue'` holds publishes in a durable-side buffer instead: no sequence
  number consumed, no schema/rate-limit budget burned — the message gets
  its full admission when it fans out at resume, in original publish
  order, each keeping the TTL deadline stamped at pause admission. A
  message whose deadline passed while buffered is counted expired at
  resume, never resurrected.
- **Gate order** — the pause check is the first gate in the publish
  admission pipeline: before the per-publisher rate limiter (EB-79), so a
  paused shed never burns a producer's budget, and before ACL / schema /
  the size cap. It applies uniformly: `publish`, `publishBatch` (per
  entry), `publishAtomic` (any paused entry — shed or queue — rejects the
  whole batch with reason `'topic-paused'`, since a queued message cannot
  be committed synchronously), `publishDelayed` (shed mode fails fast at
  schedule time; queue mode accepts the schedule and defers the due-time
  fan-out into the pause buffer while the pause is still active; a
  schedule accepted before the pause is shed at its due time in shed
  mode), and `publishIdempotent` (the pause check runs before the dedup
  claim, so a paused attempt never occupies a dedup slot; a retry buffered
  twice under queue mode still delivers once).
- **Durability** — buffered messages are persisted as seq-0 pause records
  (the delayed-delivery convention: state, not messages — invisible to
  replay, never moving the sequence counter), tombstoned as each message
  drains at resume or is dropped as expired. Restart recovery rebuilds
  the buffer and fans it out through full admission — pause rules
  themselves are runtime state and do not survive a restart, so the new
  process starts unpaused (the delayed-delivery "already due" precedent).
- **Observability** — `getStats().pausedTopics` lists the active rules
  (`{ topicPattern, mode, bufferedMessages }`); Prometheus:
  `eventbus_paused_messages_total` (counter),
  `eventbus_topic_paused_messages_total{topic}` (counter),
  `eventbus_paused_topics` (gauge). Pauses and resumes are recorded in
  the subscription audit log as `topic_paused` (`{ mode }`) and
  `topic_resumed` (`{ mode, drainedMessages }`).

## Cancelling delayed deliveries by topic

`cancelDelayedByTopic(topicPattern)` (`src/bus.ts`, EB-89) cancels every
pending delayed delivery scheduled for one topic — or for every concrete
topic matching a wildcard pattern — in a single call:

```ts
const eu = bus.publishDelayed('orders.eu', order, { delayMs: 60_000 });
const us = bus.publishDelayed('orders.us', order, { delayMs: 60_000 });
bus.cancelDelayedByTopic('orders.*'); // -> { cancelled: 2, delayIds: [eu, us] }
```

How it works:

- **Scope** — only pending, not-yet-fanned-out schedules are cancelled:
  entries that already fanned out, expired, or were individually cancelled
  are not counted. An exact topic matches directly; a pattern expands to
  every concrete topic with pending schedules that it matches (the same
  wildcard syntax as `subscribe`).
- **Result** — `{ cancelled, delayIds }`: the count and the cancelled
  schedule ids in schedule order. An empty result is `{ cancelled: 0,
  delayIds: [] }` — never a throw. Non-string patterns throw `TypeError`,
  empty patterns `RangeError`.
- **Durability** — each cancellation reuses `cancelDelayed`, so every
  cancelled schedule is tombstoned to the durable log (the delayed-delivery
  convention): a restart never resurrects them. Keyed schedules release
  their subscribers' per-key expectations, so a buffered successor is
  enqueued instead of hanging on a keySeq that will never fan out.
- **Observability** — recorded in the subscription audit log as
  `delayed_cancelled` (`{ cancelled, delayIds, topicPattern }`), including
  the empty case, so the operator action is always on record.

## Publish-side partition routing

`setTopicPartitions(topicPattern, n)` (`src/bus.ts`, EB-90) gives a topic
`n` publish-side partitions (default 1) — the sharding dimension a
partitioned consumer tier needs. Each `(topic, partition)` pair has an
independent sequence counter (the EB-13 per-topic seq, extended), so
`BusMessage.seq` is scoped to the message's partition and subscriber gap
detection baselines are tracked per `(topic, partition)`: partitions never
count each other's numbers as gaps.

```ts
bus.setTopicPartitions('orders', 8);
bus.publish('orders', order, { partitionKey: order.userId }); // sha256(key) mod 8
bus.publish('orders', order, { partition: 3 }); // explicit wins over partitionKey
```

How it works:

- **Selectors** — `PublishOptions.partitionKey` is a consistent-hash
  selector (`sha256(partitionKey) mod partitionCount`): the same key
  always lands on the same partition. `PublishOptions.partition` is an
  explicit partition number and wins when both are set. Both are opt-in;
  without either, every publish lands on partition 0 — a topic whose
  count stays at the default 1 behaves exactly as before.
- **Validation** — `partitionKey` must be a non-empty string;
  `partition` must be an integer `>= 0` and `<` the topic's partition
  count (on a single-partition topic the only valid value is `0`).
  `setTopicPartitions` rejects empty patterns and non-positive-integer
  counts. Everything throws `RangeError` before anything is mutated.
- **Rule matching** — mirrors the TTL convention: an exact-topic rule
  wins over patterns; between patterns the earliest-registered rule wins.
  `clearTopicPartitions(topicPattern)` removes a rule (already-published
  sequence numbers are untouched).
- **Keyed ordering** — partition is a seq/routing dimension only: the
  per-key publish-order mechanism (EB-36) is untouched, so one key keeps
  its publish order even when its messages land on different partitions.
- **Durability** — the assigned partition is stamped on the envelope
  (`BusMessage.partition`) and persisted on the durable-log record:
  replay restores it, and restart recovery rebuilds the per-(topic,
  partition) counters from the log — numbering never restarts and never
  reuses a number across restarts (old logs without partition data seed
  partition 0 from the topic max).
- **Observability** — `getStats()` exposes per-partition
  `publishedMessages`/`lastSeq` rows (only for topics with more than one
  partition; single-partition snapshots are byte-identical to before),
  Prometheus renders
  `eventbus_topic_partition_published_total{topic,partition}`, and rule
  changes are audited as `partition_rule_set` / `partition_rule_cleared`.
- **Scope note** — partition routing is a publish-side admission
  decision: delayed schedules, pause-buffered messages, and bridge/cluster
  ingress fan out as ordinary (partition-0) publishes.

## Admission dry-run: previewPublish

`bus.previewPublish(topic, payload, opts?)` (`src/bus.ts`, EB-108 —
backend engineering) walks the FULL publish-side admission gate sequence
with ZERO side effects and reports the verdict:

```ts
bus.previewPublish('orders.eu', order);
// -> { admitted: true }

bus.pauseTopic('orders.*');
bus.previewPublish('orders.eu', order);
// -> { admitted: false, reason: 'topic-paused', failedGate: 'topic-pause' }
```

The gate walk mirrors `publish` / `publishIdempotent` exactly: topic-alias
resolution (EB-44) → partition routing (EB-90) → topic pause (EB-88) →
per-publisher rate limit (EB-79) → ACL (EB-41) → schema validation incl.
versioned schemas (EB-20/EB-86) → per-message size cap (EB-64) →
per-topic rate limit (EB-19). With a non-empty `opts.messageId` the
idempotent-dedup suppression gate (EB-27) is consulted where
`publishIdempotent` consults it (after the ACL, before the remaining
gates). The verdict is computed by the same pure shadow decider
(`admissionVerdict`) that `publishAtomic`'s phase-1 admission uses, so a
preview and the real path agree gate-for-gate. One intentional exception:
adaptive sampling (EB-38) is not previewed — the gate is probabilistic,
and a preview cannot draw from the injected random stream without
disturbing the sequence of real publish decisions, so a preview on a
sampled topic reports "admitted" whenever every other gate passes.

Zero side effects means: no sequence number consumed (no downstream
gap), no durable-log writes, no rate-limit tokens taken (budgets are
peeked read-only — repeated previews never drain anything), no enqueue,
no stats counters moved, no subscription-audit events, no idempotency
slot claimed, and no `onAdmissionRejected` callback invocation. Option
validation throws exactly like `publish` (`RangeError` / `TypeError` /
`NamespaceDeniedError`); an out-of-range explicit partition throws
`RangeError`, never a rejection, like `publish`.

Intentional divergences (documented in `src/bus.ts`): a preview never
claims the `(topic, messageId)` dedup slot — a real `publishIdempotent`
after the preview still admits once; chaos faults (EB-103) are not
evaluated (they are one-shot injections); the advisory topic-registry
check (EB-77) is skipped; barrier cut markers (`opts.barrier`) take the
slim alias→ACL admission path, mirroring `fanOutBarrier`.

## Publisher confirmations: publishWithConfirmation

`bus.publishWithConfirmation(topic, payload, opts?)`
(`src/bus.ts`, EB-125 — backend engineering) runs the EXACT `publish`
pipeline — chaos gate, option validation, namespace resolution, circuit
breaker, `fanOut` admission, flush scheduling — and resolves with a
receipt saying what admission decided, Kafka-`acks` style:

```ts
const ok = await bus.publishWithConfirmation('orders.eu', order);
// -> { admitted: true, topic: 'orders.eu', seq: 41,
//      admittedAtMs: 1728981234567,
//      payloadSha256: '9f2c…' }

bus.setTopicSchema('orders.eu', (p) => p.total > 0);
const no = await bus.publishWithConfirmation('orders.eu', { total: -1 });
// -> { admitted: false, topic: 'orders.eu', reason: 'schema',
//      rejectedAtMs: 1728981234567 }
```

The receipt is a discriminated union on `admitted`. `topic` is the fully
resolved concrete topic (namespace + alias resolution) — the topic the
sequence number and the durable-log record belong to. `seq` is the
per-(topic, partition) sequence number stamped at fan-out. `reason`
reuses the `onAdmissionRejected` vocabulary verbatim (`'acl'`,
`'schema'`, `'message-too-big'`, `'payload-too-large'`,
`'rate-limit'`, `'publisher-rate-limit'`, `'alias-retired'`,
`'topic-paused'`) — no new spellings — and the
rejection's zero-side-effect contract matches `publish` exactly: no
sequence number consumed, nothing written to the durable log, no
rate-limit budget burned (the `'rate-limit'` and shed-mode
`'topic-paused'` sheds consume a sequence number, exactly like
`publish`, so subscribers observe the same gap). Invalid arguments
(empty topic, illegal options) throw `RangeError` fail-fast,
synchronously, exactly like `publish`.

`payloadSha256` is the SHA-256 hex (`node:crypto`, zero new
dependencies) of the admitted application-payload bytes: post
admission-time redaction (EB-68) — the exact bytes the durable log
persists for the payload and subscribers receive — and pre
compression (EB-22) / encryption (EB-80), so the publisher can
reproduce the digest from its own bytes (`sha256(JSON.stringify(payload))`
for JSON; raw bytes for binary payloads, EB-92). Payloads with no JSON
encoding hash as the empty byte string.

Opt-in `waitForDurable: true` gives Kafka-`acks=all` semantics: the
receipt resolves only after the message's durable-log record has been
appended. The bus's durable-log append is synchronous (the record is
handed to the OS before `append` returns), so this never sleeps or
polls — the receipt simply resolves after the synchronous publish, at
which point the record is guaranteed on disk. Without a durable log
configured there is nothing to persist and the publish is trivially
durable. (Bus durability is OS-handoff, not fsync-per-message: a power
loss can still lose whatever the OS had not flushed.)

Notes: a topic paused in queue mode (EB-88) reports
`{ admitted: false, reason: 'topic-paused' }` — the message is buffered
for resume, not rejected, and no second receipt fires at resume;
barrier cut markers (`opts.barrier`) confirm with `seq: 0`
(control-plane, never durably logged).

## Declarative configuration snapshots

`exportBusConfig()` / `applyConfig(snapshot, { dryRun? })` /
`detectConfigDrift([baseline])` (`src/config.ts`, `src/bus.ts`, EB-91)
treat the bus's configuration as data: ACL rules, namespace
registrations, per-publisher rate limits, durable-log retention, and the
per-topic rule tables (TTL, retained, publish rate limit, max
subscribers, allowed lateness, compression, signing-key metadata, alias,
route, partitions) export as one JSON-safe snapshot (`v: 1`).

```ts
const snapshot = bus.exportBusConfig();
JSON.stringify(snapshot); // check into version control, ship to another process

// Preview a desired state without touching anything:
const { changes } = bus.applyConfig(desired, { dryRun: true });

// Apply it: every change goes through the same setters the imperative
// API uses, so all validation still applies.
const { applied } = bus.applyConfig(desired);

// Did the runtime drift since the last apply?
const drift = bus.detectConfigDrift(); // [{ path, expected, actual }]
```

How it works:

- **Diff, then apply** — the snapshot is validated fail-fast
  (`TypeError`/`RangeError` on malformed input, before anything is
  mutated), diffed against the live configuration, and each granular
  change (`topicRules["market.**"].ttlMs`, `namespaces["t1"]`, `acl`, …)
  is applied through the existing setters.
- **Atomic rollback** — if any change throws mid-apply, every
  already-applied change is rolled back to the pre-apply state and a
  `ConfigApplyError` is thrown carrying the failed path, the applied
  paths, and whether the rollback succeeded. The bus is never left
  half-configured. Applies and rollbacks are audited as
  `config_applied` / `config_rolled_back` in the subscription audit log.
- **Fail-closed restores** — a snapshot never carries enough to rebuild
  everything, so changing these via `applyConfig` is refused rather
  than guessed: signing keys (snapshots carry key ids only, never
  secrets — restore with `setTopicSigningKey`), compression dictionaries
  (id only — re-register bytes with `setTopicCompression`), and routes
  whose live registration carries a predicate. Exporting is always safe:
  key material is never exported.
- **Drift detection** — `applyConfig` stores the applied snapshot as
  the baseline; `detectConfigDrift()` diffs live state against it
  (pass an explicit snapshot to compare against any other baseline).
  Without a baseline it throws — there is nothing to compare against.
- **Scope note** — schema validators and route predicates are
  functions and cannot be serialized; per-subscriber runtime state
  (queues, health, offsets) is operational state, not configuration.
  Neither is snapshotted.

## Binary payload passthrough

`publish(topic, bytes, { binary: true })` (`src/bus.ts`, EB-92) sends
raw bytes (`Uint8Array`, or an `ArrayBuffer` which is wrapped in a
zero-copy view) through the whole pipeline with no JSON serialization —
for protocols, codecs, and media frames that are already bytes. Anything
else as payload with `binary: true` throws `TypeError` before anything
is mutated.

```ts
const frame = new Uint8Array([0xde, 0xad, 0xbe, 0xef]);
bus.publish('camera.frame', frame, { binary: true });
bus.subscribe('camera.**', (msg) => {
  msg.payload instanceof Uint8Array; // true — the same bytes
});
```

How it works:

- **Zero-copy in process** — the subscriber receives the published
  buffer itself (no copy, no `JSON.stringify`); subscribers that do not
  opt in keep the classic JSON behavior, and a `Uint8Array` published
  *without* `binary: true` still serializes as JSON exactly as before.
- **Byte-true admission** — the max-message-bytes cap (EB-64) and the
  compression threshold (EB-22) measure wire bytes, not JSON length;
  the schema validator (EB-20) receives the `Uint8Array`, so a binary
  topic's validator is written against bytes; redaction (EB-68) is
  skipped (bytes have no fields).
- **Signing covers wire bytes** — the HMAC topic signature (EB-84)
  covers the raw bytes (post-compression when compressed), and
  `verifySignature` subscribers check those same bytes; unsigned binary
  messages fail closed like unsigned JSON ones.
- **Durable log v2** — binary messages persist as `v: 2` lines carrying
  `payload_b64` (base64 of the wire bytes, with an explicit version
  marker), so a JSON reader never mistakes the bytes for a value.
  Restart recovery and `resumeFromSeq` replay restore raw bytes; a
  binary message compressed at publish inflates back to bytes, never
  `JSON.parse`d. Corrupt base64 reads as a corrupt line, not as
  truncated bytes.
- **Scope note** — only `publish` / `publishIdempotent` accept
  `binary`; batch, atomic, and delayed publishes do not carry
  per-message binary flags. Binary messages survive topic routes,
  cross-bus forwards, and the pause buffer as bytes. The bridge
  transport does not encode binary payloads (a JSON-based transport
  would need its own framing) — binary is an in-process feature.
  Auto-trained compression dictionaries (EB-60) skip binary topics;
  use an explicit dictionary via `setTopicCompression` instead.

## Subscription lifecycle audit log

Every subscription change is recorded in an append-only audit log —
`subscribe` / `unsubscribe` / pattern hot-updates (`updateSubscriptionPattern`)
/ consumer-group join-leave / group rebalances / topic-registry changes:

```ts
const sub = bus.subscribe('orders.*', handler, { actor: 'checkout-svc' });
bus.updateSubscriptionPattern(sub.id, 'orders.v2.*');
sub.unsubscribe();

bus.getSubscriptionAudit({ action: ['subscribe', 'unsubscribe'] });
// [{ v:1, at:…, action:'subscribe', subId:'sub-1', pattern:'orders.*', actor:'checkout-svc' },
//  { v:1, at:…, action:'unsubscribe', subId:'sub-1', pattern:'orders.v2.*', actor:'system' }]
```

This is the complement of `onAuthzDenied`, which only fires for
*rejected* authorization decisions: the audit log records the normal,
successful lifecycle the denial hook deliberately ignores.

Details:

- **Actor** — `SubscribeOptions.actor` names who made the change
  (service, operator); defaults to `'system'` (lease/idle expiry, pattern
  updates, group leave). An empty/non-string actor throws `RangeError`
  before anything registers.
- **Query** — `getSubscriptionAudit({ since, until, action, limit })`
  filters the bounded in-memory index (default 1000 entries, oldest
  evicted; `subscriptionAuditMaxEntries` tunes it). `since` is inclusive,
  `until` exclusive, `limit` keeps the most recent matches, oldest first.
- **Persistence** — `EventBusOptions.subscriptionAuditDir` opts into daily
  `audit-YYYY-MM-DD.jsonl` files (one per UTC day, created when missing);
  without it the in-memory index still records everything, but a restart
  loses the history. On startup the index back-fills from existing files,
  skipping corrupt lines — a damaged audit directory never prevents the
  bus from starting.
- **Error isolation** — a failed disk append is counted, never thrown
  into the subscribe/unsubscribe path; a throwing audit hook cannot
  exist, because there is no hook — only the log.
- **Detail payloads** — `update_pattern` carries `{ from }`;
  `rebalance` carries `{ trigger, memberId, members }`;
  topic-registry changes carry the registration metadata;
  `signature_verification_failed` carries `{ seq, kid? }`;
  `retention_updated` carries `{ from, to }` (null when unset);
  `retained_rule_set` carries `{ enabled }`, `retained_cleared` carries
  `{ via: 'publish' }`; `partition_rule_set` carries `{ partitions }`;
  `barrier_dropped` carries `{ epoch, lastEpoch }` (EB-98).

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
| `eventbus_deprecated_schema_messages_total` | counter | `deprecatedSchemaVersions` — publishes admitted under a superseded-but-graced schema version (EB-86) |
| `eventbus_late_messages_total` | counter | `lateMessages` — admitted messages older than their topic's event-time watermark (EB-59; still delivered) |
| `eventbus_rate_limited_messages_total` | counter | `rateLimitedMessages` — per-topic rate-limit sheds |
| `eventbus_circuit_broken_messages_total` | counter | `circuitBrokenMessages` — publishes refused by publish circuit breakers (EB-116): the breaker was open, so `publish` threw `CircuitBreakerOpenError` (fast-fail) |
| `eventbus_circuit_breaker_trips_total` | counter | `circuitBreakerTrips` — times a publish circuit breaker opened (EB-116) |
| `eventbus_duplicate_messages_total` | counter | `duplicateMessages` — idempotent-publish suppressions |
| `eventbus_unregistered_topic_publishes_total` | counter | `unregisteredTopicPublishes` — publishes to topics with no live catalog registration (advisory; delivered normally) |
| `eventbus_filtered_messages_total` | counter | `filteredMessages` — subscriber content-filter skips |
| `eventbus_signature_failures_total` | counter | `signatureFailures` — messages discarded by `verifySignature` subscribers (missing/invalid HMAC signature, EB-84) |
| `eventbus_signature_failures_total{topic}` | counter | per-topic `signatureFailures` (EB-84) |
| `eventbus_mirror_conflicts_total{topic}` | counter | per-topic mirror write conflicts — stale writes dropped by per-key last-writer-wins arbitration on the mirror receive side (EB-131); only topics with ≥1 conflict carry a series |
| `eventbus_retained_topics` | gauge | `retainedTopics.length` — topics currently holding an MQTT-style retained message (EB-87) |
| `eventbus_dedup_dropped_messages_total` | counter | `dedupDropped` — subscriber exactly-once dedup suppressions |
| `eventbus_dead_lettered_messages_total` | counter | `deadLetteredMessages` |
| `eventbus_diagnostic_events_total` | counter | `diagnosticEvents` — admitted DLQ diagnostic events |
| `eventbus_sequence_gaps_total` | counter | `sequenceGaps` |
| `eventbus_expiring_soon_alerts_total` | counter | `expiringSoonAlerts` — TTL expiring-soon warnings fired for subscriber queue heads (EB-110) |
| `eventbus_consumer_lag_slo_breach_total` | counter | `lagSloBreaches` — consumer-group lag SLO breach episodes fired (EB-112) |
| `eventbus_keyed_reordered_messages_total` | counter | `keyedReorderedMessages` — keyed messages held in per-(subscriber, key) reorder buffers |
| `eventbus_causal_buffer_dropped_messages_total` | counter | `causalBuffer.droppedMessages` — causal messages dropped from full per-source reorder buffers (drop-oldest anti-deadlock) |
| `eventbus_causal_buffer_regressed_messages_total` | counter | `causalBuffer.regressedMessages` — causal messages delivered immediately on clock regression |
| `eventbus_topic_published_messages_total{topic}` | counter | per-topic `publishedMessages` |
| `eventbus_topic_circuit_broken_messages_total{topic}` | counter | per-topic `circuitBrokenMessages` (EB-116) |
| `eventbus_topic_partition_published_total{topic,partition}` | counter | per-partition `publishedMessages` (EB-90); only emitted for topics with more than one configured partition |
| `eventbus_topic_late_messages_total{topic}` | counter | per-topic `lateMessages` (EB-59) |
| `eventbus_topic_event_time_watermark{topic}` | gauge | per-topic event-time watermark in epoch ms (EB-59); only emitted for topics with at least one event-time publish |
| `eventbus_subscribers` | gauge | `totalSubscribers` |
| `eventbus_unacked_deliveries` | gauge | `unackedDeliveries` |
| `eventbus_throttled_subscribers` | gauge | `throttledSubscribers` |
| `eventbus_degraded_subscribers` | gauge | `degradedSubscribers` |
| `eventbus_paused_subscribers` | gauge | `pausedSubscribers` — operator-paused subscriptions (EB-113) |
| `eventbus_circuit_breakers_open` | gauge | publish circuit breakers currently open (fast-failing; EB-116) |
| `eventbus_shaped_subscribers` | gauge | `shapedSubscribers` |
| `eventbus_pending_delayed` | gauge | `pendingDelayed` |
| `eventbus_queue_bytes` | gauge | `queueBytes` — buffered payload bytes across subscriber queues |
| `eventbus_causal_buffer_depth` | gauge | `causalBuffer.depth` — messages currently held in per-(subscriber, source) causal reorder buffers |
| `eventbus_topic_subscribers{topic}` | gauge | per-topic fan-out width (subscribers matched by the most recent publish) |
| `eventbus_delivery_latency_ms{quantile,subscriber,pattern}` | gauge | per-subscriber enqueue→delivery queue-dwell p50/p95/p99 (`quantile` = "0.5"/"0.95"/"0.99"); only `deliveryLatency`-tracked subscriptions |
| `eventbus_delivery_latency_samples{subscriber,pattern}` | gauge | samples in the subscriber's latency window |
| `eventbus_delayed_schedule_skew_ms{quantile}` | gauge | delayed-delivery schedule skew: actual fan-out time minus due time (`deliverAt`), p50/p99 (`quantile` = "0.5"/"0.99"); only `scheduleSkew`-enabled buses, only once at least one schedule has been sampled (EB-136) |
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
| `eventbus_publisher_published_messages_total{publisher}` | counter | admitted publishes per publisher identity (`PublishOptions.publisherId`); only emitted when at least one publisher has admitted a publish |
| `eventbus_nack_reasons_total{subscriber,reason}` | counter | per-subscriber redelivery counts by trigger (`reason` = `handler_throw` / `explicit_nack` / `ack_timeout`); one series per non-zero class, so `getStats().nackReasons` rows with zero counts emit nothing (EB-129) |

The per-publisher series grow with the distinct publisher identities ever
admitted — the same bound as `BusStats.publishers` — so a bus admitting
millions of ad-hoc identities grows the series count. Identities come from
the producer, so this is bounded by the application's own producer space;
anonymous publishes add no series at all.

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

## Barrier cut markers

A barrier (`publish(topic, payload, { barrier: { epoch } })`, EB-98) is a
consistency cut point for barrier-opted-in subscribers
(`subscribe(pattern, handler, { barrier: true })`) — the primitive behind
config switches and snapshot boundaries: "everything the subscriber saw
before this marker is pre-cut; everything after is post-cut."

Protocol, per opted-in subscriber:

1. The first barrier with an epoch strictly higher than any seen before
   arms the gate. The barrier itself is delivered to the handler as a
   control message — `msg.barrier.epoch` on the envelope, `seq` 0, the
   published payload carried verbatim as the cut metadata.
2. Every message arriving after the barrier in *delivery order* is held in
   a pre-queue side buffer (no backpressure budget, no filter/throttle
   evaluated yet) instead of being enqueued.
3. When the barrier is handed to the handler (drained), everything held
   behind it is released at once — re-injected through the normal
   pipeline in arrival order — and the gate disarms.
4. Barrier epochs must be monotonically increasing per subscriber. An
   out-of-order barrier (epoch at or below the highest seen) is discarded
   — never delivered, never arming — counted in
   `getStats().barrier.droppedBarriers`, and recorded in the subscription
   audit log as `barrier_dropped` with `{ epoch, lastEpoch }`.

The cut orders the delivery stream, not publish time. A barrier is
control-plane, not business data: it consumes no per-topic sequence
number (subscribers see no gap; gap detection skips barrier messages),
passes the ACL but skips schema validation, rate limiting, compression,
signing, TTL and the durable log, and never crosses the bridge. The
barrier control message bypasses the subscriber's content filter and
composite matching — a filter must never swallow the cut marker and wedge
the hold behind it — but it is subject to backpressure: if the queue
sheds the barrier, the hold releases fail-open immediately (abandoning
the cut beats abandoning the data, and the stream was already lossy).
Subscribers that did not opt in never see barrier messages at all.

Combination matrix:

- **Causal clocks (EB-62)** — the barrier gate runs first, then the
  causal gate. A held message re-enters the pipeline after the barrier
  gate at release time, so happens-before order is preserved on both
  sides of the cut; the cut marker itself is never reordered behind data.
  (Per-key ordering layers the same way: barrier, then causal, then
  keyed.)
- **Delayed delivery (EB-24)** — a delayed schedule fans out through the
  normal pipeline at its due time, so a message whose due time falls
  after the barrier armed arrives after the barrier in delivery order and
  is held behind it — even when it was scheduled earlier. A barrier
  cannot itself be delayed (`publishDelayed` has no barrier option): the
  cut arms at fan-out time, and a future-dated cut would arm at an
  unpredictable delivery-stream position.
- **Retained messages (EB-87)** — the retained snapshot is handed to a
  late joiner synchronously at subscribe time, before any barrier can
  arm, so it bypasses the barrier gate. A barrier publish ignores the
  `retain` flag: a cut marker is not a value snapshot and never becomes
  one.
- **Durable-log replay (EB-17/EB-42)** — barriers are never written to
  the durable log (replay restores data, not live coordination cuts), so
  a resuming subscriber replays data messages only, with their original
  seqs, and the replay path bypasses the barrier gate. Live barriers arm
  only on live barrier messages, after the replay completes.
- **Consumer groups** — a barrier routes like any message: in a
  competing group exactly one member (the assignee) receives it and arms
  its own hold. It carries no business seq, so it never moves the group's
  committed offsets.
- **Shadow subscriptions** — the mirror carries barrier markers like any
  message, but the hold/release gate does not apply to shadows.

Every hold terminates: the side buffer is bounded (`maxHeldMessages`,
default 1000). On overflow the gate fails open — everything held is
released at once, the barrier is disarmed, and the abandonment is
counted in `getStats().barrier.overflowedHolds`. `getStats().barrier`
reports `{ publishedBarriers, droppedBarriers, heldMessages,
releasedMessages, overflowedHolds }`. Invalid epochs (non-integer,
negative) and hold bounds throw `RangeError` fail-fast, before anything
is mutated.

## WebSocket gateway (EB-99)

`src/gateway-ws.ts` exposes an `EventBus` over RFC 6455 WebSocket so remote
processes — other languages, browsers, edge workers — can publish and
subscribe without embedding the bus. Zero new dependencies (`node:net` +
`node:crypto` only); the server-side frame codec is adapted from the
client-side codec in webhook-relay-ts (`src/websocket.ts`, WR-45).

```ts
import { EventBus } from './src/bus.ts';
import { GatewayServer } from './src/gateway-ws.ts';

const bus = new EventBus({ durableLogDir: './data' });
const gw = new GatewayServer(bus, { port: 8080, authToken: 's3cr3t' });
const port = await gw.listen(); // resolves with the bound port
// ... later:
await gw.close(); // drops connections (unsubscribing them) and stops
```

Wire protocol — JSON text frames, plus binary frames (EB-109):

- client → server: `subscribe{pattern,opts}` / `unsubscribe{subId}` /
  `publish{topic,payload,opts}` / `publishBatch{entries}` / `ack{deliveryId}` /
  `nack{deliveryId}` / `ping`
- server → client: `subscribed{subId}` / `unsubscribed` / `message{...}` /
  `published{delivered,duplicate?}` / `pong` / `error{code,message}`
- client → server (binary): a binary frame is
  `[u32 BE headerLen][UTF-8 JSON header][raw payload bytes]`. The only
  binary frame type is `publish` — header `{type:'publish', id?, topic,
  opts?}`, body = raw bytes — published with `binary: true` (EB-92), so
  the full admission pipeline runs on the wire bytes. Answered with the
  text `published` frame.
- server → client (binary): for subscriptions whose `subscribe` carried
  `opts.binary: true`, each delivery is a binary frame with header
  `{type:'message', subId, topic, seq, encoding: 'binary'|'json',
  deliveryId?, redeliveries?}`. `encoding: 'binary'` means the message was
  a binary (EB-92) publish and the body is its raw wire bytes;
  `encoding: 'json'` means any other payload, UTF-8 JSON-encoded.

Semantics:

- **Full admission on publish.** Gateway publishes go through `publish`
  (or `publishIdempotent` when `opts.messageId` is set, EB-27), so ACL
  (EB-41), schema (EB-20), rate-limit (EB-19), TTL, and dedup all apply
  exactly as for in-process publishers. `publishBatch` maps to
  `publishBatch` (EB-03). A refused publish replies `delivered: 0`.
- **ACL on subscribe.** Unauthorized patterns get an error frame with
  `code: 'subscribe_denied'` and nothing registers; the connection stays
  usable.
- **At-least-once over the wire.** `subscribe` with `opts.reliable`
  routes through `subscribeReliable` (EB-11): each message carries a
  `deliveryId` the client must `ack` / `nack`. Nack and ack-timeout
  redeliver through the bus's normal redelivery path; unknown delivery
  ids are ignored (stale acks are normal).
- **Binary payloads over the wire (EB-109).** A binary `publish` goes
  through the exact same admission as a text publish — schema validators
  see the raw bytes (EB-20), the byte-size cap (EB-64) and compression
  threshold (EB-22) measure wire bytes, the HMAC topic signature (EB-84)
  covers those bytes, and `opts.messageId` still routes through
  idempotent dedup. A binary subscriber receives binary publishes as raw
  bytes; non-binary publishes arrive JSON-encoded (`encoding: 'json'` in
  the binary header), so binary subscribers on mixed topics never break.
  Backpressure treats binary frames like text frames: `maxPendingBytes`
  counts their byte size, and overflowing binary frames are dropped and
  counted in `droppedFrames` the same way.
- **Reconnect resume.** `opts` is a `SubscribeOptions` passthrough, so
  `resumeFromSeq` / `resumeFromTime` (EB-17/EB-42) and namespaces (EB-52)
  work for reconnecting clients. Function-valued options (filters, hooks)
  cannot cross the wire.
- **Retained snapshots keep wire order.** A synchronous retained snapshot
  (EB-87) is buffered until the subscription registers, so the client
  sees snapshot messages before the `subscribed` ack, then live messages.
- **Auth (opt-in).** With `authToken` set, the handshake must carry
  `Authorization: Bearer <token>` (constant-time compare); otherwise the
  server answers 401 and drops the connection.
- **Backpressure.** While a connection's socket buffer is past
  `sendHighWaterMarkBytes` (default 256 KiB) the gateway pauses pushing
  frames and queues them locally, bounded by `maxPendingBytes` (default
  1 MiB). Past the bound the newest frame is dropped and counted in
  `droppedFrames` — the gateway sheds load instead of growing memory.
  `gw.getStats()` reports
  `{connections, framesReceived, framesSent, droppedFrames, authFailures,
  protocolErrors}`.

Invalid option values throw `RangeError` fail-fast. Inbound frames larger
than `maxFrameBytes` (default 1 MiB) close the connection with 1009 —
this bounds binary frames too. Malformed binary frames (bad header
length, unparseable header) get `bad_frame` error frames without dropping
the connection; malformed JSON text frames and unknown frame types behave
the same.

## Atomic batch subscription (EB-100)

`bus.subscribeMany(entries, commonOpts?)` registers many independent
subscriptions atomically: either every entry registers or none does. On
the first failure — invalid pattern, ACL denial (EB-41), namespace
violation (EB-52), bad options — every already-registered entry is
unsubscribed and the original error is rethrown, leaving the bus exactly
as before the call.

```ts
const subs = bus.subscribeMany(
  [
    { pattern: 'orders.**', handler: onOrder },
    { pattern: 'payments.**', handler: onPayment, opts: { queueSize: 500 } },
    // Consumer-group batch join (EB-16): same group, competing member.
    { pattern: 'jobs.**', handler: onJob, opts: { group: 'workers' } },
  ],
  { filter: (payload) => payload !== null }, // commonOpts, entry opts win
);
// subs: Subscription[] in registration order
```

Notes:

- Each entry is an **independent** subscription (own handler, queue,
  backpressure policy, lifecycle). Contrast with composite patterns
  (EB-70, `SubscribeOptions.patterns`): a composite is one subscription
  matching several patterns with OR/AND/exclusion semantics; a
  `subscribeMany` entry is a separate subscription — the batch is only an
  atomic registration mechanism.
- Per-entry `opts` are shallow-merged over `commonOpts` (entry wins).
  `opts.group` routes the entry through `subscribeToGroup`; `opts.batch`
  entries use the batch-delivery overload. Shadow (EB-63) and namespace
  (EB-52) combination rules apply per entry exactly as in `subscribe`.
- An empty entries array throws `RangeError`; non-function handlers and
  non-object opts throw `TypeError` — all before anything registers.

## Health doctor (EB-101)

`bus.exportBusHealth()` returns a one-shot structured diagnosis of the
bus — the observability entry point for operators:

```ts
const report = bus.exportBusHealth();
// { status: 'healthy' | 'degraded' | 'critical',
//   at: <bus-clock ms>,
//   checks: [{ name, status: 'pass'|'warn'|'fail', detail, suggestion }] }
```

The overall status is `healthy` when every check passes, `degraded` when
any check warns, `critical` when any check fails. Each check carries its
evidence (`detail`, with numbers) and an operator action (`suggestion`).
The call is purely read-only: no publishes, no durable-log writes, no
state changes, no callbacks fired.

Checks, in order:

- `slow-subscribers` — top slowest subscribers by p99 queue dwell
  (EB-31) above `slowSubscriberP99Ms` (default 1000ms), plus handler
  p99 breaching its own latency-SLO threshold (EB-53, reused
  per-subscriber config).
- `subscriber-lag` — live lag watermarks (EB-35) at or above the
  subscriber's own `lagMonitor.thresholdMs` when configured, else the
  doctor fallback `lagWatermarkDefaultMs` (default 30000ms; EB-35
  leaves the threshold absent by design, so the doctor owns the
  fallback).
- `dead-letter-backlog` — messages held in subscriber DLQs (warn);
  a DLQ at `maxEntries` is `fail` (oldest entries are being evicted).
- `consumer-group-lag` — group-lag rows (EB-48) above the effective
  threshold (per-group override via `setGroupLagThreshold`, else the
  bus default of 100).
- `topic-aliases` — re-verifies the alias table is acyclic
  (defense-in-depth; EB-44 already prevents cycles at registration).
- `namespaces` — warns on registered-but-unused namespaces (no
  topics, subscribers, or published traffic).
- `durable-log` — corrupt segments/archives isolated by checksum
  (EB-94 sidecar) fail; malformed lines skipped during recovery warn.
- `partition-coverage` — partitioned consumer groups (EB-90) with
  fewer live members than configured partitions warn. Note: this bus
  has no quorum concept (the word "quorum" appears nowhere in the
  codebase), so EB-101's quorum item is honestly mapped to partition
  coverage — partitions sharing consumers instead of dedicated owners —
  rather than an invented consensus layer.

Tuning: `bus.exportBusHealth({ slowSubscriberP99Ms, lagWatermarkDefaultMs })`
overrides the two doctor-owned defaults; every other threshold is
reused from the module that owns it.

## Transactional outbox relay (EB-102)

The transactional outbox closes the dual-write gap between a business
database and the bus: the application writes its domain rows **and** one
outbox row in the same DB transaction, so a crash can never leave "the
order was saved but the event was never published". A relay drains the
outbox table into the bus.

```ts
import { EventBus } from './src/bus.ts';
import { MemoryOutboxStore } from './src/outbox.ts';

const bus = new EventBus();
const store = new MemoryOutboxStore(); // or your DB-backed OutboxStore
const relay = bus.attachOutboxRelay(store, { pollIntervalMs: 100 });
relay.start();

// ... in the business transaction:
//   INSERT INTO orders ... ;
//   INSERT INTO outbox (topic, payload) VALUES ('orders.created', '{"id": 7}');
```

One drain cycle is one `publishAtomic` batch: subscribers see a batch's
messages across topics all at once or not at all — never a torn
multi-topic write. The cursor advances only after the batch commits, so
a crash between commit and ack replays the same records after
`store.recover()`, and every record carries a stable dedup identity
(`messageId = "outbox:<seq>"`, EB-27) that suppresses the replay for
subscribers with `deduplicateMessages` enabled — with a `consumerId` and
a durable log the window is rehydrated across restarts, so there is no
double-delivery after a crash. Without `deduplicateMessages` the relay
is at-least-once after an ack loss: consumers stay idempotent.

A rejected batch (schema, rate-limit, ACL…) advances nothing: the
cursor stays on the poison record, `onRelayError` fires, and the relay
keeps the backlog until the operator fixes the store or advances the
cursor manually — a poison record is never silently skipped.

Lag is observable: `bus.getStats()` exposes `outboxPending`,
`outboxLagMs` (age of the oldest unrelayed record), `outboxCursor` and
`outboxRelayed`, and the Prometheus exposition carries
`eventbus_outbox_pending`, `eventbus_outbox_lag_ms` and
`eventbus_outbox_relayed_total` — alert on a climbing lag with a stuck
cursor.

SQL shape for a Postgres-backed store:

```sql
CREATE TABLE outbox (
  seq       BIGSERIAL PRIMARY KEY,
  topic     TEXT NOT NULL,
  payload   JSONB NOT NULL,
  key       TEXT,              -- optional: keyed ordering / compaction key
  headers   JSONB,             -- optional: traceparent, audit labels
  at_ms     BIGINT NOT NULL    -- wall-clock ms at append
);
-- The relay's durable cursor; ackCursor updates this in its own short
-- transaction (and may DELETE FROM outbox WHERE seq <= cursor).
CREATE TABLE outbox_cursor (
  id     INT PRIMARY KEY CHECK (id = 1),
  cursor BIGINT NOT NULL DEFAULT 0
);
```

## Static consumer-group membership (EB-104)

Like Kafka's static group membership, a consumer can join a group under a
stable identity. Pass `instanceId` in `GroupSubscribeOptions` (non-empty,
must not collide with auto-generated `sub-N` ids) and the bus keys the
roster by it instead of the ephemeral subscription id:

```ts
const member = bus.subscribeToGroup('orders', 'orders.events', onOrder, {
  instanceId: 'order-processor-3',
  sessionTimeoutMs: 30_000, // default 30s; must be a finite number > 0
});
```

When a static member unsubscribes, its slot is not released immediately:
it enters a **limbo** for `sessionTimeoutMs` — the roster still names it,
no rebalance fires, and its partition assignment stays frozen. Messages
published during the window route to the remaining live members (a limbo
member is never drained to). If the same `instanceId` rejoins before the
window expires, it reclaims its slot with **no rebalance** and the exact
same assignment it had before; otherwise the session expires, a real
`leave` rebalance fires, and the partitions are reassigned cooperatively
(the expiry is recorded as `group_session_expired` in the subscription
audit trail).

Details worth knowing:

- Joining twice with the same `instanceId` while it is online throws
  `RangeError` (the failed join is rolled back). Rejoining after expiry
  is a normal fresh join and does trigger a `join` rebalance.
- The sticky assignor (EB-100) treats a limbo member as a member: when
  others join mid-limbo, the limbo member's partitions stay on it.
- `getGroupOffsets(groupId, instanceId)` returns that member's
  per-topic assignment watermark; `commitOffset(groupId, topic, seq,
  { instanceId })` records a per-member checkpoint (also per partition
  with `{ partition, instanceId }`), so a rejoining member resumes from
  its own checkpoint. Instance checkpoints are journaled with the same
  append-only durability as group checkpoints (EB-94) and do not move
  the group-level checkpoint or group lag.
- Sessions run on unref'd wall-clock timers, so an idle process can still
  exit. A static member in limbo is reported by `getGroupMembers` with
  its `instanceId`; `subscription.instanceId` is readable from the
  returned `Subscription`.

## Chaos testing (EB-103)

**Test / pre-production use only.** The deterministic fault-injection
harness lets tests and staging drills reproduce the failure paths that
are otherwise timing-dependent — degraded subscribers (EB-18), the
dead-letter queue (EB-28), and backpressure — without real handler bugs
or network faults. Never enable on a bus carrying real traffic:
injected faults deliberately violate delivery guarantees.

```ts
import { EventBus } from './src/bus.ts';

// Every handler invocation throws: the health probe counts consecutive
// failures and auto-pauses the subscriber — a deterministic EB-18 drill.
const bus = new EventBus({
  chaos: {
    scenarios: [
      { type: 'error', target: 'handler', rate: 1, seed: 42 },
    ],
  },
});
```

Each scenario owns a seeded RNG stream (mulberry32): the same seed in
the same call order reproduces the identical fault sequence, so a
failing drill is reproducible bit for bit. `rate` is the per-evaluation
probability in `[0, 1]` (`1` always fires, `0` never fires).

Supported (type, target) pairs — anything else throws `RangeError` at
construction:

| target      | error | drop | stall | latency |
|-------------|-------|------|-------|---------|
| `admission` | publish throws `ChaosInjectionError` | publish shed (returns its zero value; nothing sequenced, logged, or fanned out) | — | — |
| `drain`     | — | subscriber's live queue discarded (loss stays visible as a sequence gap) | drain round skipped; the queue accumulates under the normal backpressure policy | drain round deferred by `latencyMs` |
| `handler`   | handler throws `ChaosInjectionError` instead of running | handler never runs; the message is consumed as delivered | — | — |

Semantics notes:

- Admission faults are evaluated once per public publish call
  (`publish`, `publishBatch`, `publishAtomic`, `publishIdempotent`,
  `publishDelayed`), before validation — zero side effects either way.
- A handler `error` is observed exactly like a real throwing handler:
  the health probe counts it toward degradation, and the
  reliable-delivery envelope nacks it (past the redelivery budget it
  lands in the dead-letter queue) — the envelope evaluates the fault
  itself, so the outer drain call is left alone for reliable
  subscriptions.
- A drain `stall` is the deterministic path to `onBackpressure` and
  drop accounting: the queue fills while the drain never runs.
- Chaos never touches shadow subscribers, migration-paused (EB-82)
  subscriptions, or durable-log replay paths — live delivery only.
- Injection counters are on `bus.getStats().chaos`
  (`{ injected, byTarget }`), present only when scenarios were
  configured. Without chaos the stats shape — and everything else — is
  byte-for-byte identical to before.

## OTLP span export (EB-105)

**Opt-in.** Ships the delivery-pipeline trace spans (`src/trace.ts`,
EB-45) to an OpenTelemetry collector over OTLP/HTTP as hand-encoded
JSON — zero new dependencies, no OTel SDK:

```ts
const bus = new EventBus({
  trace: { sampleRate: 1 },
  otlp: {
    endpoint: 'http://collector:4318/v1/traces',
    serviceName: 'checkout-bus', // default 'realtime-event-bus'
    batchSize: 512, // spans per POST (default 512)
    flushIntervalMs: 5000, // periodic flush (default 5000)
    timeoutMs: 5000, // per-request timeout (default 5000)
    headers: { Authorization: 'Bearer …' }, // extra POST headers
    sampleRate: 1, // export-side sampling, independent of trace sampling
    maxBuffered: 10000, // drop-oldest bound (default 10000)
  },
});
```

Every sampled span (`bus.publish` → `bus.admission` → `bus.fanout` →
`bus.enqueue` → `bus.deliver` → `bus.ack`) becomes one OTLP span:
`traceId`/`spanId` need no translation — EB-45 already mints 32-hex
trace ids and 16-hex span ids, exactly the OTLP 128/64-bit format — so
an exported trace joins the same trace a `webhook-relay-ts` WR-18/WR-83
pipeline reports by string equality. Span attributes map 1:1
(`string`→`stringValue`, integer→`intValue`, float→`doubleValue`,
`boolean`→`boolValue`); `service.name` rides the resource attributes.

Semantics notes:

- `otlp` requires `EventBusOptions.trace` to be enabled — spans only
  exist when the trace pipeline samples them. Setting `otlp` without
  `trace` throws `RangeError` from the constructor.
- Best-effort, never in the delivery path: spans buffer in memory and
  flush on an unref'd timer (never holds the process open) or when the
  buffer reaches `batchSize`. A failed export POST drops that batch and
  counts it — the exporter does not retry, and a down collector surfaces
  as a rising counter, not as backpressure.
- Composes with a user-supplied `TraceOptions.onTraceSpan`: the callback
  still fires for every sampled span.
- `getStats().otlp` (`{ endpoint, bufferedSpans, exportedSpans,
  failedSpans, droppedSpans }`) is present only when export is
  configured; without it the stats shape is byte-identical to before.
- `bus.flushOtlp()` drains the buffer deterministically — the hook for
  tests and graceful shutdown. Invalid options throw `RangeError` from
  the constructor.

## Pluggable partition assignors (EB-106)

**Opt-in.** Consumer-group partition placement (`partitions` on
`subscribeToGroup`) is no longer limited to the two built-in
strategies — any `{ name, assign }` object works as the group's
assignor (`src/assignor.ts`):

```ts
import type { PartitionAssignor } from './src/assignor.ts';

// Rack-aware placement: keep each member's partitions on its own rack.
const rackAware: PartitionAssignor = {
  name: 'rack-aware',
  assign(partitions, members, previous) {
    // …pure and deterministic: same inputs, same assignment…
    return new Map(/* partition -> owner member id */);
  },
};

bus.subscribeToGroup('g', 'orders.*', handler, {
  partitions: 12,
  assignor: rackAware,
});
```

The built-ins are adapted as assignors with their exact historical
semantics, so the existing string options are unchanged:
`rendezvousPartitionAssignor` (`'rendezvous'`, the default) and
`stickyPartitionAssignor` (`'sticky'`, `src/sticky.ts`). Pass
`assignor` alone, or together with the `assignment` string set to the
assignor's own `name` — a contradictory pair throws `RangeError` at
registration.

Semantics notes:

- `assign(partitions, members, previous)` must be pure and deterministic;
  `previous` is the assignment in force before the rebalance (absent for
  a fresh group) for sticky-style minimal-migration logic. The bus never
  calls it with an empty roster — the drained-group case stays a
  bus-owned invariant, so unsubscribe teardown cannot break on a custom
  assignor.
- Fail-fast: a malformed assignor object throws `RangeError` at
  `subscribeToGroup`; an assignor that returns an invalid assignment
  (empty, missing partition, unknown owner, out-of-range key) fails the
  join with `RangeError` before the roster is touched — the subscription
  rolls back with nothing half-registered. A throwing assignor
  propagates to the joiner.
- Like the partition count, the assignor is fixed by the group's first
  member; a later member naming a different assignor throws `RangeError`
  (members that specify nothing inherit the group's assignor).
- `partitionRebalance.strategy` now reports the assignor *name* (a
  string: `'rendezvous'`/`'sticky'` for the built-ins), and every
  assignment is recorded in the subscription audit log
  (`getSubscriptionAudit({ action: 'rebalance' })` → `detail.assignor` +
  `detail.assignment`), so an operator can reconstruct exactly who owned
  what after the fact.

## TTL expiring-soon warnings (EB-110)

`SubscribeOptions.expiringSoon` (`src/expiring.ts`) arms an early warning
that a slow consumer is about to lose messages to TTL expiry (EB-10): when
the oldest queued message's remaining TTL is at or below `expiringSoonMs`,
the bus calls `onExpiringSoon` — before the messages are silently
discarded as `expired`, while there is still time to intervene (scale the
consumer, raise the TTL, shed the backlog).

```ts
bus.setTopicTtl('orders.**', 30_000);
bus.subscribe('orders.**', handler, {
  expiringSoon: {
    expiringSoonMs: 5_000, // warn when the head has <= 5s of life left
    onExpiringSoon: ({ subscriberId, pattern, remainingMs, queueDepth, at }) => {
      alert(`consumer ${subscriberId} on ${pattern}: head expires in ${remainingMs}ms, ${queueDepth} queued`);
    },
  },
});
```

Semantics:

- **Read-only.** The warning never changes delivery or TTL semantics: the
  message still delivers if the consumer catches up, still expires if it
  does not. Messages without a TTL never warn.
- **Edge-triggered, re-arms on drain.** The callback fires once per
  backlog — even as the head turns over — and re-arms when the queue
  drains empty. (This differs from the lag monitor's latch, which re-arms
  when the watermark drops below its threshold.)
- **Already-expired heads still warn.** A head past its deadline fires
  with a non-positive `remainingMs` — the loudest possible signal that
  the consumer is behind.
- **Error-isolated.** A throwing `onExpiringSoon` is swallowed; a
  monitoring hook can never break the publish or drain path.
- **Evaluated on every enqueue and after every drain**, on the bus's
  injected clock (`EventBusOptions.now`) — a completely idle bus cannot
  observe its own deadlines approaching, the same documented limitation
  as the lag watermark.

`getStats().expiringSoonAlerts` counts every warning bus-wide, rendered as
`eventbus_expiring_soon_alerts_total` in the Prometheus exposition.
Invalid options throw fail-fast from `subscribe` (`RangeError` for a
negative/non-finite threshold, `TypeError` for a non-function callback).

## Pull-mode consumption (long-polling) (EB-111)

`bus.pull(topic, { maxMessages, waitMs?, fromOffset? })` is the opt-in
pull-mode complement to push `subscribe`: it returns up to `maxMessages`
messages on one exact topic with `seq > fromOffset`, in ascending `seq`
order — the same per-topic sequence numbers (EB-13) push subscribers see
on `BusMessage.seq`. No subscription is registered; the caller owns the
cursor.

```ts
let offset = 0;
for (;;) {
  // Long-poll up to 5s for the next batch; resolves [] on expiry.
  const batch = await bus.pull('orders', { maxMessages: 100, waitMs: 5000, fromOffset: offset });
  for (const msg of batch) {
    await handle(msg);          // your code
    offset = msg.seq;           // advance only after handling
  }
}
```

Semantics:

- **Caller-owned cursor, at-least-once.** The bus keeps no server-side
  cursor — pass back the last handled message's `seq` as `fromOffset` to
  advance. Not advancing returns the same messages again, so every
  message is delivered at least once and duplicates are the caller's to
  dedupe (e.g. via `messageId`). Concurrent `pull` calls on the same
  topic are allowed; each carries its own offset and they never disturb
  each other — the stateless contract that keeps pull horizontally
  scalable.
- **Exact topic only.** `topic` must not contain `*` (`RangeError`):
  pull offsets live in one topic's sequence space, and a pattern fan-out
  would mix several topics' sequence numbers into one unreadable cursor.
- **Message source.** With `EventBusOptions.durableLogDir` the pull reads
  the durable log (`readSince`, `seq > fromOffset`), so a restarted bus
  keeps pulling from the same offset — the disaster-recovery counterpart
  to `resumeFromSeq`. Without a durable log it reads a per-topic
  in-memory ring of the most recent admitted messages
  (`EventBusOptions.pullBufferSize`, default 1000); history older than
  the ring is unavailable, and an unknown topic resolves `[]`, never an
  error.
- **Long poll.** When nothing newer than `fromOffset` is available and
  `waitMs > 0`, the call waits up to `waitMs` for the next publish on
  the topic instead of returning `[]` immediately; on expiry it resolves
  `[]` (never throws). The wait timer is `unref()`'d, so a pending poll
  never keeps the process alive.
- **Orthogonal to reliable delivery / ACK.** Pulled messages never enter
  the `AckTracker`, never consume ACK timeouts, and never move
  `getStats().unackedDeliveries` — pull and push-ACK are independent
  consumption contracts on the same stream.
- **Application payloads.** Returned `payload`s are the same values a
  push subscriber's handler receives (compressed payloads inflated,
  binary payloads stay bytes); TTL-expired messages are never returned.
  `messageId` / `publisherId` ride along when the publish carried them.

`getStats().pullStats` reports `{ pulls, pulledMessages,
longPollTimeouts }` bus-wide. Validation throws `RangeError` fail-fast
(empty topic, pattern topic, `maxMessages < 1`, negative/non-finite
`waitMs`, negative/non-integer `fromOffset`); a denied namespace or an
unauthorized topic throws exactly like `subscribe` does
(`NamespaceDeniedError` / `AclDeniedError`).

## Consumer-lag SLO alerting (EB-112)

`EventBusOptions.groupLag.slo` layers opt-in SLO alerting on top of the
always-on consumer-group lag rows (`getStats().groupLag`): while
`onGroupLag` pages on a single threshold excursion, the SLO watches for a
*breach episode* — the condition holding long enough to matter.

```ts
const bus = new EventBus({
  groupLag: {
    slo: {
      maxLagMessages: 1000, // breach when a row's lag exceeds 1000 messages…
      maxLagMs: 60_000,     // …or stays above zero lag for 60s straight
      breachSamples: 3,      // …for 3 consecutive evaluations
      onLagSloBreach: (e) => page(e),
    },
  },
});
```

Semantics:

- **Two bounds, either trips.** `maxLagMessages` breaches when a lag
  row's message lag exceeds it; `maxLagMs` breaches when the row's
  continuous above-zero-lag episode lasts at least that long on the bus
  clock (the consumer has been behind for the whole window). At least one
  bound is required (`RangeError` otherwise).
- **Debounced.** The breach condition must hold for `breachSamples`
  consecutive evaluations (integer `>= 1`, default 3) before the callback
  fires — one sample per lag evaluation (group assignment on publish,
  `commitOffset`, rebalance, threshold change), so a single slow commit
  never pages.
- **Edge-triggered latch.** The callback fires once per episode and
  rearms only after the breach clears; `event.violated` reports which
  bound tripped (`'maxLagMessages'` / `'maxLagMs'` / `'both'`).
- **Isolated.** A throwing `onLagSloBreach` is swallowed — monitoring
  never breaks the publish/commit path. Works with or without
  `onGroupLag`; valid but silent when no callback is set.

`getStats().lagSloBreaches` counts total breach episodes fired, and the
Prometheus exposition carries
`eventbus_consumer_lag_slo_breach_total`. Clocks are injectable
(`EventBusOptions.now`), so SLO timing stays deterministic in tests.

## Ops alert webhook outlet (EB-132)

`EventBusOptions.opsAlert` opts into a signed webhook outlet for the
bus's operational signals — consumer-lag SLO breaches (on top of the
EB-112 in-process `onLagSloBreach` callback), dead-letter queue growth,
and gateway disconnect storms. Following the AV-118 keeper-notifier
convention, the transport is pluggable: the caller implements the
actual HTTP POST via the `OpsAlertSink` trait (`send(delivery)`), and
the bus never touches the network.

```ts
import { EventBus } from './src/bus.ts';

const bus = new EventBus({
  opsAlert: {
    sink: {
      // POST delivery JSON; set x-ops-signature to delivery.signature.
      send: async (delivery) => {
        await fetch('https://ops.example.com/alerts', {
          method: 'POST',
          headers: {
            'content-type': 'application/json',
            'x-ops-signature': delivery.signature,
          },
          body: JSON.stringify(delivery),
        });
      },
    },
    secret: 'ops-webhook-secret', // HMAC-SHA256 secret (string or Buffer)
    keyId: 'ops-2026-10',         // optional rotation label
    dedupWindowMs: 5 * 60_000,    // burst-collapse window (default 5m)
    maxAttempts: 3,               // bounded retries (default 3)
    retryBaseMs: 1000,            // exponential backoff base (default 1s)
    gatewayDisconnectThreshold: 10,  // storm threshold (default 10)
    gatewayDisconnectWindowMs: 60_000, // storm window (default 60s)
  },
});

// Wire a gateway to the storm detector:
const gateway = new GatewayServer(bus, {
  onDisconnect: () => bus.noteGatewayDisconnect(),
});
```

Semantics:

- **Three signals.** `lag-slo-breach` fires on top of the EB-112
  callback (the user's hook runs first, then the alert is raised);
  `dead-letter-growth` fires on every per-subscriber DLQ move;
  `gateway-disconnect-storm` fires when the disconnect count inside
  the window reaches the threshold (edge-triggered, rearms below
  threshold).
- **Signed.** Every delivery carries `signature =
  hex(HMAC-SHA256(secret, canonicalJSON(alert)))` in the
  `x-ops-signature` header, with an optional `keyId`. `verifyDelivery`
  is the receiver-side check — forge a byte and it fails.
- **Bounded + deduplicated.** Each alert is attempted at most
  `maxAttempts` times with capped exponential backoff (unref'd timers),
  then dropped and counted. Repeats for the same `(kind, key)` inside
  the dedup window are suppressed — a DLQ flood or a flapping SLO
  produces one alert per window, not one per event.
- **Never blocks.** `note` is synchronous and infallible; delivery
  runs on a detached promise chain. A dead ops endpoint can neither
  stall nor break the publish path.
- **Off by default.** Without `opsAlert` the bus creates no alerter —
  zero behavior change. `getStats().opsAlert` reports
  `{ sent, failed, suppressed }`.

## Manual subscriber pause/resume (EB-113)

`bus.pauseSubscriber(subId, { reason? })` / `bus.resumeSubscriber(subId)`
give operators a per-subscriber delivery hold — the runbook counterpart
to the health probe's automatic degraded pause (EB-18):

```ts
const sub = bus.subscribe('orders.eu', handle);
// Deploy window: hold this consumer without touching the others.
bus.pauseSubscriber(sub.id, { reason: 'deploy freeze' });
// …later, pick up exactly where delivery paused:
bus.resumeSubscriber(sub.id);
```

Semantics:

- **Queue keeps accumulating.** While paused the subscriber's queue
  keeps filling under its normal backpressure policy (`drop-oldest` /
  `drop-newest` / `block`) — the pause itself drops nothing — and the
  drain skips the subscriber, so resume redelivers the preserved backlog
  in order on the next flush.
- **Orthogonal to the health probe.** Manual pause outranks degraded
  recovery: `resume()` (and the probe's auto-resume timer) clears only
  the degraded flag, never a manual pause — a subscriber can be in both
  states at once, and each is cleared by its own API.
- **Idempotent.** Pausing an already-paused subscriber (or resuming a
  non-paused one) returns `false` and audits nothing; unknown subscriber
  ids throw, matching `resume()` / `throttledCount()`.
- **Audited.** Transitions land in the subscription audit log as
  `subscriber_paused` (`{ reason }` when given) / `subscriber_resumed`,
  queryable via `getSubscriptionAudit({ action })`.

`getStats().pausedSubscribers` counts live manual pauses and the
Prometheus exposition carries `eventbus_paused_subscribers`.

## Consumer offset seek (EB-130)

`bus.seek(subId, target, { actor? }?)` repositions one subscriber's
consumption position — the manual counterpart to the subscribe-time
`resumeFromSeq` / `resumeFromTime` replays:

```ts
const sub = bus.subscribe('orders.eu', handle);
// Reprocess everything after seq 120:
bus.seek(sub.id, { seq: 120 });
// Or jump to the head and skip the backlog:
bus.seek(sub.id, 'latest');
```

Targets: `'earliest'` (position 0), `'latest'` (skip to the head),
`{ seq, topic? }` (exclusive — messages with a higher per-topic `seq`
are (re)delivered), `{ timestamp, topic? }` (just before the first
message published after the bus-clock timestamp, resolved against the
durable log). `topic` selects one concrete topic; it defaults to the
subscription pattern when the pattern is concrete, and is required for
`{ seq }` / `{ timestamp }` on multi-topic patterns.

Semantics:

- **Durable-log aware.** With a durable log the new position replays
  from it via the same `readSince` + admission path as subscribe-time
  resume, so filters, TTL expiry, keyed ordering and signature
  verification behave identically. Without a log the seek is
  forward-only — a backward move throws `RangeError`, and the new
  position takes effect for subsequent live messages.
- **Outbox aware.** Outbox records (EB-102) are pre-publish and carry
  no per-topic `seq`, so a seek never touches them; when the relay
  publishes them they draw fresh sequence numbers and flow as ordinary
  live messages.
- **Reliable in-flight invalidated.** Outstanding unacked deliveries
  have their ack timers cancelled without requeueing; stale
  consumer-held handles settle to silent no-ops and can never fabricate
  a gap. Replayed messages are tracked as fresh deliveries.
- **Auto-commit idempotent (EB-127).** For consumer-group members the
  seek never regresses a confirmed offset — highest-seq-wins: a
  requested position below the group's committed offset is clamped up
  to it (reported as `clampedByCommitted`), and the checkpoint itself
  is never moved by a seek.
- **No fake gaps.** The reposition moves the per-topic gap baselines
  directly; a seek itself never counts into `sequenceGaps`.
- **Stale backlog dropped.** Queued messages that are stale under the
  new position are dropped (with a log the replay is authoritative;
  without one only messages at or below the position go) — other
  topics' backlog is preserved.
- **Illegal positions throw `RangeError`**: negative `seq`, future
  `timestamp`, `{ timestamp }` without a durable log, a `topic`
  outside the subscription pattern, or a backward move without log
  history. Unknown subscriber ids throw `Error`.
- **Audited.** Every seek lands in the subscription audit log as
  `subscriber_seek` (`{ mode, topics: [{ topic, from, requested,
  position, replayed, droppedQueued, clampedByCommitted? }],
  invalidatedInflight }`), queryable via
  `getSubscriptionAudit({ action: 'subscriber_seek' })`.

The call returns a `SubscriberSeekResult` with one per-topic outcome
(`{ topic, from, requested, position, replayed, droppedQueued }`) plus
the `invalidatedInflight` count. Clocks are injectable
(`EventBusOptions.now`), so timestamp seeks stay deterministic in
tests.

## Mirror active-active write-conflict resolution (EB-131)

Two buses mirroring the same keyed topic to each other
(`dst.mirror(src, rules)`, EB-37) form an active-active pair: either
side may publish a new value for the same key. Every admitted message
carries a hybrid logical clock timestamp (`BusMessage.hlc`,
`{ pt, l, node }` — physical millis from the bus clock, a logical
tick, and the bus id as tiebreak), stamped at admission and preserved
verbatim across mirror hops.

On the mirror receive side the destination arbitrates per-key
last-writer-wins: it keeps the greatest HLC seen per `(topic, key)`;
an incoming mirrored write whose HLC does not beat the register is
dropped as stale and counted in `getStats().mirrorConflicts` (per-topic
in `getStats().mirrorConflictsByTopic`, exported as
`eventbus_mirror_conflicts_total{topic}`). Unkeyed messages skip
arbitration. The destination merges the incoming HLC into its own
clock on receipt, so a local write made after seeing a remote write
orders after it even under clock skew.

The arbitration runs after the EB-37 trail loop check, and it runs
even for loop-dropped messages: a message whose trail already holds
this bus's id counts as `loopsPrevented` *and* still updates the LWW
register / counts a conflict, so a loop can never resurrect a stale
write. The application key now rides the envelope end to end
(`BusMessage.key`), so the mirror frame carries it and the destination
draws its own per-key sequence numbers for the mirrored write.

## Publish circuit breaker (EB-116)

`bus.setPublishCircuitBreaker(topic, opts?)` arms an opt-in publish-side
circuit breaker for one topic — a fail-fast for a topic whose consumers
are all down. Disabled by default: without a rule the topic keeps
exactly its old publish behavior (backward compatible).

```ts
bus.setPublishCircuitBreaker('orders.eu', {
  consecutiveShedPublishes: 5, // default
  halfOpenProbeIntervalMs: 1000, // default, bus clock
});
```

Semantics:

- **Trip condition.** The breaker opens when `consecutiveShedPublishes`
  publishes in a row each find *every* production subscriber of the
  topic degraded (health-probe auto-pause, EB-18) or shedding at the
  publish side (adaptive-throttle excursion, EB-14). A publish that finds
  at least one healthy subscriber resets the streak; a topic with no
  subscribers never trips.
- **Fast-fail, never silent.** While open, `publish` throws
  `CircuitBreakerOpenError` immediately instead of fanning the message
  out to subscribers that cannot consume it — a dropped message is never
  mistaken for a delivered one. The refusal is counted in
  `TopicStats.circuitBrokenMessages` / `BusStats.circuitBrokenMessages`
  (plus `circuitBreakerTrips` per open transition) and surfaced on
  `EventBusOptions.onAdmissionRejected` with reason `'circuit-breaker'`.
  Refusals are pre-admission: no sequence number is consumed.
- **Half-open probe.** After `halfOpenProbeIntervalMs`, one publish is
  admitted as a probe: it closes the breaker when it finds a healthy
  subscriber, and re-opens it (restarting the interval) when every
  subscriber is still degraded/shedding.
- **Observability.** `getStats().circuitBreakers` lists each armed
  breaker (`topic`, `state`, `rejectedMessages`);
  `bus.circuitBreakerState(topic)` reads one breaker's live state
  (`'closed'` | `'open'` | `'half-open'`, `undefined` when no rule is
  armed); `bus.clearPublishCircuitBreaker(topic)` disarms it. The
  Prometheus exposition carries
  `eventbus_circuit_broken_messages_total`,
  `eventbus_circuit_breaker_trips_total`,
  `eventbus_circuit_breakers_open`, and per-topic
  `eventbus_topic_circuit_broken_messages_total{topic}`.

## Credit-based delivery flow control (EB-117)

`bus.grantCredits(subId, n)` gives one subscriber an explicit delivery
budget — subscriber-driven backpressure for push subscribers, orthogonal
to `deliveryShaping` (bus-paced rate smoothing, EB-21) and `pull()`
(consumer-driven long-polling, EB-111). Opt-in: a subscriber that never
receives a grant has unlimited credit and behaves exactly as before.

```ts
const sub = bus.subscribe('orders.eu', handle, {
  onCreditExhausted: (e) => alert(`${e.subscriberId} out of credit, ${e.pendingMessages} queued`),
});
bus.grantCredits(sub.id, 100); // returns the new balance (100)
```

Semantics:

- **One credit per delivery.** The bus delivers only while the balance
  is positive; each delivered message consumes 1. Batch deliveries
  (`SubscribeOptions.batch`) consume per message, like shaping's
  one-token-per-message take.
- **Exhaustion pauses, never drops.** At zero credit the drain holds
  the subscriber back — the backlog keeps accumulating under its normal
  backpressure policy — and `onCreditExhausted` fires once per episode
  (edge-triggered latch, `{ subscriberId, pattern, pendingMessages }`).
  Granting more credit re-arms the latch and schedules a flush, so the
  preserved backlog resumes automatically, in order.
- **Durable-log replay walks an independent budget.** Replayed messages
  never consume credit (they don't even count against the drain's
  credit bound), so a resume never wedges behind an exhausted
  subscriber — while FIFO order with live messages is preserved.
- **Validation.** `n` must be a positive integer (`RangeError`
  otherwise); unknown subscriber ids throw, matching
  `pauseSubscriber` / `resumeSubscriber`.

`getStats().creditExhaustedSubscribers` counts live exhausted
subscriptions and the Prometheus exposition carries
`eventbus_credit_exhausted_subscribers`.

## Priority lanes (EB-122)

Every subscriber queue is split into three priority lanes — `urgent`,
`normal`, `bulk` — and the drain serves them in weighted round-robin
rotation. Opt-in: `publish` without a priority behaves exactly as before
(everything rides the normal lane, and a single occupied lane drains as a
plain FIFO, so the rotation is a no-op until you use a second lane).

```ts
const bus = new EventBus({
  priorityLanes: {
    weights: { urgent: 8, normal: 2, bulk: 1 }, // the default
    bulkStarvationRounds: 10,                  // the default
  },
});
bus.publish('alerts', page, { priority: 'urgent' });
bus.publish('events', click, { priority: 'bulk' }); // default is 'normal'
```

Semantics:

- **Where the lanes live.** Per subscriber queue, not per topic — the
  drain already runs per subscriber, so each subscriber's backlog is its
  own three-lane queue. Per-topic and global lane counters are aggregated
  in `getStats()`.
- **Weighted rotation.** Each drain pass serves up to `weights.urgent`
  urgent messages, then `weights.normal` normal, then `weights.bulk`
  bulk, and repeats while the drain budget (shaping, throttle, credits)
  allows. Weights must be positive integers; anything else throws
  `RangeError` at construction or per publish.
- **Bulk starvation protection.** If a drain round serves no bulk while
  bulk is waiting, a starvation counter increments; when it would reach
  `bulkStarvationRounds`, the next round force-interleaves one bulk
  message ahead of the rotation. The forced message still consumes drain
  budget — the guard reorders within the budget, it never exceeds it.
- **FIFO within a lane; priority across lanes.** Ordering is preserved
  per lane, but an urgent message published later jumps ahead of an
  earlier bulk message. That is the documented trade-off; keep one
  priority per causal chain (e.g. same-key keyed messages) if cross-lane
  reordering would break the consumer.
- **Sequence numbers stay publish-order.** `seq` is assigned at admission,
  so it is monotonic per topic regardless of lane. A subscriber that
  watches `sequenceGaps` sees lane reordering as at-least-once
  redeliveries, never phantom gaps.
- **Backpressure is per lane, precedence-aware.** Under `drop-oldest`
  the victim lane is the lowest-precedence non-empty lane at or below
  the newcomer's precedence — a bulk newcomer never evicts an urgent
  message, and is refused outright when the queue holds only
  higher-precedence lanes (no churn). `drop-newest` refuses overflow
  regardless of lane. Byte budgets work the same way, on aggregate
  bytes. Urgent messages still drop when the policy says so; priority
  is not a backpressure bypass.
- **Metrics.** `TopicStats.lanes` and the top-level `BusStats.lanes`
  carry `{ queued, delivered, dropped }` × `{ urgent, normal, bulk }`.
  `queued` is the live backlog (`enqueued − delivered − dropped −
  expired` per lane); `unsubscribe` releases a subscriber's lane backlog
  from the gauges.
- **Durable log.** The lane persists on log lines (only non-normal lanes
  are written, so default traffic stays byte-identical) and is restored
  on replay; delayed messages keep their lane from schedule to fan-out.
- **Interactions.** Admission features run before the lane is consulted:
  rate-limit shed (EB-19) and schema validation (EB-20) can still reject
  the publish. Consumer-group assignment (EB-16) happens at fan-out, so
  the lane survives group routing. Shaping (EB-21), throttling (EB-14)
  and credits (EB-117) bound the drain budget that the rotation allocates
  within; the EB-117 replay bypass still flows even at zero budget. Keyed
  / causal gates run pre-queue. Routes, forwards, the bridge and cluster
  links re-admit messages into the normal lane. Pause buffers, barriers
  and shadow traffic keep their existing semantics (shadow drains never
  touch lane stats).

## Topic-level fair scheduling (EB-135)

`SubscribeOptions.topicFairness` opts a subscription into deficit
round-robin across the topics queued for it — instead of draining its
queue in plain FIFO order, the drain serves the queued topics in
weighted rotation. Opt-in: without it the drain behaves exactly as
before.

```ts
bus.subscribe('*', handler, {
  topicFairness: { weights: { hot: 1, cold: 3 } },
});
// drains one `hot` message for every three `cold` ones,
// interleaved at single-message granularity
```

Semantics:

- **Deficit round-robin, quantum = 1 message.** Each drain round walks
  the live backlog's topics in first-appearance order, credits every
  topic with its weight, and dequeues one message per unit of deficit —
  so the long-run service ratio matches the weight ratio, and each
  topic's own messages stay in publish order. A topic absent from
  `weights` drains at weight 1.
- **The scheduler only picks *which* messages the budget covers.** The
  drain budget is still the minimum of the delivery gates
  (`deliveryShaping`, `rateLimit`, `maxInflight`, `slowStart`, credits),
  exactly as for the plain drain — fairness composes orthogonally with
  all of them. The EB-117 replay bypass still flows freely, consuming
  neither budget nor deficit.
- **Deficits carry across rounds.** A topic that still holds messages
  keeps its leftover deficit for the next drain round; a topic that
  drained completely rejoins at zero. The per-(subscriber, topic)
  deficits are aggregated bus-wide in
  `getStats().topicFairness.deficits`.
- **Per-topic order is preserved.** Gap detection runs per topic, so
  fair reordering across topics never reads as a sequence gap.
- **Batch subscribers get single-topic batches.** With `batch`, each
  collection round resolves the round-robin winner (the queue head's
  topic — the group a DRR round always serves first) once and fills
  only from that topic in per-topic FIFO order, so a delivered batch
  never mixes topics. Across collections the head topic is served first;
  the weight ratio governs the plain (non-batch) drain at message
  granularity.
- **Validation.** Every weight must be a positive integer — anything
  else throws `RangeError` from `subscribe`. Cannot be combined with
  `shadow` (throws `RangeError`).
- **Metrics.** `getStats().topicFairness.scheduledByTopic` counts, per
  topic, the messages the fair scheduler dequeued ("scheduled", not
  "delivered"), and `src/metrics.ts` renders them as
  `eventbus_fair_scheduled_messages_total{topic}` — only for topics with
  at least one scheduled message.

## Subscription filter expressions (EB-123)

`SubscribeOptions.filterExpr` attaches a declarative, compile-time-checked
predicate to a subscription — a content filter you can write as data
instead of code:

```ts
const sub = bus.subscribe('market.**', handle, {
  filterExpr: `type == "trade" && price > 100 && exists(user.id) && symbol =~ /^BTC/`,
});
```

The expression is compiled once at subscribe time by the zero-dependency
parser in `src/filterexpr.ts` (hand-written tokenizer + recursive-descent
parser — no `eval`, no `Function` constructor). An illegal expression
(syntax error, unknown operator, invalid regex) throws `RangeError` from
`subscribe`, before anything registers.

### Syntax

| Class | Operators | Examples |
|---|---|---|
| Equality | `==`, `!=` | `type == "trade"`, `retries != 3` |
| Range | `>`, `>=`, `<`, `<=` | `price > 100`, `price <= 500` |
| Existence | `exists(path)` or `path exists` | `exists(user.id)`, `user.id exists` |
| Regex | `=~` with a `/re/flags` literal | `symbol =~ /^BTC/`, `sym =~ /^btc/i` |

- **Composition:** `&&`, `||`, prefix `!`, and parentheses:
  `a == 1 && (b == 2 || !c == 3)`.
- **Paths:** dotted field paths with array indexes — `user.id`,
  `legs[0].amount`.
- **Literals:** single/double-quoted strings (with escapes), numbers
  (`-5`, `1.5e2`), `true`, `false`, `null`.

### Evaluation semantics

- **Never throws.** A missing field, a type mismatch, or a non-object
  payload simply fails the predicate — a filter expression can never
  take down a publish call (unlike a throwing `filter` predicate, which
  propagates by design).
- **Missing is not a value.** A missing field never equals a literal —
  not even `null` (`x == null` is false when `x` is absent, `x != null`
  is true); only `exists(x)` observes absence.
- **Range** compares numbers numerically and strings lexicographically;
  mixed or non-comparable types never match (no coercion).
- **Regex** requires the field to be a string; the pattern is validated
  by the RegExp engine at compile time.

### Pipeline position and interactions

Evaluated at fan-out, **after** the `filter` predicate and **before**
composite patterns — `filter` and `filterExpr` compose with AND (both
must accept). A rejected message:

- is never queued — no backpressure budget, no throttle token, no
  sequence gap (the per-topic baseline advances over it, exactly like a
  `filter` rejection);
- **consumes no delivery credit** — credit (EB-117) is spent per message
  at drain time, and a filtered message never reaches the drain, so
  filtering can never trip `onCreditExhausted` or starve real deliveries;
- **creates no ack obligation** — ack tracking starts at delivery, which
  never happens, so reliable subscriptions see no pending delivery, no
  redelivery timer, and no timeout for filtered messages;
- counts into `getStats().filteredMessages` (global + per-topic), like
  `filter` rejections.

Applies uniformly to live fan-out, durable-log replay
(`resumeFromSeq` / `resumeFromTime`), the retained snapshot, and shadow
mirrors (uncounted there, like `filter`). Like `filter`, it is a string
and therefore survives subscription migration snapshots; unlike
`filter`, it needs no code on the receiving side.

## Recurring publishes (cron) (EB-124)

`bus.scheduleRecurring(topic, payloadFactory, cronExpr, opts?)` registers a
cron-driven recurring publish: on every trigger of `cronExpr` the bus calls
`payloadFactory()` and publishes its return value to `topic` through the
normal publish pipeline — admission, schema validation, rate limiting, TTL —
exactly as if `publish` had been called at that moment. Returns a
`{ id, cancel() }` handle (`cancel()` is idempotent).

```ts
const job = bus.scheduleRecurring(
  'ops.heartbeat',
  () => ({ at: Date.now(), status: 'ok' }),
  '*/30 * * * *', // every 30 minutes
  { publisherId: 'cron-agent' },
);
// ... later:
job.cancel();
```

The expression language is five fields — `minute hour day-of-month month
day-of-week` — evaluated in **UTC**, parsed by the zero-dependency parser in
`src/cronparse.ts` (also exported as `parseCronExpression` /
`nextCronOccurrence` for reuse):

- `*`, `*/n`, `n`, `n-m`, `n-m/s`, comma lists, and `n/s` (= `n-max/s`);
- month and weekday names (`JAN`–`DEC`, `SUN`–`SAT`), case-insensitive;
  day-of-week accepts `0`–`7` with `7` meaning Sunday;
- `L` / `W` / `#` extensions: `L` (last day of month; in day-of-week: last
  Saturday), `L-n` (n days before month end), `LW` (last weekday of month),
  `15W` (weekday nearest the 15th), `5L` (last Friday of month), `MON#3`
  (third Monday of month).

Day matching is classic cron: both day fields restricted → either may match;
exactly one restricted → it decides; neither → every day. Parsing is
fail-fast — `scheduleRecurring` throws `RangeError` for any malformed
expression (wrong field count, out-of-range value, reversed range, bad
`L`/`W`/`#` form, or an expression that can never fire such as February 30)
and for an invalid topic, with zero side effects: no job, no timer, no log
record. A non-function factory throws `TypeError`.

Trigger semantics:

- The factory is called fresh on every trigger. A throwing factory skips the
  trigger and counts it (`errorCount`) — the schedule never dies.
- A trigger rejected by admission (schema, rate limit, TTL, ACL…) follows
  normal admission semantics — counted, not published — and the job keeps
  its cadence. A throwing schema validator is caught and counted like a
  factory throw, so one bad trigger cannot kill the timer chain.
- **No catch-up**: a job that falls behind fires exactly once per sweep,
  then recomputes its next trigger strictly after now — missed occurrences
  are skipped, never replayed in a burst (no restart storm).
- TTL is stamped at fire time (each trigger is a fresh publish); topic
  aliases resolve at fire time too.

Durability is opt-in via `EventBusOptions.durableLogDir`: scheduling writes
a seq-0 schedule record (`recurringJobId` + `cronExpr`), every trigger
writes a seq-0 trigger record (audit trail), and `cancel()` writes a seq-0
tombstone — the EB-24 convention, invisible to replay. Restart recovery:
a tombstoned job is never rebuilt; a schedule record without a tombstone
becomes a **dormant** entry in `getStats().recurringJobs` (its next trigger
recomputed from now) — the payload factory is in-process state and cannot
be restored, so no live timer is rebuilt and nothing is re-recorded. The
application re-registers its jobs on startup (as it does its subscriptions);
re-registering the same (topic, cron) supersedes the dormant entry so
dormant generations do not accumulate.

`getStats().recurringJobs` reports `{ active, jobs: [{ id, topic, cron,
nextTriggerMs, triggerCount, errorCount, dormant? }] }`. The shared
wake-up timer is unref'd (a pending job never keeps the process alive), and
due sweeps also run at the start of every flush, so advancing an injected
clock past a trigger time and then publishing anything fires
deterministically in tests.

## Topic lifecycle management (EB-126)

`bus.createTopic(name, opts)` provisions a topic — one call that registers
the topic in the lifecycle registry and atomically applies a policy bundle
through the existing per-topic configuration APIs, keyed by the exact
(concrete, namespace-resolved) topic name:

```ts
bus.createTopic('orders.eu', {
  metadata: { owner: 'payments-svc', tags: ['payments'] },
  ttlMs: 60_000, // → setTopicTtl
  compression: { thresholdBytes: 1024 }, // → setTopicCompression
  signing: { keys: [{ kid: 'k1', secret: '…' }] }, // → setTopicSigningKey
  retention: { maxAgeMs: 86_400_000 }, // → setDurableLogMaxAgeMs (bus-global!)
});
```

Every option is validated before the first policy lands — a bad bundle
throws `RangeError` with zero side effects. Re-provisioning a live topic
updates its policies (returns `{ created: false }`); a topic deleted
earlier re-provisions fresh (`{ created: true }`) and admits publishes
again. This registry is independent of the advisory EB-77 catalog:
`createTopic` neither requires nor performs `registerTopic`.

One honest deviation: **retention is bus-global** in this codebase (EB-85
`setDurableLogMaxAgeMs`, EB-119 `durableLogMaxBytesPerTopic`) — there is
no per-topic retention rule API to apply. `retention.maxAgeMs` therefore
moves the shared durable-log age bound for every topic, exactly like
calling `setDurableLogMaxAgeMs` yourself (same validation, same audit
trail). Pass it only when you mean it.

`bus.deleteTopic(name, { purge })` tears the topic down and leaves a
tombstone:

- **Wildcard subscriptions die with the topic.** Any subscription whose
  pattern contains `*` and whose compiled matcher still hits the topic is
  removed through the normal unsubscribe path (queues, timers, indexes
  disposed; `unsubscribe` audited). For consumer-group members that path
  performs the group leave and fires the **real** `onRebalance`
  (`GroupRebalanceEvent`, trigger `'leave'`) — the membership hook's
  actual scope. Plain wildcard subscriptions get the audit event, not a
  fabricated group event: there is no generic topic-level rebalance hook
  in this codebase. Exact-topic subscriptions are deliberately left
  alone — they idle, and on a soft delete their replay still works.
- **Future publishes are refused.** The tombstone is an admission gate in
  `fanOut` (also `previewPublish` and `publishAtomic`): publishes to a
  deleted topic are rejected with reason `'alias-retired'` — the
  vocabulary's only "this name is retired from service" item, with its
  zero-side-effect contract (no sequence number, no durable-log write,
  no rate-limit budget). The gate is named `'topic-lifecycle'` in
  `previewPublish` so the verdict stays attributable. The retired-alias
  machinery itself is *not* reused: it is TTL-driven on alias links with
  a migration target, while deletion has no successor.
- **`purge: true` destroys durable history**: the live `<topic>.log`
  file, archived segments and their checksum sidecars, the topic's
  archive-index and segment-chain lines, the registered compression
  dictionary (only when no other rule references its id), and every
  sequence counter (`lastSeqs`, per-partition seqs, the bus's `topicSeq`)
  — a re-provisioned topic numbers from 1 again. Without purge the
  history is kept and stays replayable (`resumeFromSeq`,
  `resumeFromTime`), including the retained message.
- Pending delayed schedules and recurring jobs targeting the topic are
  cancelled (a timer firing into a tombstoned topic would only burn
  rejection counts); exact-name policy rules are removed (pattern keys
  may still match other topics).

Not purged, by design: the advisory EB-77 catalog, topic aliases/routes,
committed consumer offsets and dedup journals (consumer/producer identity
state, not topic data), cumulative `TopicStats`, and cross-topic key
sequence cursors. Dictionaries and encryption keys live in memory in this
codebase — there are no per-topic dictionary/key files; the sealed log
lines die with the `.log` file.

Names: empty strings, non-strings, and anything containing `*` throw
`RangeError` — lifecycle manages concrete topics only. Both calls take an
`actor` (default `'system'`) and are audited as `topic_created` /
`topic_deleted` in the subscription lifecycle audit log. Deleting an
unknown topic is a no-op returning `{ deleted: false }`; a soft-deleted
topic deleted again with `purge: true` purges the retained history then.
