/**
 * Subscription lifecycle audit log (EB-78): an append-only JSONL journal of
 * every subscription change on the bus — subscribe / unsubscribe / pattern
 * hot-updates (EB-74) / consumer-group join-leave / group rebalances / topic
 * registry changes (EB-77) / backpressure drops (EB-83,
 * `backpressure_dropped`) / signature-verification failures (EB-84,
 * `signature_verification_failed`) / durable-log retention policy changes
 * (EB-85, `retention_updated`) / retained-message rule and state changes
 * (EB-87, `retained_rule_set` / `retained_rule_cleared` /
 * `retained_cleared`) / retained-snapshot TTL rule changes (EB-378,
 * `retained_ttl_rule_set` / `retained_ttl_rule_cleared`) / topic pause/resume (EB-88, `topic_paused` /
 * `topic_resumed`) / bulk delayed-delivery cancellation (EB-89,
 * `delayed_cancelled`) / publish-side partition-count rule changes (EB-90,
 * `partition_rule_set` / `partition_rule_cleared`) / out-of-order barrier
 * discards (EB-98, `barrier_dropped`) / manual subscriber pause-resume
 * (EB-113, `subscriber_paused` / `subscriber_resumed`) / manual
 * consumer-offset seek (EB-130, `subscriber_seek`) / handler fault
 * isolation quarantine (EB-197, `subscriber_quarantined` /
 * `subscriber_quarantine_resumed`).
 *
 * This complements `EventBusOptions.onAuthzDenied`, which only fires for
 * *rejected* authorization decisions: the audit log records the normal,
 * successful subscription lifecycle that the denial hook deliberately
 * ignores. Operators answering "who subscribed to what, and when" query
 * here instead of reconstructing it from delivery metrics.
 *
 * Two layers:
 * - an always-on bounded in-memory ring (default 1000 entries, oldest
 *   evicted) that `EventBus.getSubscriptionAudit()` queries — zero
 *   configuration, zero disk I/O;
 * - opt-in disk persistence via `dir`: one `audit-YYYY-MM-DD.jsonl` file
 *   per UTC day (daily rotation is implicit — each event lands in the file
 *   named by its own timestamp), so a restarted bus keeps its audit
 *   trail. On construction the in-memory ring is back-filled from the
 *   newest on-disk lines, skipping corrupt lines.
 *
 * All timestamps come from the bus clock (`now`), so tests stay
 * deterministic. Disk writes are synchronous and error-isolated: a failed
 * append never throws into the subscribe path — it is counted in
 * `writeErrors()` instead.
 */
import { appendFileSync, mkdirSync, readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import {
  AUDIT_CHAIN_GENESIS,
  attachChainLink,
  effectiveRecordHash,
  isHashString,
  verifyChainLinks,
  type ChainVerification,
} from './auditchain.ts';

/** Every lifecycle action the audit log records. */
export type SubscriptionAuditAction =
  | 'subscribe'
  | 'unsubscribe'
  | 'update_pattern'
  | 'group_join'
  | 'group_leave'
  | 'group_session_expired'
  | 'group_paused'
  | 'group_resumed'
  | 'rebalance'
  | 'topic_register'
  | 'topic_deregister'
  | 'topic_created'
  | 'topic_deleted'
  | 'topic_renamed'
  | 'topic_forked'
  | 'topics_merged'
  | 'topics_split'
  | 'topic_data_imported'
  | 'backpressure_dropped'
  | 'signature_verification_failed'
  | 'retention_updated'
  | 'retained_rule_set'
  | 'retained_rule_cleared'
  | 'retained_ttl_rule_set'
  | 'retained_ttl_rule_cleared'
  | 'retained_cleared'
  | 'topic_paused'
  | 'topic_resumed'
  | 'topic_pause_buffer_limit_revised'
  | 'delayed_cancelled'
  | 'delayed_rescheduled'
  | 'delayed_ttl_extended'
  | 'recurring_job_cancelled'
  | 'recurring_job_revised'
  | 'ack_deadline_extended'
  | 'ack_timeout_revised'
  | 'redelivery_backoff_revised'
  | 'delivery_window_revised'
  | 'max_inflight_revised'
  | 'filter_revised'
  | 'delivery_shaping_revised'
  | 'health_probe_revised'
  | 'throttle_revised'
  | 'batch_revised'
  | 'retry_ladder_revised'
  | 'dead_letter_budget_revised'
  | 'dead_letter_auto_replay_revised'
  | 'dead_letter_approval_revised'
  | 'rate_limit_revised'
  | 'delivery_priority_revised'
  | 'topic_fairness_revised'
  | 'lag_monitor_revised'
  | 'slow_handler_revised'
  | 'handler_fault_isolation_revised'
  | 'delivery_latency_revised'
  | 'delivery_concurrency_revised'
  | 'lag_forecast_revised'
  | 'ack_latency_revised'
  | 'session_window_revised'
  | 'queue_size_revised'
  | 'queue_max_bytes_revised'
  | 'subscriber_max_payload_bytes_revised'
  | 'subscriber_schema_version_revised'
  | 'key_range_revised'
  | 'backpressure_tier_revised'
  | 'drop_policy_revised'
  | 'multi_patterns_revised'
  | 'filter_expr_revised'
  | 'expiring_soon_threshold_revised'
  | 'key_hotspot_threshold_revised'
  | 'key_hotspot_recover_depth_revised'
  | 'key_hotspot_auto_mitigate_revised'
  | 'cluster_link_heartbeat_revised'
  | 'cluster_link_reconnect_backoff_revised'
  | 'deduplicate_messages_revised'
  | 'tail_sampling_revised'
  | 'partition_health_revised'
  | 'otlp_revised'
  | 'otlp_metrics_revised'
  | 'chaos_revised'
  | 'global_publish_rate_limit_revised'
  | 'outbox_relay_revised'
  | 'schedule_skew_revised'
  | 'admission_latency_revised'
  | 'bridge_health_revised'
  | 'bridge_revised'
  | 'cep_sequence_pattern_revised'
  | 'cep_sequence_pattern_paused'
  | 'cep_absence_window_revised'
  | 'latency_slo_threshold_revised'
  | 'latency_slo_window_size_revised'
  | 'causal_revised'
  | 'barrier_revised'
  | 'window_revised'
  | 'delayed_topic_held'
  | 'delayed_topic_resumed'
  | 'delayed_key_held'
  | 'delayed_key_resumed'
  | 'delayed_predicate_held'
  | 'delayed_predicate_resumed'
  | 'partition_rule_set'
  | 'partition_rule_cleared'
  | 'partition_expansion'
  | 'topic_partition_scaled'
  | 'config_applied'
  | 'config_rolled_back'
  | 'segment_checksum_failed'
  | 'barrier_dropped'
  | 'subscriber_paused'
  | 'subscriber_resumed'
  | 'subscriber_seek'
  | 'subscriber_quarantined'
  | 'subscriber_quarantine_resumed'
  | 'dead_letter_replay_requested'
  | 'dead_letter_replay_approved'
  | 'dead_letter_replay_rejected'
  | 'delivery_sample_rate_updated'
  | 'header_filter_updated'
  | 'subscriber_labels_updated'
  | 'client_pressure'
  | 'client_pressure_recovered'
  | 'tenant_quota_warning'
  | 'bridge_unhealthy'
  | 'bridge_recovered'
  | 'mirror_throttled'
  | 'mirror_throttle_revised'
  | 'topic_key_rotated'
  | 'decrypt_failed'
  | 'auto_remediate_resume'
  | 'auto_remediate_linger_pruned'
  | 'auto_remediate_rebalance'
  | 'auto_remediate_run'
  | 'auto_remediate_revised'
  | 'ops_alert_revised'
  | 'ops_alert_secret_rotated'
  | 'admission_webhook_revised'
  | 'publish_audit_revised'
  | 'priority_lanes_revised'
  | 'trace_revised'
  | 'dedup_bloom_revised'
  | 'lag_slo_revised'
  | 'trace_archive_revised'
  | 'idempotency_revised'
  | 'lineage_revised'
  | 'drop_journal_revised'
  | 'pull_buffer_revised'
  | 'fan_out_chunk_size_revised'
  | 'subscription_audit_revised'
  | 'durable_log_max_entries_revised'
  | 'delayed_capacity_revised'
  | 'topic_delayed_capacity_revised'
  | 'key_delayed_capacity_revised'
  | 'topic_durable_log_max_age_revised'
  | 'rpc_settled_retention_revised'
  | 'publish_rate_ring_capacity_revised'
  | 'namespace_options_revised'
  | 'composite_patterns_revised'
  | 'durable_log_max_bytes_revised'
  | 'durable_log_publish_dedup_max_entries_revised'
  | 'durable_log_segment_rotation_revised'
  | 'durable_log_readback_cache_revised'
  | 'compression_auto_train_revised'
  | 'publish_circuit_breaker_revised'
  | 'subscriber_breaker_revised'
  | 'durable_log_retain_local_segments_revised'
  | 'durable_log_encryption_key_rotated'
  | 'durable_log_segment_offload_client_revised'
  | 'durable_log_key_compaction_revised'
  | 'state_store_max_keys_revised'
  | 'processing_latency_slo_alert_revised'
  | 'slow_start_revised'
  | 'group_rebalance_debounce_revised'
  | 'idle_window_revised'
  | 'idle_activity_revised'
  | 'group_handoff_linger_revised'
  | 'group_auto_commit_revised'
  | 'group_assignor_revised'
  | 'group_offsets_seeked'
  | 'group_session_timeout_revised'
  | 'group_auto_offset_reset_revised'
  | 'group_auto_offset_reset_cleared'
  | 'group_default_session_timeout_revised'
  | 'group_default_session_timeout_cleared'
  | 'subscription_lease_revised'
  | 'stream_join_window_revised'
  | 'stream_join_max_buffered_per_key_revised'
  | 'stream_join_how_revised'
  | 'allowed_lateness_revised'
  | 'auth_token_revised'
  | 'group_rebalance_triggered';

/**
 * One audit entry. `detail` carries action-specific context without
 * bloating the flat schema:
 * - `update_pattern`: `{ from: <old pattern> }`
 * - `rebalance`: `{ trigger: 'join'|'leave'|'expand'|'remediate'|'assignor'|'operator', memberId, members: <count> }`
 * - `group_session_expired`: `{ instanceId }` (static membership, EB-104:
 *   the held roster slot was released after the session window)
 * - `topic_register`: `{ owner?, description?, schemaVersion?, tags?, updated: <bool> }`
 * - `topic_deregister`: `{ owner?, description? }` (last-known metadata)
 * - `topic_created`: `{ updated: <bool>, ttlMs?, retentionMaxAgeMs?,
 *   compression: <bool>, signing: <bool> }` — a topic lifecycle
 *   provisioning (EB-126); the `topic` field carries the concrete topic
 * - `topic_deleted`: `{ purge: <bool>, invalidatedSubscriptions: <count>,
 *   hadProvisioning: <bool> }` — a topic lifecycle deletion (EB-126); the
 *   `topic` field carries the concrete topic. Per invalidated wildcard
 *   subscription a second event is recorded with `subId`/`pattern` and
 *   `detail: { reason: 'wildcard_subscription_invalidated', purge }`
 * - `backpressure_dropped`: `{ policy: <backpressure tier>, dropped: <count>, queueSize: <number> }`
 * - `signature_verification_failed`: `{ seq: <per-topic seq>, kid?: <key id that signed, when present> }`
 * - `topic_key_rotated`: `{ oldKeyId, newKeyId, rotatedAtMs }` — a
 *   per-topic DEK rotation (EB-179); the `topic` field carries the rule's
 *   topic pattern, `actor` is `'system'`
 * - `decrypt_failed`: `{ seq: <per-topic seq>, keyId?: <key id on the
 *   envelope, when present> }` — a sealed message dropped fail-closed at
 *   delivery (EB-179): unknown key id, retired past its grace window, or
 *   tampered envelope
 * - `auto_remediate_resume`: `{ reason }` — the doctor auto-remediator
 *   (EB-211) resumed a stuck health-probe-degraded subscriber; `subId`
 *   carries the subscriber
 * - `auto_remediate_linger_pruned`: `{ pruned: <count>, reason }` — the
 *   remediator dropped expired handoff-linger windows; `groupId` carries
 *   the group
 * - `auto_remediate_rebalance`: `{ reason }` — the remediator forced a
 *   rebalance (`trigger: 'remediate'`) for a skewed partitioned group;
 *   `groupId` carries the group
 * - `auto_remediate_run`: `{ dryRun, healthBefore, planned, applied,
 *   capped, maxActions }` — one `autoRemediate()` run's summary,
 *   including dry-run previews
 * - `auto_remediate_revised`: `{ intervalMs, oldIntervalMs, maxActions,
 *   oldMaxActions, dryRun, oldDryRun, doctor, oldDoctor }` — the bus-level doctor
 *   auto-remediation schedule was revised (EB-306, doctor tuning EB-360; only a bus that
 *   already enabled `autoRemediate` can be revised — a never-enabled
 *   bus refuses until the opt-in is set at construction, since it holds
 *   no schedule at all; the new values reuse the construction-time
 *   validation (`assertValidRemediateSchedule`: the interval a positive
 *   finite number, maxActions a non-negative integer), plus a boolean
 *   check on `dryRun` and a positive-finite check on each provided
 *   doctor threshold, so a failed call leaves the live schedule
 *   untouched, and only the fields provided move — a partial revision
 *   keeps the fields it leaves out, and an illegal value in any field
 *   leaves every live value exactly as it was; the `doctor` pair
 *   carries the parsed thresholds (the doctor defaults when never
 *   configured), and only the scheduled run reads the stored tuning —
 *   a manual `autoRemediate(options)` still diagnoses with its own
 *   per-call `options?.doctor`, and `exportBusHealth(options?)` with
 *   its own per-call options — and a moved interval clears and re-arms
 *   the timer at the new interval with no extra run fired, while a
 *   maxActions/dryRun/doctor-only revision leaves the pending run's
 *   countdown untouched and takes effect on the next run; a revision
 *   that provides neither field, and a revision identical to the
 *   current schedule, are both no-ops with no audit)
 * - `ops_alert_revised`: `{ dedupWindowMs, oldDedupWindowMs, maxAttempts, oldMaxAttempts, retryBaseMs, oldRetryBaseMs, gatewayDisconnectThreshold, oldGatewayDisconnectThreshold, gatewayDisconnectWindowMs, oldGatewayDisconnectWindowMs }` — the bus-level ops-alert tuning was revised (EB-307; only a bus that already enabled `opsAlert` can be revised — a never-enabled bus refuses until the outlet is opted into at construction, since it holds no alerter at all; the new values reuse the construction-time validation (`resolveOpsAlerterOptions`: the windows and the backoff base positive finite numbers, the attempts and the threshold positive integers), so a failed call leaves the live tuning untouched, and only the fields provided move — a partial revision keeps the fields it leaves out, and an illegal value in any field leaves every live value exactly as it was; only the five numeric bounds move — the `sink` / `secret` / `keyId` / `busId` are untouched, and neither is the `now` clock — and in-flight state is untouched, so the new values govern from the next note onward — the next dedup check reads the live window, the next disconnect note truncates at the live storm window and trips at the live threshold, and the next delivery attempt budgets at the live maxAttempts and backs off at the live retryBaseMs (a backoff timer already armed keeps the delay it was armed with); a revision that provides neither field, and a revision identical to the current tuning, are both no-ops with no audit)
 * - `ops_alert_secret_rotated`: `{ keyId, oldKeyId }` — the bus-level ops-alert signing secret was rotated live (EB-397; only a bus that already enabled `opsAlert` can be rotated — a never-enabled bus refuses until the outlet is opted into at construction, since it holds no alerter at all; the new secret reuses the construction-time validation (`resolveOpsAlerterOptions`: the secret a non-empty string or Buffer, the key id a non-empty string when given), so a failed call leaves the live identity untouched, and omitting the key id keeps the live key id; a rotation whose secret bytes and effective key id both match the live values is a no-op with no audit; the new secret signs from the next note onward while a delivery already signed keeps its old signature through its bounded retry chain, and the dedup table and the storm state are untouched; the secret value is never written to the audit log — only the key ids are recorded, with an absent key id recorded as null — and there is no bus-side dual-key grace window, so the receiver must deploy the new secret before the bus rotates)
 * - `admission_webhook_revised`: `{ timeoutMs, oldTimeoutMs, failureMode, oldFailureMode, headersChanged }` — the bus-level external admission webhook tuning was revised (EB-308; only a bus that already enabled `admissionWebhook` can be revised — a never-enabled bus refuses until the hook is opted into at construction, since it holds no endpoint at all; the new values reuse the construction-time validation (`validateAdmissionWebhookOptions`: the timeout a positive finite number, the failure mode `'fail-open'`/`'fail-closed'`, the headers a string-to-string map), so a failed call leaves the live tuning untouched, and only the fields provided move — a partial revision keeps the fields it leaves out, and an illegal value in any field leaves every live value exactly as it was; only `timeoutMs` / `failureMode` / `headers` move — the `url` is untouched — and a revision swaps in a fresh merged options object, so the new timeout governs from the next `decideAdmission` call, the new failure mode from the next timeout or transport failure, and the new headers from the next decision request, while a publish already blocked in its webhook call keeps the timeout it was called with; header values are never written to the audit log (they may carry bearer tokens) — only whether the header set moved is recorded; a revision that provides neither field, and a revision identical to the current tuning, are both no-ops with no audit)
 * - `publish_audit_revised`: `{ maxEntries, oldMaxEntries, maxBytes, oldMaxBytes }` — the bus-level publish audit journal tuning was revised (EB-309; only a bus that already enabled `publishAudit` can be revised — a never-enabled bus refuses until the journal is opted into at construction, since it holds no log at all; the new values reuse the construction-time validation (the in-memory bound a positive integer, the live-file byte budget a positive finite number of bytes), so a failed call leaves the live tuning untouched, and only the fields provided move — a partial revision keeps the fields it leaves out, and an illegal value in any field leaves every live value exactly as it was; only `maxEntries` / `maxBytes` move — the journal directory and the clock are untouched — and in-flight entries are retained, never cleared or rewritten: shrinking `maxEntries` does not evict immediately (the next record evicts down to the live bound) and a revised `maxBytes` is honored by the next record's rotation check, so a tightened budget may rotate the live file at the next record while a loosened one simply defers the next rotation; a revision that provides neither field, and a revision identical to the current tuning, are both no-ops with no audit)
 * - `priority_lanes_revised`: `{ weights, oldWeights, bulkStarvationRounds, oldBulkStarvationRounds }` — the bus-level priority-lane tuning was revised (EB-310; lanes are always on — every subscriber queue carries them, so there is no never-enabled bus to refuse; the new values reuse the construction-time validation (`resolvePriorityLanesOptions`: every weight a positive integer, `bulkStarvationRounds` a positive integer), so a failed call leaves the live tuning untouched, and only the fields provided move — a partial revision keeps the fields it leaves out, and a partial `weights` object moves only the lanes it names, and an illegal value in any field leaves every live value exactly as it was; the revision fans out to every in-flight subscriber queue and replaces the bus's own live values, so a subscriber created after the revision builds its queue from the new tuning — and in-flight entries are retained, never cleared or reordered: the new weights govern from the next drain round, and a bulk starvation counter already counting keeps counting against the live threshold, so a tightened threshold may force-interleave on the very next round while a loosened one simply defers it; a revision that provides neither field, and a revision identical to the current tuning, are both no-ops with no audit)
 * - `trace_revised`: `{ sampleRate, oldSampleRate, bufferSize, oldBufferSize }` — the bus-level head-based trace tuning was revised (EB-311; only a bus that already enabled `trace` can be revised — a never-enabled bus refuses until tracing is opted into at construction, since it holds no recorder at all; the new values reuse the construction-time validation (`resolveTraceOptions`: the rate a finite number in [0, 1], the buffer a positive integer), so a failed call leaves the live tuning untouched, and only the fields provided move — a partial revision keeps the fields it leaves out, and an illegal value in any field leaves every live value exactly as it was; only `sampleRate` / `bufferSize` move — the `sampler`, the `onTraceSpan` callback, and the `archive` journal are untouched — and in-flight trace records are untouched, so the new values govern from the next decision onward — the next `sample()` reads the live rate, and the next span write wraps at the live capacity, while shrinking the buffer does not trim the ring at retune time (the buffered spans stay buffered until the next span covers the excess down to the live bound) and growing it keeps every buffered span in chronological order; a revision that provides neither field, and a revision identical to the current tuning, are both no-ops with no audit)
 * - `dedup_bloom_revised`: `{ expectedInsertions, oldExpectedInsertions, falsePositiveRate, oldFalsePositiveRate }` — the bus-level dedup bloom sizing was revised (EB-312; only a bus that already enabled `dedupBloom` can be revised — a never-enabled bus refuses until the pre-filter is opted into at construction, since it holds no filter at all; the new values reuse the construction-time validation (`resolveDedupBloomOptions`: the sizing an integer >= 1, the rate in the open interval (0, 1)), so a failed call leaves the live filter untouched, and only the fields provided move — a partial revision keeps the fields it leaves out, and an illegal value in any field leaves every live value exactly as it was; the revision builds a fresh filter at the merged sizing and rebuilds it from the live dedup table — the map stays authoritative, so no dedup decision is lost and the insertion count restarts from the live table size (zero when the table is empty), dropping stale bits — and the new filter governs from the next probe onward; a revision that provides neither field, and a revision identical to the current sizing, are both no-ops with no audit)
 * - `lag_slo_revised`: `{ maxLagMessages?, oldMaxLagMessages?, maxLagMs?, oldMaxLagMs?, breachSamples, oldBreachSamples }` — the bus-level consumer-lag SLO tuning was revised (EB-314; only a bus that already enabled `groupLag.slo` can be revised — a never-enabled bus refuses until the SLO is opted into at construction, since it holds no SLO at all; the new values reuse the construction-time validation (`resolveLagSloOptions`: each bound a finite number `>= 0`, at least one bound present, `breachSamples` an integer `>= 1`), so a failed call leaves the live tuning untouched, and only the fields provided move — a partial revision keeps the fields it leaves out, and an illegal value in any field leaves every live value exactly as it was; only the three numeric bounds move — the `onLagSloBreach` callback (and its identity, including the ops-alert outlet wrap) is untouched — and in-flight episode state is preserved, so the new values govern from the next lag evaluation onward — a tightened bound may fire on the very next sample, a loosened one simply defers it, and a new `breachSamples` counts from the next consecutive sample without resetting the in-flight consecutive count, latched breach, or episode start; a bound's old value is omitted from the audit when it was not configured; a revision that provides neither field, and a revision identical to the current tuning, are both no-ops with no audit)
 * - `trace_archive_revised`: `{ maxEntries, oldMaxEntries, maxBytes, oldMaxBytes }` — the bus-level trace span archive journal tuning was revised (EB-315; only a bus that already enabled `trace.archive` can be revised — a never-enabled bus refuses until the journal is opted into at construction, since it holds no journal at all; the new values reuse the construction-time validation (`assertValidTraceArchiveOptions`: the in-memory bound a positive integer, the live-file byte budget a positive finite number of bytes), so a failed call leaves the live tuning untouched, and only the fields provided move — a partial revision keeps the fields it leaves out, and an illegal value in any field leaves every live value exactly as it was; only `maxEntries` / `maxBytes` move — the journal directory and the clock are untouched — and in-flight entries are retained, never cleared or rewritten: shrinking `maxEntries` does not evict immediately (the next record evicts down to the live bound) and a revised `maxBytes` is honored by the next record's rotation check, so a tightened budget may rotate the live file at the next record while a loosened one simply defers the next rotation; a revision that provides neither field, and a revision identical to the current tuning, are both no-ops with no audit)
 * - `idempotency_revised`: `{ windowMs, oldWindowMs, maxEntries, oldMaxEntries }` — the bus-level idempotency dedup tuning was revised (EB-316; idempotency is always on — there is no never-enabled bus to refuse; the new values reuse the construction-time validation (`assertValidIdempotencyOptions`: the window a positive finite number of milliseconds, the cap a positive integer), so a failed call leaves the live tuning untouched, and only the fields provided move — a partial revision keeps the fields it leaves out, and an illegal value in any field leaves every live value exactly as it was; only `windowMs` / `maxEntries` move — the dedup table contents and the bloom pre-filter are untouched — and in-flight entries are retained, never cleared or rewritten: the EB-27 `(topic, messageId)` table reads both values live, so a tightened window expires old entries at the next dedup check and a tightened cap evicts the oldest down to the new bound at the next claim, while a widened window re-judges old entries under the new window and a widened cap simply admits more; content-dedup and explicit-key claims (EB-146/EB-151) keep the window they were claimed under — each claim stores its own `windowMs` at claim time, so a revised bus window governs only claims made after the revision, while their eviction still follows the live cap; a revision that provides neither field, and a revision identical to the current tuning, are both no-ops with no audit)
 * - `lineage_revised`: `{ maxLineageHops, oldMaxLineageHops }` — the bus-level lineage hop cap was revised (EB-317; lineage is always on — there is no never-enabled bus to refuse; the new value reuses the construction-time validation (`resolveMaxLineageHops`: a positive integer), so a failed call leaves the live cap untouched; only the cap moves — the lineage records themselves are never rewritten, hop collection is unchanged — and every admission check (the formal check and the `publishAtomic` shadow check) reads the cap live, so a tightened cap rejects a chain that already reached the new bound at the next admission while a widened one admits a chain the old cap would have depth-rejected; messages already admitted keep their lineage — an in-flight message is never re-judged against the new cap, and a message already rejected is never revived; a revision that provides no field, and a revision identical to the current cap, are both no-ops with no audit)
 * - `drop_journal_revised`: `{ maxEntries, oldMaxEntries }` — the bus-level drop journal retention bound was revised (EB-318; the drop journal is always on — there is no never-enabled bus to refuse; the new value reuses the construction-time validation (`assertValidDropJournalMaxEntries`: a positive integer), so a failed call leaves the live bound untouched; only the bound moves — the drop determination itself is untouched, drops are still decided by each queue path — and the rows already recorded are retained, never cleared or rewritten: shrinking does not evict immediately (the next record evicts the oldest rows down to the live bound) while a widened bound simply retains more, and lifetime totals (`total` / `byReason`) are unaffected by the revision; a revision that provides no field, and a revision identical to the current bound, are both no-ops with no audit)
 * - `pull_buffer_revised`: `{ pullBufferSize, oldPullBufferSize }` — the bus-level pull buffer bound was revised (EB-319; pull is always on — there is no never-enabled bus to refuse; the new value reuses the construction-time validation (`assertValidPullBufferSize`: a positive integer), so a failed call leaves the live bound untouched; only the bound moves — the pull mechanism itself is untouched, offsets stay caller-owned, long-poll waiters keep re-reading from their own `fromOffset`, and pull stays independent of push-ACK — and the entries already buffered are retained, never cleared or rewritten: shrinking does not evict immediately (the next publish on a topic truncates that topic's ring down to the live bound) while a widened bound simply retains more; only buses without `durableLogDir` are affected in what `pull()` returns — with a durable log `pull()` reads the log, not the ring; a revision that provides no field, and a revision identical to the current bound, are both no-ops with no audit)
 * - `fan_out_chunk_size_revised`: `{ fanOutChunkSize, oldFanOutChunkSize }` (`null` when fully-synchronous) — the bus-level fan-out chunk size was revised (EB-320; chunking is always available — there is no never-enabled bus to refuse; the new value reuses the construction-time validation (`assertValidFanOutChunkSize`: a positive integer, or `Infinity` for the fully-synchronous default), so a failed call leaves the live size untouched; only the size moves — the fan-out itself is untouched, matching, filtering, signing, backpressure, and per-subscriber FIFO are unchanged, and barrier control messages still bypass chunking — and the new size governs from the next publish: the fan-out captures the live size at publish start, so a chain already in flight keeps the size it started with and runs its scheduled microtask chunks to completion at that size; a revision that provides no field, and a revision identical to the current size (including `Infinity` against the fully-synchronous default), are both no-ops with no audit)
 * - `subscription_audit_revised`: `{ maxEntries, oldMaxEntries }` — the bus-level subscription audit retention bound was revised (EB-321; the subscription audit log is always on — the bus always constructs it, so there is no never-enabled bus to refuse; the new value reuses the construction-time validation (`assertValidSubscriptionAuditMaxEntries`: a positive integer), so a failed call leaves the live bound untouched; only the bound moves — the audit chain itself is untouched (`dir` / `now` and the hash chain head are unchanged) — and the entries already recorded are retained, never cleared or rewritten: shrinking does not evict immediately at the log level (the next `append` evicts the oldest rows down to the live bound) while a widened bound simply retains more, and the revision event itself is appended to the same log under the new bound, so a shrink is already reflected in the in-memory index when the call returns — the revision event is the record that evicts the oldest rows down to the new bound; evicted rows only leave the in-memory view (the on-disk files are never rewritten, and restart recovery still truncates at the live bound) and the hash chain stays valid — the oldest retained event is the segment anchor; a revision that provides no field, and a revision identical to the current bound, are both no-ops with no audit)
 * - `durable_log_max_entries_revised`: `{ maxEntriesPerTopic, oldMaxEntriesPerTopic }` — the bus-level durable-log per-topic entry budget was revised (EB-322; only a bus with `durableLogDir` can be revised — a bus without one refuses with `RangeError`, since it holds no durable log at all (unlike the construction-time option, which is silently ignored without a dir); the new value reuses the construction-time validation (`assertValidDurableLogMaxEntriesPerTopic`: a positive integer), so a failed call leaves the live bound untouched; only the bound moves — the retention mechanism itself is untouched (the amortized 2x compact trigger, the keyed-compaction rule, and the offset/dedup journal guards all keep reading the bound live) — and the records already persisted are retained, never cleared or rewritten: tightening does not force an immediate compact (the next append past twice the new bound rewrites the file down to the newest entries) while a widened bound simply allows more capacity; the revision fans out to the root log and every namespace child log, and a namespace registered later opens its child log with the live bus bound; a revision identical to the current bound is a no-op with no audit)
 * - `delayed_capacity_revised`: `{ maxEntries, oldMaxEntries }` (`null` when a bound is unlimited, since JSON has no `undefined`) — the bus-wide delayed-delivery capacity budget was revised (EB-384; the budget is always present — `undefined` means unlimited — so there is no never-enabled bus to refuse; the new value reuses the construction-time validation (`maxDelayedEntries`: a non-negative integer), so a failed call leaves the live budget untouched; only the bound moves — the pending schedules themselves are untouched, tightening never evicts them (they drain naturally and new schedules are admitted again once the pending count drops below the new bound) while loosening governs from the very next schedule attempt — and the revision itself is live-only (a restart restores the construction-time option value, and restart recovery rebuilds persisted schedules without consulting the budget); a revision identical to the current budget (including clearing an already-unlimited budget) is a no-op with no audit)
 * - `topic_delayed_capacity_revised`: `{ topic, maxEntries, oldMaxEntries }` (`null` when a bound is unlimited, since JSON has no `undefined`) — the per-topic delayed-delivery capacity budget for one concrete topic was revised (EB-385; the revision itself is live-only — a restart clears every per-topic budget, unlike the bus-wide budget which restores its construction-time option value — and restart recovery rebuilds persisted schedules without consulting any budget; tightening never evicts the topic's pending schedules (they drain naturally and new schedules are admitted again once the topic's pending count drops below the new budget) while loosening governs from the very next schedule attempt; a revision identical to the current budget for the topic (including clearing an already-unlimited topic) is a no-op with no audit)
 * - `key_delayed_capacity_revised`: `{ topic, key, maxEntries, oldMaxEntries }` (`null` when a bound is unlimited, since JSON has no `undefined`) — the per-key delayed-delivery capacity budget for one concrete (topic, key) pair was revised (EB-389; the revision itself is live-only — a restart clears every per-key budget, like the per-topic budgets — and restart recovery rebuilds persisted schedules without consulting any budget; tightening never evicts the pair's pending schedules (they drain naturally and new schedules are admitted again once the pair's pending count drops below the new budget) while loosening governs from the very next schedule attempt; unkeyed schedules are never subject to the per-key gate; a revision identical to the current budget for the pair (including clearing an already-unlimited pair) is a no-op with no audit)
 * - `topic_durable_log_max_age_revised`: `{ topic, maxAgeMs, oldMaxAgeMs }` (`null` when a bound is unset, since JSON has no `undefined`) — the per-topic durable-log age-retention bound for one concrete topic was revised (EB-388; the revision itself is live-only — a restart clears every per-topic override, unlike the bus-wide bound which restores its construction-time option value — and the first revision of a topic reports the live global bound as its previous value, since that is what the topic was following; tightening never forces an immediate compact (the next append-triggered compact evicts under the new bound) while loosening governs from the very next compact; an overridden topic no longer follows later global `setDurableLogMaxAgeMs` moves (the override pins it) until cleared, while uncovered topics keep following the live global value; seq-0 delayed-delivery schedule records are never age-evicted, under an override exactly as under the global bound; a revision identical to the topic's current override (including clearing a topic that holds no override) is a no-op with no audit)
 * - `rpc_settled_retention_revised`: `{ previous: { ringSize, ttlMs }, current: { ringSize, ttlMs } }` — the RPC settled-ring retention was revised (EB-391; the settled ring is always on — there is no never-enabled bus to refuse; the new values reuse the module defaults' validation (the ring a positive integer, the TTL a positive finite number of milliseconds), so a failed call leaves the live retention untouched, and only the fields provided move — a partial revision keeps the fields it leaves out, and an illegal value in either field leaves both live values exactly as they were; only the retention moves — the per-call `timeoutMs`, the response routing decision, and `rpcPending` are untouched — and the entries already settled keep the `settledAt` they were recorded with, never rewritten: the new bounds govern from the next prune, so a tightened ring evicts the oldest entries down to the new bound and a tightened TTL forgets entries older than the new TTL at that next prune, while a widened ring only affects future evictions; the revision itself is live-only — a restart restores the module defaults, since there is no construction-time option for either bound; a revision that provides neither field, and a revision identical to the current retention, are both no-ops with no audit)
 * - `publish_rate_ring_capacity_revised`: `{ capacity, oldCapacity }` — the per-topic publish-rate ring capacity was revised (EB-396; the rate table is always on — there is no never-enabled bus to refuse; the new value reuses the construction-time validation (a positive integer), so a failed call leaves the live capacity untouched; only the capacity moves — the window widths, `HOT_TOPICS_LIMIT`, and the sample hot path are untouched — and existing rings are rebuilt in place keeping their newest `min(len, capacity)` samples, never cleared or rewritten: a tightened capacity drops the oldest samples first, so the 5m rate caps at `capacity / 300` msg/s while the narrower windows stay exact much longer, and a widened capacity keeps every sample the old ring held; new topics build their ring at the live capacity from the next publish; `getStats().rates` and `hotTopics` read the live rings, so both reflect the revision immediately; the revision itself is live-only — a restart restores the 60_000 default, since there is no construction-time option; a revision identical to the current capacity is a no-op with no audit)
 * - `namespace_options_revised`: `{ prefix, allowPublish, oldAllowPublish, allowSubscribe, oldAllowSubscribe }` — one namespace's admission flags were revised live (EB-386; the revision is partial — only the flags provided move, the rest keep their live values — and the new values reuse the create-time validation (`resolveNamespaceOptions`: each flag a boolean), so a failed call leaves the live flags untouched; only the flags move — the namespace registration, its child durable log, its subscribers, and its quota rules are untouched — and the admission gates read the stored flags live on every publish/subscribe call, so the new flags govern from the very next admission while in-flight deliveries and already-established subscriptions are unaffected (denying subscribe blocks only new subscriptions, never evicting existing subscribers); a revision identical to the live flags is a no-op with no audit; the revision itself is live-only — a restart restores the `createNamespace` construction values, since flag revisions are never written into snapshots)
 * - `composite_patterns_revised`: `{ patterns, exclude, mode, oldPatterns, oldExclude, oldMode }` (`null` when the composite was cleared) — one composite subscription's include/exclude patterns were revised live (EB-387; the `subId` / `pattern` ride on the event itself, like `lag_monitor_revised`; the revision is partial — only the fields provided move, the rest keep their live values — and the merged values reuse the subscribe-time validation (`resolveCompositeOptions`, including the exclude-covers-include fail-fast), so a failed call leaves the live state untouched; only the composite state moves — the base pattern, the queue, backpressure, dedup, and lag state are untouched — and matching is evaluated live per message, so the new state governs from the next message awaiting enqueue while already-queued messages are never re-judged or reordered and replay filters its remaining records under the new state; `null` clears the composite (the subscription degrades to an ordinary single-pattern subscription and later revisions are refused until it opts in again at subscribe time); a revision identical to the live patterns is a no-op with no audit)
 * - `durable_log_max_bytes_revised`: `{ from: <previous maxBytesPerTopic, null when unset>, to: <new maxBytesPerTopic, null when cleared> }` — the bus-level durable-log per-topic byte budget was revised (EB-323; only a bus with `durableLogDir` can be revised — a bus without one refuses with `RangeError`, since it holds no durable log at all (unlike the construction-time option, which is silently ignored without a dir); the new value reuses the construction-time validation (`assertValidDurableLogMaxBytesPerTopic`: `undefined` or a positive finite number of bytes), so a failed call leaves the live bound untouched; `undefined` clears the byte bound and returns to count-only retention; only the bound moves — the retention mechanism itself is untouched (the amortized 2x compact trigger, the rotation trigger, and the newest-first byte-budget fill all keep reading the bound live) — and the records already persisted are retained, never cleared or rewritten: tightening does not force an immediate compact (with rotation off the next append past twice the new bound rewrites the file down to the newest records whose bytes fit; with rotation on crossing the bound archives the live segment instead) while a widened bound simply allows more capacity; the revision fans out to the root log and every namespace child log, and a namespace registered later opens its child log with the live bus bound; a revision identical to the current bound (including clearing an already-unset bound) is a no-op with no audit)
 * - `durable_log_publish_dedup_max_entries_revised`: `{ publishDedupMaxEntries, oldPublishDedupMaxEntries }` — the bus-level durable-log publish-dedup journal bound was revised (EB-328; only a bus with `durableLogDir` can be revised — a bus without one refuses with `RangeError`, since it holds no durable log at all (unlike the construction-time option, which is silently ignored without a dir); the new value reuses the construction-time validation (`assertValidDurableLogPublishDedupMaxEntries`: a positive integer), so a failed call leaves the live bound untouched; only the bound moves — the dedup mechanism itself is untouched (the amortized 2x compact trigger keeps reading the bound live, claim identities and their suppression semantics are unchanged) — and the claims already persisted are retained, never cleared or rewritten: tightening does not force an immediate journal rewrite (the next claim past twice the new bound compacts the journal down to the newest claims) while a widened bound simply allows more capacity; the revision fans out to the root log and every namespace child log, and a namespace registered later opens its child log with the live bus bound; a revision identical to the current bound is a no-op with no audit; the bound starts at the construction-time `idempotencyMaxEntries` and evolves independently of it afterwards — `setIdempotency` never moves it)
 * - `durable_log_segment_rotation_revised`: `{ from: { maxEntriesPerSegment, maxSegmentAgeMs }, to: { maxEntriesPerSegment, maxSegmentAgeMs } }` (`null` when a trigger is unset) — the bus-level durable-log segment-rotation triggers were revised (EB-355; only a bus with `durableLogDir` can be revised — a bus without one refuses with `RangeError`, since it holds no durable log at all (unlike the construction-time option, which is silently ignored without a dir); a bus constructed without `durableLogSegmentRotation` also refuses with `RangeError`, since rotation cannot be enabled after the fact — the live segment's identity is only tracked while rotation is on; the new triggers reuse the construction-time validation (`resolveSegmentRotationOptions`: at least one trigger, a positive-integer entry budget, a positive-finite age budget), so a failed call leaves the live triggers untouched; only the triggers move — the archiving/offload configuration is untouched, the in-flight live segment keeps its original start time and counted entries, and the new triggers govern from the next append (a tightened entry budget rotates on that next append, the age budget is judged on record timestamps) — and the records already persisted are retained, never cleared or rewritten; a partial revision moves only the fields provided (passing a field as `undefined` clears that trigger, as long as at least one trigger remains); the revision fans out to the root log and every namespace child log, and a namespace registered later opens its child log with the live bus triggers; a revision identical to the current triggers, or a patch with neither field, is a no-op with no audit)
 * - `durable_log_readback_cache_revised`: `{ readbackCacheMaxSegments, oldReadbackCacheMaxSegments, readbackCacheMaxBytes, oldReadbackCacheMaxBytes }` — the bus-level durable-log S3 readback-cache bounds were revised (EB-361; only a bus with `durableLogDir` can be revised — a bus without one refuses with `RangeError`, since it holds no durable log at all (unlike the construction-time option, which is silently ignored without a dir); a bus constructed without `durableLogSegmentOffload` also refuses with `RangeError`, since offload cannot be enabled after the fact — there is no S3 client or remote key space for the cache to serve; the new bounds reuse the construction-time validation (both bounds non-negative integers), so a failed call leaves the live bounds untouched; only the two bounds move — the S3 client, key prefix, namespace, `retainLocalSegments`, and segment rotation are untouched; tightening prunes the cache to the new bounds at once (least-recently-used first, until both bounds hold) while loosening governs from the next insert; a bound of 0 disables the cache entirely and a single segment larger than the byte bound is never cached; the revision applies to the root log and skips namespace child logs, which do not offload; a partial revision moves only the fields provided; a revision identical to the current bounds, or a patch with neither field, is a no-op with no audit)
 * - `compression_auto_train_revised`: `{ topicPattern, autoTrain, oldAutoTrain }` (`autoTrain` is `null` when the trainer was retired; the `onTrained` callback is recorded only as `'set'`, never serialized) — one compression rule's auto-train parameters were revised live (EB-369; only a rule that currently has an auto-trainer can be revised — an unknown pattern, a rule without `autoTrain`, or a retired trainer refuses with `RangeError`, since there is nothing live to retune; a `null` patch retires the trainer entirely — its timers are cleared, its map entry removed, and the rule's `autoTrainState` cleared — and a later revision is refused until `setTopicCompression` opts in again; the new values reuse the construction-time validation (`resolveAutoTrainOptions`: `sampleCount` a positive integer, `sampleMaxBytes` a positive finite number, triggers positive when given, `onTrained` a function), so a failed call leaves the live parameters, samples, timers, and audit untouched; only the five parameters move — the trainer instance, sample window, counters, `version`, `dictionaryId`, `lastTrainedAt`, and the compression rule fields are untouched — and a partial revision moves only the fields provided, with an explicit `null` on a trigger clearing that trigger (`onTrained` can only be replaced, never cleared); trigger interplay follows the construction-time default rule — setting `retrainEveryMessages` without `retrainEveryMs` clears the cadence, clearing `retrainEveryMessages` without `retrainEveryMs` falls back to the 60_000 ms default, and leaving both untouched keeps both live values; tightening `sampleCount` / `sampleMaxBytes` prunes the window at once (oldest first, newest sample always kept) while loosening governs from the next sample, a lowered `retrainEveryMessages` trains on the next sample once `sinceLastTrain` reaches it, and `retrainEveryMs` re-arms the cadence timer from the revision moment only when its resolved value actually changed; a revision identical to the current five parameters is a no-op with no audit)
 * - `durable_log_retain_local_segments_revised`: `{ retainLocalSegments, oldRetainLocalSegments }` (`null` when a count is `Infinity`, since JSON has no `Infinity`) — the bus-level durable-log S3 offload local-retain segment count was revised (EB-362; only a bus with `durableLogDir` can be revised — a bus without one refuses with `RangeError`, since it holds no durable log at all (unlike the construction-time option, which is silently ignored without a dir); a bus constructed without `durableLogSegmentOffload` also refuses with `RangeError`, since offload cannot be enabled after the fact — there is no S3 client or remote manifest for the retention to serve; the new count reuses the construction-time validation (a non-negative integer or `Infinity`), so a failed call leaves the live count untouched; only the retain count moves — the S3 client, key prefix, namespace, readback-cache bounds, and segment rotation are untouched; tightening trims local segments to the new bound at once (least-recently-used first, only segments the remote manifest confirms as uploaded) while loosening governs from the next offload pass; a count of 0 keeps nothing local and `Infinity` restores full retention (the construction default); when the remote manifest cannot be read the value still moves and the trim is deferred to the next automatic offload pass, counted in `offloadFailed`; a dry-run S3 client never trims; the revision applies to the root log and skips namespace child logs, which do not offload; a revision identical to the current count is a no-op with no audit)
 * - `durable_log_encryption_key_rotated`: `{ keyId, oldKeyId }` — the bus-level durable-log at-rest encryption DEK was rotated live (EB-398; only a bus constructed with a `RotatingDekEncryptor` as `durableLogEncryptor` can be rotated — a bus without an encryptor refuses with `RangeError`, since encryption cannot be enabled after the fact, and a bus whose encryptor is a plain `AesGcmEncryptor`, `NoopEncryptor`, or any other ringless `PayloadEncryptor` refuses likewise, since there is no key ring to rotate; the new key id reuses the construction-time validation (`assertValidKeyId`: 1–64 `[A-Za-z0-9_-]`, unique within the ring), so a failed call leaves the live key untouched, and omitting the id takes the ring's smallest free `k<N>`; every rotation mints a fresh random DEK, so there is no same-value no-op and every call is audited; the new key seals from the next append onward while old sealed lines keep their envelope key id and are never re-sealed, and namespace child logs share the same ring reference so they follow automatically; the retired key keeps decrypting inside the ring's live grace window, and with the default grace of 0 an old line fails loudly at recovery once the wall clock has moved past the rotation and is counted in `corruptLines`, never returned as garbage — the operator must set a grace window on their ring reference before rotating if the old key must keep opening history; only the key ids are recorded — the key itself is never written to the audit log)
 * - `durable_log_segment_offload_client_revised`: `{ isDryRun, oldIsDryRun }` (`null` when a client carries no dry-run brand) — the bus-level durable-log S3 offload client was replaced live (EB-399; only a bus with `durableLogDir` can be revised — a bus without one refuses with `RangeError`, since it holds no durable log at all (unlike the construction-time option, which is silently ignored without a dir); a bus constructed without `durableLogSegmentOffload` also refuses with `RangeError`, since offload cannot be enabled after the fact — there is no S3 client or remote key space for the new client to serve; the new client reuses the construction-time validation (all five methods `putObject` / `getObject` / `headObject` / `listObjects` / `deleteObject` must be functions), so a failed call leaves the live client untouched; only the client moves — the key prefix, namespace, `retainLocalSegments`, readback-cache bounds, and segment rotation are untouched, and segments already uploaded are not re-pushed (the manifest entries stand — only later uploads and readbacks walk the new client); the new client governs from the next offload call (the next `offloadNow` pass, the next manifest load, the next remote readback), while a pass already in flight keeps the resolved offload object it started with — the replacement is in place on that same object, so its remaining calls still address the same key space; both clients must serve the same bucket and key space (the operator's precondition — a new client over a different key space would read an empty manifest and re-upload, and trimmed segments would read back as missing); a dry-run client rehearses only — the next pass records intended keys and never uploads, writes a manifest, or trims local segments — and swapping back to a real client resumes real uploads; installing the very same client object is a no-op with no audit; only the dry-run booleans are recorded — never an endpoint or a credential — and there is no getter that returns the client itself, since a real client holds credentials; the revision applies to the root log and skips namespace child logs, which do not offload)
* - `durable_log_key_compaction_revised`: `{ enabled, oldEnabled }` — the bus-level durable-log keyed-compaction flag was flipped (EB-364; only a bus with `durableLogDir` can be revised — a bus without one refuses with `RangeError`, since it holds no durable log at all (unlike the construction-time option, which is silently ignored without a dir); a bus constructed without keyed compaction can still enable it — the flip only changes the read view and rebuilds the in-memory key index from disk, and needs no construction-time structure (unlike offload or rotation, which cannot be enabled after the fact); a non-boolean flag throws `RangeError` with the live flag and the audit log untouched; enabling flips the read view to latest-per-key at once (superseded records stop replaying even before the file is rewritten) and rebuilds each log's key index and superseded counts from the records already on disk, so the next append counts the dead weight that predates the flip and the amortized compact trigger stays honest, while disabling is a view switch, not an un-delete (the read view returns to full history at once, appends stop maintaining the key index, and the superseded counts freeze); the per-key TTL gate (`setTopicKeyTtl`) follows the live flag, and stored TTL rules only sleep while disabled; the revision fans out to the root log and every namespace child log, and a namespace registered later opens its child log with the live bus flag; the live flag is visible in `getStats().durableLog.keyCompaction`; a revision identical to the current flag is a no-op with no audit)
 * - `state_store_max_keys_revised`: `{ maxKeys, oldMaxKeys, evictedKeys }` — the materialized state store's capacity bound was revised (EB-329; only a bus that already enabled the state store can be revised — a never-enabled bus refuses with `RangeError`, since it holds no table at all (unlike the construction-time option, which is silently inert without `enableStateStore`); the new value reuses the construction-time validation (`validateStateStoreOptions`: a positive integer), so a failed call leaves the live bound untouched; only the bound moves — the store mechanism itself is untouched (`apply` / `restore` keep reading the bound live) — and the rows already tracked are retained with their content, `version` and `seq` intact, except that tightening evicts the least-recently-updated rows down to the new bound immediately (`evictedKeys` counts the rows this revision dropped, and `getStats().stateStore.evictedKeys` accumulates them) while a widened bound simply raises the ceiling, and the changelog topic is untouched; a revision identical to the current bound is a no-op with no audit)
 * - `processing_latency_slo_alert_revised`: `{ enabled, oldEnabled }` — the bus-level processing-latency SLO alert switch was revised (EB-331; the switch is bus-wide and always present, so there is no never-enabled bus to refuse; the new value reuses the construction-time rule (`options?.processingLatencySloAlert === true` arms it), so a non-boolean value throws `RangeError` and leaves the live switch untouched; only the switch moves — `subscribe` reads the live value at subscription time, so the revision governs only subscriptions created after it (a `latencySlo` subscriber created while the switch is on builds a `sloPhase`, one created while it is off builds none) — and subscriptions already built are untouched: an armed subscriber keeps its `sloPhase` and keeps attributing misses after a disable, while an unarmed subscriber gains none after an enable; existing SLO statistics are untouched (the trackers, their samples, the `alerted` latches, and the `getStats()` shape are unchanged); a revision identical to the current switch is a no-op with no audit)
 * - `slow_start_revised`: `{ slowStart: { initialPerSec, rampMs, strategy } | null, oldSlowStart: { initialPerSec, rampMs, strategy } }` — one slow-starting subscriber's ramp parameters were revised (EB-332; only a subscriber that already opted into `slowStart` can be revised — a never-configured subscriber (or one whose completed gate a drain already dropped) refuses with `RangeError`, since it holds no gate at all; the new values reuse the subscribe-time validation (`resolveSlowStartOptions`: `initialPerSec` / `rampMs` positive finite numbers, `strategy` `'linear'` / `'exponential'`), so a failed call leaves the live gate untouched, and the revision is a full replacement — a field left out takes the documented default, not the live value — while `undefined` / `false` are not a clear (only an explicit `null` clears, returning the subscriber to full speed at once); only the ramp moves — the gate is replaced with a fresh one built from the new options, so the subscriber re-ramps from the revision moment (a gate whose window already elapsed but is still attached re-ramps the same way), the target is read live at revision time (the subscriber's `deliveryShaping` cap when configured, full speed otherwise), and the queued messages, the in-flight count, and the shaping bucket are untouched — and the drain reads the live gate, so the new ramp paces the next flush round; a revision that would build no gate (`initialPerSec` at or above the live target) is a documented no-op that leaves the live gate alone, and a revision identical to the current gate's parameters, field by field, is a no-op with no audit)
 * - `group_rebalance_debounce_revised`: `{ windowMs: <number> | null, oldWindowMs: <number> | null }` — one consumer group's rebalance-debounce window was revised (EB-333; `groupId` / `pattern` ride on the event itself, like `rebalance`; `null` means the group is undebounced; only a group the bus currently holds can be revised — a never-joined or fully-drained group refuses with `RangeError`, since a drained group's retained window configuration is re-fixed by its next generation's first member, not revised; the new value reuses the join-time validation (a finite number `>= 0`, `0` meaning no debounce), so a failed call leaves the live window untouched; only the window moves — the roster, the partition assignment, the assignor, and the member subscriptions are untouched, and no rebalance fires for the revision itself — and a window already armed keeps the timer it was armed with and flushes at the old value, while the new window governs the next window opened after the revision (a change arriving while a window is pending is absorbed into that pending window); `undefined` (or `0`) clears the window back to immediate rebalances, and a group that never debounced can be opted in this way; a revision identical to the current window, including clearing an already-undebounced group, is a no-op with no audit)
 * - `idle_window_revised`: `{ maxIdleMs: <number> | null, oldMaxIdleMs: <number> }` — one idle subscriber's reclamation window was revised (EB-334; only a subscriber that already opted into `maxIdleMs` can be revised — a never-opted-in subscriber, or one whose window was already cleared, refuses with `RangeError`, since it holds no live window at all (unlike the construction-time option, which is silently inert without `maxIdleMs`); the new value reuses the subscribe-time validation (a positive integer number of milliseconds), so a failed call leaves the live window untouched; only the window moves — the pending timer is cleared and a fresh window of the new size is armed from the revision moment, later activity re-arms with the new value, and an expiry after the revision reports the new `maxIdleMs` — and the `idleActivity` mode is untouched, the lease (if any) is untouched, and the queue is untouched; `null` clears the window (the subscriber keeps running with no idle expiry, and a later revision is refused until the subscriber opts in again at subscribe time, the EB-267 clear rule); a revision identical to the current window is a no-op with no audit and the pending timer is not re-armed)
 * - `idle_activity_revised`: `{ idleActivity: <mode>, oldIdleActivity: <mode> }` — one idle subscriber's activity mode was revised (EB-341; only a subscriber that already opted into `maxIdleMs` can be revised — a never-opted-in subscriber, or one whose window was already cleared, refuses with `RangeError`, since it holds no live window at all; the new value reuses the subscribe-time validation (`'deliver'` / `'ack'` / `'any'`), so a failed call leaves the live mode untouched; only the mode moves — both the subscriber record and the `IdleReclaimer` entry move together, so the next event is gated by the new mode and an expiry after the revision reports the new mode — and the pending timer is untouched, so an armed window keeps the remaining time it was armed with, while the window size, the lease (if any), and the queue are untouched; a revision identical to the current mode is a no-op with no audit and the pending timer is not re-armed)
 * - `group_handoff_linger_revised`: `{ handoffLingerMs: <number>, oldHandoffLingerMs: <number> }` — one consumer-group member's handoff-linger window was revised (EB-335; `subId` / `groupId` ride on the event itself; only a subscriber that is currently a group member can be revised — an unknown subscriber, or a plain subscriber with no `groupId`, refuses with `RangeError`; the new value reuses the subscribe-time validation (a finite number `>= 0`, `0` meaning no linger), so a failed call leaves the live window untouched; only the window moves — the member, its group, its assignment, its committed offsets, and the roster are untouched, and no rebalance fires for the revision itself — and the new value governs only departures after the revision (`beginLingerHandoff` reads the live value at leave time), so a window already armed keeps the `until` it was stamped with and its covered seqs stay covered until that stamped expiry; `0` closes future windows, and a member that never opted in can be opted in this way; a revision identical to the current window is a no-op with no audit)
 * - `group_auto_commit_revised`: `{ intervalMs: <number> | null, oldIntervalMs: <number> }` — one consumer group's offset auto-commit interval was revised (EB-344; `groupId` rides on the event itself; only a group that already opted into auto-commit can be revised — a never-opted-in group, or one whose last armed member already left, refuses with `RangeError`, since it holds no live interval at all (unlike the construction-time option, which is silently inert without `autoCommit`); the new value reuses the subscribe-time validation (a finite number of milliseconds `> 0`), so a failed call leaves the live interval untouched; only the interval moves — the roster, the partition assignment, the assignor, and the member subscriptions are untouched, and no rebalance fires for the revision itself — and every armed member's timer is re-armed in place at the new interval (the refcount is untouched and the pending checkpoints stay put, flushing on the next tick at the new cadence), while `null` flushes the pending checkpoints once at the old cadence first and then stops every armed member's timer, dropping the group's remembered interval so the next opt-in member re-fixes it; the revised interval is remembered per groupId, so a member that joins later and opts in must use the revised value (a different one fails to join with `RangeError`) while a member that does not opt in joins freely and runs no timer; a revision identical to the current interval is a no-op with no audit)
 * - `group_assignor_revised`: `{ assignor: <string>, oldAssignor: <string> }` — one partitioned consumer group's partition assignor was switched live (EB-346; `groupId` / `pattern` ride on the event itself, like `rebalance`; only a group the bus currently holds as partitioned can be revised — an unknown groupId, a group that never opted into partitions, or an ambiguous groupId refuses with `RangeError`, since a round-robin group has no placement to switch; the new value reuses the subscribe-time validation (a `PartitionAssignor` object with a non-empty string `name` and a function `assign`, or the string form `'rendezvous'` / `'sticky'`, or `null` to restore the default rendezvous), so a failed call leaves the live assignor untouched — and a custom assignor is trial-run against the live roster before anything moves (a throwing or invalid assignor aborts with the old assignor in place); only the assignor moves — the roster, the partition count, the committed offsets, the assignment watermarks, and the rebalance-debounce window are untouched — and the switch reuses the existing rebalance path (the cached assignment is invalidated, the new assignor recomputes with the pre-switch assignment as its `previous` input, each migrated partition's new owner replays its uncommitted backlog from the durable log, and an operator rebalance fires with `trigger: 'assignor'` and the debounce bypassed, so `PartitionRebalanceInfo.strategy` reports the new name), while a fully drained group records the switch by the audit entry alone and its next generation joins under the new assignor; a revision identical to the current assignor, by name, is a no-op with no audit)
 *   `group_offsets_seeked`: `{ groupId, pattern, targets, previous }` — one consumer group's committed offsets were repositioned by an operator seek (EB-376; `groupId` / `pattern` ride on the event itself, like `rebalance`; `targets` is the per-topic seek as passed (`'earliest'` / `'latest'` / explicit seq) and `previous` the pre-seek committed offset per targeted topic, `0` when nothing was committed — the seek moves the checkpoint through the `commitOffset` path (durable-log offset journal append, `OffsetStore` mirror, lag re-evaluation) and replays the durable log from the new position into every live member of the competing set, so a rewind redelivers under at-least-once semantics while a fast-forward to the live head replays nothing)
 * - `group_session_timeout_revised`: `{ instanceId: <string>, previous_timeout_ms: <number>, timeout_ms: <number> }` — one static consumer-group member's session timeout was revised live (EB-347; `groupId` / `pattern` ride on the event itself, like `rebalance`; only a member the bus currently holds a stored timeout for can be revised — an unknown group/pattern/instance, a dynamic member, or a member whose session already expired refuses with `RangeError`, since there is no stored timeout to revise; the new value reuses the subscribe-time validation (a finite number of milliseconds `> 0`), so a failed call leaves the live timeout untouched; only the timeout moves — the roster, the partition assignment, the assignor, and the member subscriptions are untouched, and no rebalance fires for the revision itself — and a member already in limbo has its live expiry timer re-armed from the revision moment at the new timeout (the renewLease from-now semantics), while an active member's new timeout takes effect from its next leave, and a rejoin that does not pass its own `sessionTimeoutMs` keeps the revised value; a revision identical to the current timeout is a no-op with no audit)
 * - `group_auto_offset_reset_revised`: `{ previous_policy: <'earliest' | 'latest' | null>, policy: <'earliest' | 'latest'> }` — one consumer group's auto.offset.reset default was revised live (EB-400; `groupId` / `pattern` ride on the event itself, like `rebalance`; `previous_policy` is the previously stored group default, `null` when none was stored; there is no existence gate — the default exists precisely for members that have not joined yet, so any groupId/pattern can hold one; the new value reuses the subscribe-time validation (`'earliest'` / `'latest'`), so a failed call leaves the live default untouched; only the default moves — the roster, the partition assignment, the committed offsets, and every live member are untouched, and no rebalance fires for the revision itself — and the new default governs only members that join after the revision and omit their own `autoOffsetReset` (a member-explicit option always wins, an explicit `resumeFromSeq` / `resumeFromTime` still wins over the policy, and a static rejoin inside its session window never resets), while the resetOnlyOnce gate is unchanged (once any committed offset exists the default never fires); a revision identical to the current default is a no-op with no audit)
 * - `group_auto_offset_reset_cleared`: `{ previous_policy: <'earliest' | 'latest'> }` — one consumer group's auto.offset.reset default was cleared live (EB-400; `groupId` / `pattern` ride on the event itself, like `rebalance`; clearing a group that holds no default is a no-op returning `false` with no audit; future members that omit their own `autoOffsetReset` fall back to `'earliest'` again)
 * - `group_default_session_timeout_revised`: `{ previous_timeout_ms: <number | null>, timeout_ms: <number> }` — one consumer group's static-membership session-timeout default was revised live (EB-403; `groupId` / `pattern` ride on the event itself, like `rebalance`; `previous_timeout_ms` is the previously stored group default, `null` when none was stored; there is no existence gate — the default exists precisely for members that have not joined yet, so any groupId/pattern can hold one; the new value reuses the join-time validation (a finite number of milliseconds `> 0`), so a failed call leaves the live default untouched; only the default moves — the roster, the partition assignment, the committed offsets, and every live member are untouched, and no rebalance fires for the revision itself — and the new default governs only members that join after the revision and omit their own `sessionTimeoutMs` (a member-explicit option always wins, an EB-347 per-instance revised value still wins over the default, and a member already in limbo keeps the timeout armed at join time), while the 30s module default still backs groups with no stored default; a revision identical to the current default is a no-op with no audit)
 * - `group_default_session_timeout_cleared`: `{ previous_timeout_ms: <number> }` — one consumer group's static-membership session-timeout default was cleared live (EB-403; `groupId` / `pattern` ride on the event itself, like `rebalance`; clearing a group that holds no default is a no-op returning `false` with no audit; future static members that omit their own `sessionTimeoutMs` fall back to the 30s module default again)
 * - `subscription_lease_revised`: `{ previous_lease_ms: <number> | null, lease_ms: <number> | null }` — one subscription's lease was armed, retuned, or cleared live (EB-348; a positive-integer `ms` arms a lease on a subscription that never opted in — expiry then unsubscribes through the subscription's outermost teardown, so a group member leaves the group and a reliable subscription clears its ack tracker — or re-arms an existing lease from the revision moment (the renewLease from-now semantics, so re-affirming the current duration still re-arms the timer and is audited); `null` clears the lease and the subscription keeps running with no lease expiry; the new value reuses the subscribe-time validation (a positive integer number of milliseconds), so a failed call leaves the live lease untouched; an unknown subscription id is not an error — the call returns `false` with nothing audited, the renewLease convention — and clearing a subscription that holds no lease is a no-op with no audit; only the lease moves — the queue, the idle window (if any), and the handler are untouched, and the expiry itself still fires `onLeaseExpired` and counts in `getStats().expiredLeases`)
 * - `stream_join_window_revised`: `{ joinId: <string>, windowMs: <number>, oldWindowMs: <number> }` — one stream join's sliding window was revised (EB-336; only a join the bus currently holds can be revised — a never-defined or removed join refuses with `RangeError`; the new value reuses the define-time validation (a positive finite number of milliseconds), so a failed call leaves the live window untouched; only the window moves — the join's topics, key function, flavor, output topic, and `maxBufferedPerKey` are untouched, and the buffered entries stay put — and the new value governs only arrivals after the revision (`observe` / `arrive` / `sweepJoin` read the window live), so a pair already derived is never re-derived or withdrawn, widening expands the pairing range and the lateness horizon while narrowing shrinks both but does not evict buffered entries immediately (the next observation or sweep trims them against the new horizon), the sweep timer is re-armed at `min(new windowMs, 60s)`, and `getStats().joins` reports the live window immediately; a revision identical to the current window is a no-op with no audit)
 * - `stream_join_max_buffered_per_key_revised`: `{ joinId: <string>, maxBufferedPerKey: <number>, oldMaxBufferedPerKey: <number> }` — one stream join's per-key buffer bound was revised (EB-345; only a join the bus currently holds can be revised — a never-defined or removed join refuses with `RangeError`; the new value reuses the define-time validation (a positive integer), so a failed call leaves the live bound untouched; only the bound moves — the join's topics, key function, flavor, output topic, and `windowMs` are untouched, and the buffered entries stay put — and the new value governs only arrivals after the revision (`arrive` reads the bound live), so a pair already derived is never re-derived or withdrawn, tightening does not evict buffered entries immediately (the capacity loop trims the key's list down to the new bound, oldest first, only on that key's next arrival) while loosening expands the capacity at once, the sweep timer is untouched (its interval depends only on `windowMs`), and `getStats().joins` reports the live bound immediately; a revision identical to the current bound is a no-op with no audit)
 * - `stream_join_how_revised`: `{ joinId: <string>, how: <string>, oldHow: <string> }` — one stream join's inner/left-outer flavor was revised (EB-383; only a join the bus currently holds can be revised — a never-defined or removed join refuses with `RangeError`; the new value reuses the define-time validation (`'inner'` / `'left'`), so a failed call leaves the live flavor untouched; only the flavor moves — the join's topics, key function, output topic, `windowMs`, and `maxBufferedPerKey` are untouched, and the buffered entries stay put — and the new value governs only arrivals after the revision (`arrive` reads the flavor live), so a pair already derived is never re-derived or withdrawn, a buffered left entry is never backfilled when the flavor flips to `'left'` (a left-outer row is derived only at arrival time), and a right arrival never derives a null row on either side of a flip; the sweep timer is untouched (its interval depends only on `windowMs`), and `getStats().joins` reports the live flavor immediately; a revision identical to the current flavor is a no-op with no audit)
 * - `allowed_lateness_revised`: `{ from: <previous allowedLatenessMs>, to: <new allowedLatenessMs> }` — the bus-wide default allowed lateness for event-time watermark tracking was revised (EB-324; watermark tracking is bus-wide and always on, so there is no never-enabled bus to refuse and no `durableLogDir` gate; the new value reuses the construction-time validation (`validateAllowedLatenessMs`: a non-negative finite number of milliseconds), so a failed call leaves the live default untouched; `undefined` is not a new default — the default is always a number, 0 when unconfigured — and throws `RangeError`; only the default moves — the watermark mechanism itself is untouched (per-topic overrides from `setTopicAllowedLateness` keep winning, exact-topic rule first then the earliest-registered matching pattern, the per-topic peak never rolls back, and messages already judged late stay late) — and the new default governs from the next event-time observation on every topic without an override, with the next `getStats()` watermark reading reflecting it immediately; a revision identical to the current default is a no-op with no audit)
 * - `auth_token_revised`: `{ gateway: 'ws' | 'graphql', changed: 'rotated' | 'cleared' }` — one gateway's bearer token was rotated or cleared live (EB-350; a non-empty string rotates, `null` clears — clearing disables auth, so a gateway that never armed auth records nothing for it; the token value itself is never recorded — only whether it changed or was cleared — and the new value governs only handshakes after the revision, so a connection that already handshook is never re-authenticated or disconnected and the old token stops working at the next handshake; the new value reuses the construction-time validation (a non-empty string, or `null` to clear), so a failed call leaves the live token untouched; a revision identical to the current token, including clearing an already-unauthenticated gateway, is a no-op with no audit)
 * - `group_rebalance_triggered`: `{ trigger: 'operator', members: <number> }` — an operator manually triggered a consumer-group rebalance (EB-351; `groupId` / `pattern` / `actor` ride on the event itself, like `rebalance`; only a group the bus currently holds can be triggered — an unknown groupId, or an ambiguous groupId without a pattern, refuses with `RangeError`; the trigger bypasses the rebalance debounce and fires immediately, so members observe it through `onRebalance`/`rebalanceListener` with `trigger: 'operator'` and the rebalance itself is audited as `rebalance`; only the trigger is recorded for a fully drained group — no rebalance fires, nobody is notified — and the `members` count is 0 there)
 * - `retention_updated`: `{ from: <previous maxAgeMs, null when unset>, to: <new maxAgeMs, null when cleared> }`
 * - `retained_rule_set`: `{ enabled: <boolean> }` (the `pattern` field carries the rule key)
 * - `retained_rule_cleared`: no detail (the `pattern` field carries the rule key)
 * - `retained_ttl_rule_set`: `{ ttlMs: <number> }` (EB-378; the `pattern` field carries the rule key)
 * - `retained_ttl_rule_cleared`: `{ ttlMs: <number> }` (EB-378; the `pattern` field carries the rule key, the removed rule's value)
 * - `retained_cleared`: `{ via: 'publish' }` (the `topic` field carries the concrete topic)
 * - `partition_rule_set`: `{ partitions: <count> }` (the `pattern` field carries the rule key)
 * - `partition_rule_cleared`: no detail (the `pattern` field carries the rule key)
 * - `partition_expansion`: `{ partitionsBefore, partitionsAfter, members }`
 *   (drained-group expansion; the live path audits `rebalance` instead)
 * - `topic_partition_scaled`: `{ partitionsBefore, partitionsAfter, groups: <groupId[]> }`
 *   (the `pattern` and `topic` fields carry the scaled topic)
 * - `topic_paused`: `{ mode: 'shed'|'queue' }` (the `pattern` field carries the rule key)
 * - `topic_resumed`: `{ mode: 'shed'|'queue', drainedMessages: <count> }` (the `pattern` field carries the rule key)
 * - `topic_pause_buffer_limit_revised`: `{ previous_max_buffered: <number | null>, max_buffered: <number | null> }` — one paused topic's queue-buffer bound was revised live (EB-401; the `pattern` field carries the rule key; `null` means unbounded on either side; only a topic the bus currently holds a pause rule for can be revised — an unknown pattern refuses with `RangeError`, since there is no rule to bound; the new value reuses the pause-time validation (a positive integer, or `null` to clear back to unbounded), so a failed call leaves the live bound untouched; only the bound moves — the rule's mode and its already-buffered messages are untouched, and no resume fires for the revision itself — and the new bound governs only admissions after the revision, so tightening never evicts buffered entries (they drain in order at resume) while loosening or clearing resumes buffering at once; a revision identical to the current bound is a no-op with no audit)
 * - `delayed_cancelled`: `{ cancelled: <count>, delayIds: <string[]>, topicPattern: <string> }`
 * - `recurring_job_cancelled`: `{ id, topic, cron }` — one live cron recurring job was cancelled by id (EB-371; the `topic` field carries the job's topic)
 * - `recurring_job_revised`: `{ id, previous_cron, cron, payloadFactoryReplaced }` — one live cron recurring job's schedule and/or payload factory was revised by id (EB-371; the `topic` field carries the job's topic)
 * - `publish_circuit_breaker_revised`: `{ topic, previous, current }` — one topic's publish circuit breaker thresholds were revised live (EB-372; `previous` / `current` each hold `{ consecutiveShedPublishes, halfOpenProbeIntervalMs }`; only the two thresholds move — the breaker's state, `consecutiveBlocked`, `openedAtMs`, and `rejectedMessages` are untouched, so an open breaker stays open and is not re-timed; a revision identical to the live thresholds is a no-op with no audit)
 * - `subscriber_breaker_revised`: `{ previous, current }` — the subscriber-side circuit breaker rule was revised live (EB-373; `previous` / `current` each hold `{ maxConsecutiveFailures, onTripped }` where `onTripped` is whether a callback is set; only the rule moves — every subscriber's streak and tripped flag are untouched, so an already-tripped subscriber stays tripped; a revision identical to the live rule is a no-op with no audit)
 * - `delayed_topic_held`: `{ topicPattern: <string> }` — the delayed-delivery sweep was held for the pattern (EB-250; the `pattern` field carries the rule key)
 * - `ack_deadline_extended`: `{ seq, extensionMs, deadlineMs }` — one outstanding reliable delivery's ack timeout was reset (EB-255; `subId` carries the subscriber, `deadlineMs` is the new bus-clock deadline)
 * - `ack_timeout_revised`: `{ ackTimeoutMs, oldAckTimeoutMs }` — a reliable subscriber's ack-timeout default for future deliveries was revised (EB-259; already-armed timers are untouched)
 * - `redelivery_backoff_revised`: `{ redeliveryBackoff: <normalized config or null>, oldRedeliveryBackoff: <normalized config or null> }` — a reliable subscriber's redelivery-backoff policy for future redeliveries was revised (EB-261; `null` on either side means "no backoff, requeue immediately"; already-armed backoff delays keep the delay they were armed with)
 * - `delivery_window_revised`: `{ deliveryWindows: <resolved windows or null>, oldDeliveryWindows: <resolved windows> }` — a windowed subscriber's delivery windows were revised (EB-263; resolved windows carry `startMin`/`endMin`/`days`/`timeZone` plus `maxHoldMs` when set; `null` means the windows were cleared back to all-day delivery; held messages move to the new gate, or are released immediately on a clear)
 * - `max_inflight_revised`: `{ maxInflight: <number or null>, oldMaxInflight: <number> }` — a capped subscriber's max in-flight cap was revised (EB-264; `null` means the cap was cleared back to unlimited; the live in-flight count and the subscribe-time delivery-concurrency cap are untouched)
 * - `filter_revised`: `{ filter: 'set' | null, oldFilter: 'set' }` — a filtered subscriber's content predicate was revised (EB-265; `null` means the predicate was cleared back to unfiltered delivery; the predicate itself is a function and is never serialized into the audit record; already-queued messages keep their original admission, and durable replay reads the live predicate per record)
 * - `delivery_shaping_revised`: `{ deliveryShaping: { messagesPerSec, burst } | null, oldDeliveryShaping: { messagesPerSec, burst } }` — a shaped subscriber's delivery-shaping rate was revised (EB-266; `null` means the shaping was cleared back to unshaped delivery; a partial revision keeps the fields it leaves out; the bucket keeps its stored budget, capped at the new burst, and the new rate applies from the next refill; queued messages and the in-flight count are untouched)
 * - `health_probe_revised`: `{ healthProbe: { maxConsecutiveFailures, processingTimeoutMs?, autoResumeAfterMs? } | null, oldHealthProbe: { ... } }` — a probed subscriber's health-probe parameters were revised (EB-267; `null` means the probe was cleared back to unprobed delivery and a degraded pause it held is released; a partial revision keeps the fields it leaves out; the consecutive-failure counter and the degraded flag are untouched; the new timeout budget applies to the next handler call, a tightened threshold trips on the next failure, and a new cooldown applies from the next degradation)
 * - `throttle_revised`: `{ throttle: { minRatePerSec, maxRatePerSec, initialRatePerSec } | null, oldThrottle: { minRatePerSec, maxRatePerSec, initialRatePerSec } }` — a throttled subscriber's adaptive-throttle rates were revised (EB-268; `null` means the throttling was cleared back to unthrottled publish; a partial revision keeps the rates it leaves out; a current excursion keeps its bucket and enforced rate until it ends, and the new rates apply from the next engagement; the drop count, the observed drain rate, and the queue are untouched)
 * - `batch_revised`: `{ batch: { maxSize, maxWaitMs } | null, oldBatch: { maxSize, maxWaitMs } }` — a batched subscriber's batch-delivery parameters were revised (EB-269; `null` means batching was cleared back to one-at-a-time delivery, after immediately delivering the pending partial batch; a partial revision keeps the fields it leaves out; the new maxSize applies from the next collection round, with an already-over-limit pending batch delivered whole first, and a new maxWaitMs applies from the next linger arming; the pending batch and the queue are untouched by a revision)
 * - `retry_ladder_revised`: `{ retryLadder: number[] | null, oldRetryLadder: number[] }` — a reliable subscriber's pre-DLQ retry ladder was revised (EB-270; `null` means the ladder was cleared back to immediate dead-lettering on budget exhaustion, by an explicit `null` or an empty array; only the ladder instance is replaced, so an already-armed rung still fires once on its old delay and the next rung arms from the new ladder, and a degraded pause re-arms the same rung from the live ladder; pending rungs, messages, sequences, TTL deadlines, and redelivery counts are untouched)
 * - `dead_letter_budget_revised`: `{ maxRedeliveries, oldMaxRedeliveries, maxEntries, oldMaxEntries, maxAgeMs, oldMaxAgeMs }` — a reliable subscriber's dead-letter budget was revised (EB-365; `null` on either side of the TTL means "no TTL"; a partial revision keeps the fields it leaves out; raising the budget rescues a message still in the immediate redelivery loop, lowering it dead-letters on the next failure, messages already on the retry ladder are unaffected, tightening `maxEntries` evicts the oldest only on the next dead-letter enqueue after an expire-first sweep, and a newly set `maxAgeMs` expires old entries on the next lazy sweep while clearing it stops expiry; a revision identical to the current budget is a no-op with no audit)
 * - `dead_letter_auto_replay_revised`: `{ autoReplay: { initialDelayMs, maxDelayMs, backoffMultiplier, maxAutoReplayAttempts } | null, oldAutoReplay: { initialDelayMs, maxDelayMs, backoffMultiplier, maxAutoReplayAttempts } }` — a reliable subscriber's dead-letter auto-replay parameters were revised (EB-367; `null` on the new side means auto-replay was cleared, and a revision after a clear is refused until the subscriber opts in again at subscribe time; a partial revision keeps the fields it leaves out; raising `maxAutoReplayAttempts` rescues a message at the poison edge, lowering it poisons on the next dead-lettering once the carried attempt count reaches the new max, the delay fields only shape the next arming — an already-armed timer keeps the delay it was armed with and still fires once after a clear — and a revision identical to the current parameters is a no-op with no audit)
 * - `dead_letter_approval_revised`: `{ requireApproval, oldRequireApproval, maxPending, oldMaxPending }` — a reliable subscriber's dead-letter replay approval gate was revised (EB-377; a partial revision keeps the field it leaves out; flipping `requireApproval` ON parks the next replay request while entries already in the DLQ are untouched, flipping it OFF does not auto-release already-parked requests — they still need approval or rejection — tightening `maxPending` keeps already-parked requests beyond the new bound and only gates new requests, and a revision identical to the current gate is a no-op with no audit)
 * - `rate_limit_revised`: `{ rateLimit: { maxMessages, perWindowMs } | null, oldRateLimit: { maxMessages, perWindowMs } }` — a rate-limited subscriber's sliding-window ceiling was revised (EB-271; `null` means the limit was cleared back to unlimited, after cancelling the armed re-flush timer; only the limiter instance is replaced and the state object retained, so an already-armed re-flush timer stays valid and the window starts fresh — tightening never retroactively blocks what was already delivered, loosening admits held messages on the next flush; the revision must be complete, and passing the current values still resets the window)
 * - `delivery_priority_revised`: `{ deliveryPriority: { weight, label }, oldDeliveryPriority: { weight, label } }` — a subscriber's drain priority was revised (EB-272; `null` restores the default `'normal'` / weight 2, recorded as the default; the drain reads the live weight from the next flush round, the DRR deficit carryover is untouched, and a revision crossing the default boundary moves the non-default count with the weight so the fast path never silently ignores a revised weight; the same weight and label is a no-op with no audit)
 * - `topic_fairness_revised`: `{ topicFairness: { weights } | null, oldTopicFairness: { weights } }` — a fair-scheduled subscriber's topic weights were revised (EB-359; `null` clears fairness back to plain FIFO order, discarding the deficits and counters with the state, and a revision after a clear is refused until the subscriber opts in again at subscribe time; only the options move — the per-topic deficit carryover and the scheduled counters are untouched, so a weight change earns no historical credit and forfeits none already banked — and the drain reads the new weights live from the next flush round; the revision is a full replacement — a topic absent from the new mapping drains at weight 1 — and a revision with the same effective mapping, with `true` equivalent to an all-1 mapping, is a no-op with no audit)
 * - `lag_monitor_revised`: `{ lagMonitor: { windowSize, thresholdMs?, onLag: 'set'? } | null, oldLagMonitor: { windowSize, thresholdMs?, onLag: 'set'? } }` — a lag-monitored subscriber's watermark parameters were revised (EB-273; `null` means monitoring was cleared, and a revision after a clear is refused until the subscriber opts in again at subscribe time; a partial revision keeps the fields it leaves out; the callback itself is never serialized, only its presence; the dwell samples, the enqueue stamps, and the alerted latch are untouched — shrinking the window evicts only the oldest samples, and the new threshold applies from the next watermark evaluation; a revision identical to the current parameters is a no-op with no audit)
 * - `slow_handler_revised`: `{ slowHandler: { thresholdMs, onSlowHandler: 'set'? } | null, oldSlowHandler: { thresholdMs, onSlowHandler: 'set'? } }` — a slow-handler subscriber's offload threshold and callback were revised (EB-274; `null` means the offload was cleared, and a revision after a clear is refused until the subscriber opts in again at subscribe time; a partial revision keeps the fields it leaves out; the callback itself is never serialized, only its presence; the state object is mutated in place, so an armed macrotask continuation keeps its `continueArmed` flag and the new threshold applies from the next handler execution; a revision identical to the current parameters is a no-op with no audit)
 * - `handler_fault_isolation_revised`: `{ handlerFaultIsolation: { maxConsecutiveErrors, onQuarantined: 'set'? } | null, oldHandlerFaultIsolation: { maxConsecutiveErrors, onQuarantined: 'set'? } }` — an isolated subscriber's error budget and quarantine callback were revised (EB-275; `null` means the isolation was cleared, refused while the subscriber is quarantined — it must be resumed via `resumeQuarantined` first — and a revision after a clear is refused until the subscriber opts in again at subscribe time; a partial revision keeps the fields it leaves out; the callback itself is never serialized, only its presence; the state object is mutated in place, so the consecutive-error counter and the quarantined latch are untouched and the new budget applies from the next handler-failure evaluation; a revision identical to the current parameters is a no-op with no audit)
 * - `delivery_latency_revised`: `{ deliveryLatency: { windowSize } | null, oldDeliveryLatency: { windowSize } }` — a sampled subscriber's delivery-latency window size was revised (EB-276; `null` means sampling was cleared, and a revision after a clear is refused until the subscriber opts in again at subscribe time; shrinking the window evicts only the oldest samples, widening evicts nothing, and the enqueue clocks of in-flight messages are untouched — the new window applies from the next sample and summary; a revision identical to the current window is a no-op with no audit)
 * - `delivery_concurrency_revised`: `{ deliveryConcurrency: <number>, oldDeliveryConcurrency: <number> }` — a subscriber's handler-concurrency cap was revised (EB-277; always armed with default 1, so there is no opt-in gate — a subscription without an explicitly set cap can be revised; `null` restores the parse-chain default, the current `maxInflight` cap or 1; only `state.max` moves, `inFlight` and `keysInFlight` are untouched, so lowering never revokes in-flight calls and raising is picked up immediately by the live-max read in `tryAcquireDeliverySlot` plus a scheduled flush; same-key ordering is unchanged; a revision identical to the current cap is a no-op with no audit)
 * - `lag_forecast_revised`: `{ lagForecast: { windowSize, minSamples, thresholdMs?, forecastHorizonMs?, onLagForecast: 'set'? } | null, oldLagForecast: { ... } }` — a forecast-enabled subscriber's lag-forecast parameters were revised (EB-278; `null` means forecasting was cleared, and a revision after a clear is refused until the subscriber opts in again at subscribe time; a partial revision keeps the fields it leaves out; the callback itself is never serialized, only its presence; the state object is mutated in place, so the enqueue stamps and the alerted latch are untouched — shrinking the window evicts only the oldest samples, widening restores nothing, and the new parameters apply from the next accepted enqueue's forecast evaluation, with the latch clearing or re-arming self-consistently under them; a revision identical to the current parameters is a no-op with no audit)
 * - `ack_latency_revised`: `{ ackLatency: { windowSize, ackSloMs, onAckSloMiss: 'set'? } | null, oldAckLatency: { ... } }` — an ack-latency-enabled subscriber's sampling parameters were revised (EB-279; `null` means sampling was cleared, and a revision after a clear is refused until the subscriber opts in again at subscribe time; a partial revision keeps the fields it leaves out; the callback itself is never serialized, only its presence; the tracker instance is mutated in place, so the per-message accept clocks and the already-recorded samples are untouched — shrinking the window evicts only the oldest samples, widening restores nothing, retuning the SLO re-scores the retained samples, and the new parameters apply from the next accepted enqueue and the next ack sample; a revision identical to the current parameters is a no-op with no audit)
 * - `session_window_revised`: `{ sessionWindow: { gapMs, aggregate, field?, maxSessionsPerKey } | null, oldSessionWindow: { ... } }` — a session-windowed subscriber's activity gap and open-session bound were revised (EB-280; `null` means session windowing was cleared, the sweep timer cancelled and the open sessions discarded, and a revision after a clear is refused until the subscriber opts in again at subscribe time; a partial revision keeps the fields it leaves out; the field is recorded only when one was set at subscribe time; the aggregate fold and the extracted field never move — a differing `aggregate` or `field` throws `RangeError` with the live configuration untouched, because changing either would reinterpret the messages already folded into the open sessions; the state object is mutated in place, so the open sessions are untouched — shrinking the bound evicts nothing and the new bound governs only the next session opening, and the new gap governs the next activity-gap judgment; a revision identical to the current parameters is a no-op with no audit)
 * - `queue_size_revised`: `{ queueSize: <number>, oldQueueSize: <number> }` — a subscriber's queue count capacity was revised (EB-281; every subscription carries a capacity — `queueSize`, default 100 — so there is no opt-in gate and `null` clears back to the default 100, never to "no capacity"; the queue object is mutated in place, so the already-queued messages are untouched — shrinking below the current depth evicts nothing and the new cap governs only future admissions, with the default `'block'` tier armed at once when the revision itself saturates the queue so arrivals are refused until the queue recedes below the new high-water mark, and growing admits immediately; `dropPolicy`, `highWaterMarkRatio`, `queueMaxBytes`, and the backpressure tier never move; a revision identical to the current effective capacity, including `null` when already at 100, is a no-op with no audit)
 * - `queue_max_bytes_revised`: `{ queueMaxBytes: <number> | null, oldQueueMaxBytes: <number> }` — a subscriber's queue byte budget was revised (EB-282; only subscriptions that already carry a `queueMaxBytes` budget can be revised — a never-set budget, and a budget cleared by an earlier revision, both refuse until the subscriber opts in again at subscribe time, and `null` clears back to no byte dimension, never to a default; the queue object is mutated in place, so the already-queued messages and their buffered-byte accounting are untouched — shrinking below the current buffered bytes evicts nothing and the new budget governs only future admissions, with over-budget newcomers shedding per the standing `dropPolicy` and never arming the `'block'` tier, and growing admits immediately; the count capacity, `dropPolicy`, `highWaterMarkRatio`, and the backpressure tier never move; a revision identical to the current budget is a no-op with no audit)
 * - `subscriber_max_payload_bytes_revised`: `{ subscriberMaxPayloadBytes: <number> | null, oldSubscriberMaxPayloadBytes: <number> }` — a subscriber's explicit payload-size cap was revised (EB-283; only subscriptions that already carry an explicit `subscriberMaxPayloadBytes` can be revised — a never-set cap, and a cap cleared by an earlier revision, both refuse until the subscriber opts in again at subscribe time, and `null` clears back to inheritance of the topic's EB-120 payload cap, or to no cap when the topic has none; only the subscriber field moves — `subscriberPayloadCapFor` reads the live field on every delivery decision, so the new cap governs from the next delivery decision, and already-delivered messages, already-skipped messages, the oversize counters, and the per-topic sequence baseline are untouched; a revision identical to the current cap is a no-op with no audit)
 * - `subscriber_schema_version_revised`: `{ schemaVersion: <number> | null, oldSchemaVersion: <number> | null }` — a subscriber's expected schema version was revised (EB-342; `null` means the expected version was cleared back to undeclared, pass-through delivery; there is no opt-in gate — a subscriber that declared no version can gain one, since declaring a version only narrows what that subscriber receives — and a declared version can be raised, lowered, or cleared; only the subscriber field moves — `checkSchemaMigrationGate` (fan-out) and `applySchemaMigration` (delivery) both read the live field on every message, so the new version governs from the next fan-out and a message already queued when the revision lands is migrated — or fail-closed dropped — under the new version at delivery time, while already-delivered messages are untouched and a message dropped fail-closed under the old version is never resurrected; a revision identical to the current version, including `null` on a subscriber that declared none, is a no-op with no audit)
 * - `key_range_revised`: `{ keyRange: { start?, end? } | null, oldKeyRange: { start?, end? } }` — a subscription's key range was revised (EB-284; only subscriptions that already carry a `keyRange` can be revised — a never-set range, and a range cleared by an earlier revision, both refuse until the subscriber opts in again at subscribe time, and `null` clears back to no narrowing, which is what restores keyless delivery; only the subscriber field moves — `deliverToSubscriber` reads the live range on every delivery decision, so the new range governs from the next delivery decision, and already-delivered messages are untouched while skips keep the `countFiltered` semantics; a revision identical to the current range is a no-op with no audit, and `{}` counts as a real range — it matches every keyed message while still excluding keyless ones — not as a clear)
 * - `backpressure_tier_revised`: `{ backpressure: <tier>, oldBackpressure: <tier> }` — a subscription's slow-consumer backpressure tier was revised (EB-285; there is no opt-in gate — every subscription always carries an already-resolved tier, default `'block'` — so there is no "unset" state; only the subscriber tier moves — `queueSize`, `queueMaxBytes`, `dropPolicy`, and `highWaterMarkRatio` never move, already-queued messages are untouched, and the new tier governs from the next enqueue admission decision, with the tier deciding block-vs-drop when full and the standing `dropPolicy` deciding which message is dropped; the revision itself neither arms nor clears the `'block'` latch — a blocked subscriber still refuses arrivals until the queue recedes below the high-water mark, when the usual `onDrained` path clears it; a revision identical to the current tier is a no-op with no audit)
 * - `drop_policy_revised`: `{ dropPolicy: <policy>, oldDropPolicy: <policy> }` — a subscription's queue shed policy was revised (EB-375; there is no opt-in gate — every subscription always carries an already-resolved policy, default `'drop-oldest'` — so there is no "unset" state; only the queue policy moves — `queueSize`, `queueMaxBytes`, `highWaterMarkRatio`, the backpressure tier, and the `'block'` latch never move, already-queued messages are untouched, and the new policy governs from the next enqueue admission decision, with the count-saturation shed, the byte-budget shed, and the `'block'` tier's first-saturation admit-with-shed all picking their victim by the new policy; the revision itself neither arms nor clears the `'block'` latch — a blocked subscriber still refuses arrivals until the queue recedes below the high-water mark, when the usual `onDrained` path clears it; a revision identical to the current policy is a no-op with no audit; a shadow subscription's queue is revised through its own subscription id and the mirror sheds per the new policy with no separate entry point)
 * - `multi_patterns_revised`: `{ patterns: <new registered set>, oldPatterns: <old set> }` — a multi-pattern aggregate subscription's pattern set was revised (EB-286; only multi-pattern subscriptions can be revised — a single-pattern subscription throws `RangeError` and must use `updateSubscriptionPattern` instead, the mirror of EB-74's multi-pattern refusal; unknown subId returns false; the array must be non-empty, string-only, non-empty and duplicate-free, and every pattern resolves through the subscribe registration path — namespace re-translation, compiled matcher, prefix-index key — and passes the per-pattern broker ACL, all before anything mutates, so a failed call leaves the old set, its counts, its cache entries and its index buckets untouched; a set-equal revision in any order is a no-op with no audit; only the pattern set, the union matcher and the index ledgers move — prefix-index buckets and `subscribersByPattern` counts with `patternCache` eviction at zero — retained patterns keep count, cache entry and bucket untouched, and the queue, ACK state, lease and idle timers, health probe and sequence baselines survive untouched, the primary-pattern label following the new first pattern with lease/idle labels relabelled not re-armed; the new set governs from the next delivery match)
 * - `filter_expr_revised`: `{ filterExpr: <new source> | null, oldFilterExpr: <old source> }` — a subscription's declarative filter expression was revised (EB-287; only subscriptions that already carry a `filterExpr` can be revised — a never-set expression, and an expression cleared by an earlier revision, both refuse until the subscriber opts in again at subscribe time, and `null` clears back to unfiltered delivery; the new expression is compiled through the subscribe path before anything mutates, so a failed call leaves the live expression untouched; only the subscriber field moves — live, durable replay (read per record), retained and shadow paths all read the live expression on every message, so the new expression governs from the next fan-out, and already-queued messages keep their original admission; a revision identical to the current expression string is a no-op with no audit)
 * - `expiring_soon_threshold_revised`: `{ expiringSoonMs: <number>, oldExpiringSoonMs: <number> }` — an expiring-soon-monitored subscriber's warning threshold was revised (EB-288; only subscriptions that already carry `expiringSoon` monitoring can be revised — a never-monitored subscription refuses until the subscriber opts in again at subscribe time; the new threshold reuses the subscribe-time validation, so a failed call leaves the live threshold untouched; only the threshold moves — the `onExpiringSoon` callback and the `alerted` latch are untouched, so a backlog that already warned does not re-fire when the threshold widens and the latch still clears only when the queue drains empty, and the new threshold governs from the next enqueue/drain evaluation; a revision identical to the current threshold is a no-op with no audit)
 * - `key_hotspot_threshold_revised`: `{ thresholdDepth: <number>, oldThresholdDepth: <number> }` — a key-hotspot-monitored subscriber's alert threshold was revised (EB-289; only subscriptions that already carry `keyHotspot` monitoring can be revised — a never-monitored subscription refuses until the subscriber opts in again at subscribe time; the new threshold reuses the subscribe-time validation (`resolveKeyHotspotOptions`: a positive integer), so a failed call leaves the live threshold untouched; only `keyHotspot.thresholdDepth` moves — the `onKeyHotspot` callback and the `autoMitigate` configuration are untouched, including `recoverDepth` snapshotted at subscribe time, which is not recomputed from the new threshold; the alert latch still re-arms only when the buffer depth drops below the (live) threshold, so lowering the threshold below the live buffer depth alerts on the next evaluation while raising it never clears an already-triggered latch, and the new threshold governs from the next keyed-delivery evaluation, which reads the live value; a revision identical to the current threshold is a no-op with no audit)
 * - `key_hotspot_recover_depth_revised`: `{ recoverDepth: <number>, oldRecoverDepth: <number> }` — a key-hotspot-monitored subscriber's auto-mitigation recovery depth was revised (EB-294; only subscriptions that already carry `keyHotspot` monitoring with `autoMitigate` enabled can be revised — a never-monitored subscription, and a detection-only subscription, both refuse until the subscriber opts in again at subscribe time; the new depth reuses the subscribe-time validation (`resolveKeyHotspotOptions`: a non-negative integer at most the live threshold), so a failed call leaves the live depth untouched, and a depth above the live threshold is refused — which also repairs the inverted hysteresis band an EB-289 threshold revision can leave behind, where the subscribe-time `recoverDepth` snapshot exceeds the lowered live threshold; only `keyHotspot.mitigate.recoverDepth` moves — the `thresholdDepth`, the `onKeyHotspot` / `onHotKeyMitigated` callbacks, and the `mitigatedKeys` latch are untouched, so an already-mitigated key is neither released nor re-armed by the revision itself and the release decision is taken at the next keyed-delivery evaluation, which reads the live depth — a raised depth releases an engaged mitigation sooner while a lowered one keeps it engaged until the buffer drains further, and `0` means no automatic release, the buffer never draining strictly below zero; a revision identical to the current depth is a no-op with no audit)
 * - `key_hotspot_auto_mitigate_revised`: `{ enabled: <boolean>, recoverDepth: <number> | null, oldEnabled: <boolean>, oldRecoverDepth: <number> | null }` — a key-hotspot-monitored subscriber's auto-mitigation switch was revised (EB-338; only subscriptions that already carry `keyHotspot` monitoring can be revised — a never-monitored subscription refuses until the subscriber opts in again at subscribe time; `true` enables with the defaults (`recoverDepth` taken from the live threshold, no callback), an object tunes `recoverDepth` (a non-negative integer at most the live threshold) and `onHotKeyMitigated`, `false` / `null` disables — and a detection-only subscription can opt in this way, unlike the EB-294 depth revision which refuses it; the new value reuses the subscribe-time validation (`resolveKeyHotspotOptions`), so a failed call leaves the live switch untouched; only the switch moves — the `thresholdDepth` and the `onKeyHotspot` callback are untouched, and enabling never touches the alert latch, so a key that already alerted stays alerted and mitigation engages on the next keyed-delivery evaluation whose depth is at or above the threshold, while disabling also clears the `mitigatedKeys` latch, releasing engaged streams immediately (the next out-of-order arrival buffers instead of being shed) so `getStats().hotKeys` never reports a mitigated stream that can no longer shed; a revision homomorphic to the current switch — both disabled, or both enabled with the same `recoverDepth` and the same callback reference — is a no-op with no audit)
 * - `cluster_link_heartbeat_revised`: `{ heartbeatMs: <number>, oldHeartbeatMs: <number> }` — the cluster link's heartbeat send interval was revised (EB-339; only a bus with a live cluster link can be revised — a never-connected bus, and a bus that already disconnected, both refuse until `connectToHub` runs again; the new interval reuses the connect-time validation (`ClusterLink` constructor: a positive finite number), so a failed call leaves the live interval untouched; only the send interval moves — the cached route table, the hub epoch, and the reconnect policy are untouched, and the armed heartbeat timer is re-armed at the new interval with no reconnect and no route re-sync, while a revision made during backoff is stored and picked up by the next `startHeartbeat`; the hub-side `heartbeatTimeoutMs` is untouched and unvalidated, so the node-side interval must stay well below the hub's silence bound; a revision identical to the current interval is a no-op with no audit)
 * - `cluster_link_reconnect_backoff_revised`: `{ baseDelayMs: <number>, maxDelayMs: <number>, maxAttempts: <number>, jitterMs: <number>, oldBaseDelayMs: <number>, oldMaxDelayMs: <number>, oldMaxAttempts: <number>, oldJitterMs: <number> }` — the cluster link's reconnect backoff policy was revised (EB-340, EB-395; only a bus with a live cluster link built with reconnect enabled can be revised — a never-connected bus, a bus that already disconnected, and a link built with `reconnect: false` all refuse until `connectToHub` runs again with reconnect enabled; a field left out keeps its live value, and an illegal value in any field (`baseDelayMs` / `maxDelayMs` not positive finite numbers, `maxAttempts` not a positive integer, `jitterMs` not a non-negative finite number) vetoes the whole revision and leaves every live value untouched — note the constructor never validated these numbers, so the rule is the revision-time contract, not a restatement of a connect-time check; only the policy moves — the cached route table, the hub epoch, and the heartbeat interval are untouched, the attempt counter and `onGiveUp` are untouched, and a backoff timer already armed keeps running at the delay it was armed with, so the new policy governs from the next backoff (which reads the live strategy) and the next failure reads the live `maxAttempts`; a revision that moves `baseDelayMs`, `maxDelayMs`, or `jitterMs` rebuilds the strategy as a fresh `ExponentialBackoff` at the merged values — including the merged jitter, so a plain base/cap revision no longer silently resets a custom jitter to the 250ms default (the EB-340 gap closed by EB-395) — while a `maxAttempts`-only revision leaves the strategy object alone; the hub-side `heartbeatTimeoutMs` is untouched and unvalidated, so a retuned backoff that waits longer than the hub's silence bound can still get the node swept on the next drop; a revision that provides no field, and a revision identical to the current policy field for field, are both no-ops with no audit)
 * - `deduplicate_messages_revised`: `{ windowMs: <number>, oldWindowMs: <number>, maxEntries: <number>, oldMaxEntries: <number> }` — a dedup-enabled subscriber's exactly-once window was revised (EB-295; only subscriptions that already opted into `deduplicateMessages` can be revised — a never-opted-in subscription refuses until the subscriber opts in again at subscribe time; the new window/bound reuse the subscribe-time validation (`resolveDeduplicateMessagesOptions`: `windowMs` a positive finite number, `maxEntries` a positive integer), so a failed call leaves the live values untouched, and only the fields provided move — a partial revision keeps the fields it leaves out, and an illegal value in either field leaves both live values exactly as they were; only `dedup.windowMs` / `dedup.maxEntries` move — the `consumerId` and the recorded `(topic, messageId)` table are untouched, so entries already seen stay seen and a durable `consumerId` keeps journaling under the same identity, and the new values govern from the next arrival, which reads the live values — shrinking the window clears entries already past the new horizon on the next arrival's expiry scan while widening it extends every recorded entry to the new horizon, and tightening `maxEntries` evicts the oldest entries down to the new bound on the next arrival's bounded-eviction loop; a revision that provides neither field, and a revision identical to the current window, are both no-ops with no audit)
 * - `tail_sampling_revised`: `{ successSampleRate: <number>, oldSuccessSampleRate: <number>, maxOpenTraces: <number>, oldMaxOpenTraces: <number>, maxSpansPerTrace: <number>, oldMaxSpansPerTrace: <number>, bufferSize: <number>, oldBufferSize: <number> }` — the bus-level tail-sampling tuning was revised (EB-296; only a bus that already enabled `tailSampling` can be revised — a never-enabled bus refuses until tail sampling is opted into at construction; the new values reuse the construction-time validation (`resolveTailSamplingOptions`: the rate a finite number in [0, 1], the rest positive integers), so a failed call leaves the live tuning untouched, and only the fields provided move — a partial revision keeps the fields it leaves out, and an illegal value in any field leaves every live value exactly as it was; only the four numeric bounds move — the `onTraceSpan` callback is untouched, and in-flight open traces keep their buffered spans, so the new values govern from the next decision onward — the next trace decision samples at the live rate, the next span buffers truncate at the live per-trace cap, the next open enforces the live open-trace budget (tightening it force-decides the oldest open traces), and shrinking the buffer trims the ring to the newest spans at once, so the next append evicts the oldest under the new bound; a revision that provides neither field, and a revision identical to the current tuning, are both no-ops with no audit)
 * - `partition_health_revised`: `{ windowSize: <number>, oldWindowSize: <number>, unhealthyThreshold: <number>, oldUnhealthyThreshold: <number>, minSamples: <number>, oldMinSamples: <number>, cooldownSuccesses: <number>, oldCooldownSuccesses: <number> }` — the bus-level partition-health tuning was revised (EB-298; only a bus that already enabled `partitionHealth` can be revised — a never-enabled bus refuses until partition health is opted into at construction; the new values reuse the construction-time validation (`resolvePartitionHealthOptions`: the window a positive integer, the threshold a number in (0, 1], `minSamples` a positive integer no larger than the merged window, the cool-down a positive integer), so a failed call leaves the live tuning untouched, and only the fields provided move — a partial revision keeps the fields it leaves out, and an illegal value in any field leaves every live value exactly as it was; only the four numeric bounds move — the records table (in-flight outcome windows, cool-down counters) is untouched, so the new values govern from the next report onward — a tightened window truncates the oldest outcomes down to the new window at the next report, a tightened threshold/minSamples condemns at the next classify, and a revised cool-down arms the cool-down of the next condemnation (an in-flight cool-down counter is not rewritten); a revision that provides neither field, and a revision identical to the current tuning, are both no-ops with no audit)
 * - `otlp_revised`: `{ sampleRate: <number>, oldSampleRate: <number>, batchSize: <number>, oldBatchSize: <number>, flushIntervalMs: <number>, oldFlushIntervalMs: <number>, timeoutMs: <number>, oldTimeoutMs: <number>, maxBuffered: <number>, oldMaxBuffered: <number> }` — the bus-level OTLP export tuning was revised (EB-299; EB-357 adds the flush interval; only a bus that already enabled `otlp` can be revised — a never-enabled bus refuses until OTLP export is opted into at construction; the new values reuse the construction-time validation (`resolveOtlpOptions`: the rate a finite number in [0, 1], `batchSize`/`maxBuffered` positive integers, `timeoutMs`/`flushIntervalMs` positive finite numbers), so a failed call leaves the live tuning untouched, and only the fields provided move — a partial revision keeps the fields it leaves out, and an illegal value in any field leaves every live value exactly as it was; only the five numeric bounds move — the `endpoint`, `headers`, and `serviceName` are untouched, and in-flight buffered spans keep their places, so the new values govern from the next decision onward — the next span admission samples at the live rate and drops the oldest past the live buffer bound, the next flush triggers and slices at the live batch size, the next export POST times out at the live timeout, and a revised flush interval rebuilds the flush timer at the new cadence from the next tick; a revision that provides neither field, and a revision identical to the current tuning, are both no-ops with no audit)
 * - `otlp_metrics_revised`: `{ flushIntervalMs: <number>, oldFlushIntervalMs: <number>, timeoutMs: <number>, oldTimeoutMs: <number>, perTopicEnabled: <boolean>, oldPerTopicEnabled: <boolean>, perTopicMaxTopics: <number>, oldPerTopicMaxTopics: <number> }` — the bus-level OTLP metrics export tuning was revised (EB-374; only a bus that already enabled `otlpMetrics` can be revised — a never-enabled bus refuses until metrics export is opted into at construction; the new values reuse the construction-time validation (`resolveOtlpMetricsOptions`: `flushIntervalMs`/`timeoutMs` positive finite numbers, `perTopic.maxTopics` a positive integer), so a failed call leaves the live tuning untouched, and only the fields provided move — a partial revision keeps the fields it leaves out, and an illegal value in any field leaves every live value exactly as it was; only the three live bounds move — the `endpoint`, `headers`, and `serviceName` are untouched, and the counters (`exportedMetrics`/`failedExports`/`lastExportAtMs`) and the Sum `startTimeUnixNano` are retained, so the collector-side rate computation is uninterrupted, and an in-flight flush is untouched — the next export POST times out at the live timeout, the next export covers the hottest topics under the live per-topic cap, and a revised flush interval rebuilds the flush timer at the new cadence from the next tick; `perTopic: false` disables every per-topic series (bus aggregates only) and resolves through the construction-time canonical form, so the live `maxTopics` reports the resolver default (20) while disabled; a revision that provides neither field, and a revision identical to the current tuning, are both no-ops with no audit; a closed bus refuses the revision)
 * - `chaos_revised`: `{ scenarios: <ChaosScenario[]>, oldScenarios: <ChaosScenario[]> }` (each scenario in canonical form with its seed defaulted) — the bus-level fault-injection scenarios were revised (EB-300; only a bus that already enabled `chaos` can be revised — a never-enabled bus refuses until chaos is opted into at construction, since it holds no injector at all; the new list reuses the construction-time validation (`assertValidChaosScenario`: the type/target pair supported, `rate` a finite number in [0, 1], `latencyMs` a positive finite number for `'latency'` faults and absent otherwise, `seed` a finite number), so a failed call leaves the live scenarios — and the RNG streams behind them — untouched; an empty list disables injection while keeping the injector and its counters alive, so a later revision can re-arm it — distinct from a bus constructed with no scenarios, which never builds an injector; each scenario in the new list gets a fresh private RNG stream from its seed, so the fault sequence restarts deterministically from the new seeds, and the injection counters are retained, not reset; an already-triggered in-flight fault is unaffected, the retune only changing what the next `faultFor` evaluation sees; a revision identical to the current scenarios, and an absent revision, are both no-ops with no audit)
 * - `global_publish_rate_limit_revised`: `{ messagesPerSecond: <number>, oldMessagesPerSecond: <number>, burst: <number>, oldBurst: <number> }` — the bus-level global publish rate limit was revised (EB-301; only a bus that already enabled `globalPublishRateLimit` can be revised — a never-enabled bus refuses until the limit is opted into at construction; the new values reuse the construction-time validation (`resolveGlobalPublishRateLimit`: the rate and the burst positive finite numbers), so a failed call leaves the live tuning untouched, and only the fields provided move — a partial revision keeps the fields it leaves out, and an illegal value in either field leaves both live values exactly as they were; a partial revision that only moves `messagesPerSecond` keeps the live `burst` — it is not recomputed from the new rate's default; only the two numeric bounds move — the stored token budget is kept, a lowered burst clamping the banked tokens to the new burst and a raised burst not topping the bucket up, so the new rate applies from the next refill; the admission gates read the live limiter on every publish, so the new tuning governs from the next publish; a revision that provides neither field, and a revision identical to the current tuning, are both no-ops with no audit)
 * - `outbox_relay_revised`: `{ pollIntervalMs: <number>, oldPollIntervalMs: <number>, batchSize: <number>, oldBatchSize: <number>, probeLimit: <number>, oldProbeLimit: <number> }` — the attached outbox relay's tuning was revised (EB-302; only a bus with an attached outbox relay can be revised — a bus with no relay refuses until one is attached with `attachOutboxRelay`; the new values reuse the construction-time validation (`resolveOutboxRelayOptions`: `batchSize`/`probeLimit` integers >= 1, `pollIntervalMs` a non-NaN number, 0 or negative disabling the poll timer), so a failed call leaves the live tuning untouched, and only the fields provided move — a partial revision keeps the fields it leaves out, and an illegal value in any field leaves every live value exactly as it was; only the three numeric parameters move — the cursor and the relayed count are untouched, so a new `batchSize` governs from the next drain cycle and a new `probeLimit` from the next `getLag()` probe, and when the relay is running a revised `pollIntervalMs` re-arms the poll timer at the new interval with no extra drain triggered (a stopped relay picks the new interval up at the next `start()`); a revision that provides neither field, and a revision identical to the current tuning, are both no-ops with no audit)
 * - `schedule_skew_revised`: `{ windowSize: <number>, oldWindowSize: <number>, alertThresholdMs: <number|undefined>, oldAlertThresholdMs: <number|undefined>, hasOnScheduleSkew: <boolean>, oldHasOnScheduleSkew: <boolean> }` — the bus-level schedule-skew observation tuning was revised (EB-303; only a bus that already enabled `scheduleSkew` can be revised — a never-enabled bus refuses until schedule-skew sampling is opted into at construction, since it holds no tracker at all; the new values reuse the construction-time validation (`resolveScheduleSkewOptions`: the window a positive integer, the threshold a non-negative finite number, the callback a function that requires a threshold in force), so a failed call leaves the live tuning untouched, and only the fields provided move — a partial revision keeps the fields it leaves out, and an illegal value in any field leaves every live value exactly as it was; only the three observation parameters move — the samples are untouched, so a tightened `windowSize` truncates the oldest samples down to the new bound at the next recorded sample and a revised `alertThresholdMs` / `onScheduleSkew` governs from the next sweep's alert check (a lowered threshold alerts on the next skew beyond it and a replaced callback fires from the next alert, the old callback never firing again); there is no way to unset the callback through a revision — `undefined` is not a value, so a bus that armed a callback keeps alerting until the callback is replaced by another one, and the audit records only whether a callback is armed (`hasOnScheduleSkew` / `oldHasOnScheduleSkew`), never the function itself; a revision that provides neither field, and a revision identical to the current tuning, are both no-ops with no audit)
 * - `admission_latency_revised`: `{ windowSize: <number>, oldWindowSize: <number>, gateWindowSize: <number>, oldGateWindowSize: <number>, slowAdmissionThresholdMs: <number>, oldSlowAdmissionThresholdMs: <number> }` — the bus-level admission-latency observation tuning was revised (EB-304; only a bus that already enabled `publishLatency` can be revised — a never-enabled bus refuses until admission-latency sampling is opted into at construction, since it holds no tracker at all; the new values reuse the construction-time validation (`resolveAdmissionLatencyOptions`: the windows positive integers, the slow threshold a non-negative finite number), so a failed call leaves the live tuning untouched, and only the fields provided move — a partial revision keeps the fields it leaves out, and an illegal value in any field leaves every live value exactly as it was; only the three observation bounds move — the samples are untouched, so a tightened window truncates the oldest samples down to the new bound at the next recorded sample and a revised slow threshold counts from the next sample onward, with samples already counted keeping the verdict they were recorded under (so the slow counter can never go negative or stale when a threshold change and a window tightening meet at an eviction), and windows created after the retune are built at the new bounds; a revision that provides neither field, and a revision identical to the current tuning, are both no-ops with no audit)
 * - `bridge_health_revised`: `{ intervalMs: <number>, oldIntervalMs: <number>, failureThreshold: <number>, oldFailureThreshold: <number> }` — the bus-level bridge-health monitoring tuning was revised (EB-305; only a bus that already enabled `bridge.health` can be revised — a never-enabled bus refuses until bridge health monitoring is opted into at construction, since it holds no monitor at all; the new values reuse the construction-time validation (`resolveBridgeHealthOptions`: the interval a positive finite number, the threshold a positive integer), so a failed call leaves the live tuning untouched, and only the fields provided move — a partial revision keeps the fields it leaves out, and an illegal value in any field leaves every live value exactly as it was; only the two tuning values move — the health state is untouched, so the consecutive-failure counter, the last-probe timestamp, and the healthy/unhealthy state all survive, a tightened threshold trips on the next failure past it and a loosened one needs more failures from here on, and a moved interval clears and re-arms the probe timer at the new interval with no extra probe fired (a probe already in flight re-arms itself at the new interval, and a threshold-only revision leaves the pending probe's countdown untouched); a revision that provides neither field, and a revision identical to the current tuning, are both no-ops with no audit)
 * - `bridge_revised`: `{ maxInboundQueue: <number>, oldMaxInboundQueue: <number> }` — the bus-level bridge inbound queue bound was revised (EB-313; only a bus that already enabled `bridge` can be revised — a never-enabled bus refuses until a bridge is opted into at construction, since it holds no bridge config at all; the new value reuses the construction-time validation (`resolveBridgeOptions`: the queue bound a positive integer), so a failed call leaves the live config untouched, and only `maxInboundQueue` moves — the `transport` object and the health monitor are untouched, so no probe timer is re-armed, no failure counter resets, and outbound mirroring is never paused or resumed by a revision — and the new bound governs from the next inbound envelope onward, which reads the live value: envelopes already queued are untouched, so tightening below the live queue depth does not trim the buffer (the queued envelopes still drain) but the next arrival sheds at once because the queue is already at or over the new bound, while loosening admits more before the next shed; a revision that provides no field, and a revision identical to the current bound, are both no-ops with no audit)
 * - `cep_sequence_pattern_revised`: `{ withinMs: <number>, oldWithinMs: <number>, maxPartialMatches: <number>, oldMaxPartialMatches: <number> }` (the `pattern` field carries the pattern id) — a defined CEP sequence pattern's window and partial bound were revised (EB-297; only a pattern that is already defined can be revised — an unknown id refuses until the pattern is defined; the new values reuse the define-time validation (`withinMs` a positive finite number, `maxPartialMatches` a positive integer), so a failed call leaves the live values untouched, and only the fields provided move — a partial revision keeps the fields it leaves out, and an illegal value in either field leaves both live values exactly as they were; only `withinMs` / `maxPartialMatches` move — the steps, the correlation key, and the pattern id are immutable, and in-flight partials are untouched, so the new values govern from the next observation — shrinking the window expires in-flight partials on the next observation's lazy sweep while widening it extends their life, tightening the bound evicts the oldest partials on the next new partial, and an absence step without an explicit `absenceMs` follows the live window from the next arming (an armed deadline re-checks the live window on fire); a revision that provides neither field, and a revision identical to the current values, are both no-ops with no audit)
 * - `cep_sequence_pattern_paused`: `{ paused: <boolean> }` (the `pattern` field carries the pattern id) — a defined CEP sequence pattern was paused or resumed live (EB-381; only a pattern that is already defined can be paused — an unknown id refuses until the pattern is defined; pausing freezes the pattern — no sweep, no advance, no new partial, no absence evaluation — disarms its armed absence deadlines without killing its partials, and resuming re-arms the deadlines of partials waiting at absence steps from the resume moment at the step's `absenceMs` or the live `withinMs`, deriving nothing itself; a pause identical to the current state is a no-op with no audit)
 * - `cep_absence_window_revised`: `{ stepIndex: <number>, absenceMs: <number|undefined>, oldAbsenceMs: <number|undefined> }` (the `pattern` field carries the pattern id) — one absence step's explicit window was revised live (EB-382; only a defined pattern's absence step can be revised — an unknown id, an out-of-range step index, a non-absence step, and an illegal value all refuse, the value reusing the define-time rule (`absenceMs` a positive finite number, or `null` to clear back to following the live `withinMs`), so a failed call leaves the live value untouched; only that step's explicit `absenceMs` moves — the steps, the topic, the absence switch, the correlation key, and the pattern id are immutable, and `withinMs` / `maxPartialMatches` still move only through `updateCepSequencePattern` (EB-297); the new value governs from the next arming — an already-armed deadline keeps its armed window (EB-379: no timer is reset, no armed deadline rewritten), a chained absence step re-arms at the new value when the previous absence settles, and the EB-379 guard is unchanged (an explicit `absenceMs` wider than the live `withinMs` still drops at settle instead of deriving); a revision made while the pattern is paused (EB-381) governs the re-arm on resume; a revision identical to the current value — including clearing a step that already follows `withinMs` — is a no-op with no audit; a closed bus refuses the revision (EB-154))
 * - `latency_slo_threshold_revised`: `{ p99ThresholdMs: <number>, oldP99ThresholdMs: <number> }` — a processing-latency-SLO-tracked subscriber's p99 threshold was revised (EB-290; only subscriptions that already carry `latencySlo` monitoring can be revised — a never-monitored subscription refuses until the subscriber opts in again at subscribe time; the new threshold reuses the subscribe-time validation (`resolveLatencySloOptions`: a positive finite number), so a failed call leaves the live threshold untouched; only `latencySlo.thresholdMs` moves — the tracker window and its samples, the `onLatencySloMiss` callback, and the `alerted` latch are untouched, so lowering the threshold below the live p99 alerts on the next sample evaluation while raising it never clears an already-triggered latch, and the latch still re-arms only when the p99 drops back to or below the (live) threshold, and the new threshold governs from the next sample evaluation on both paths (plain/batched invocation sampling and reliable `ack()`-completion sampling), which read the live value; a revision identical to the current threshold is a no-op with no audit)
 * - `latency_slo_window_size_revised`: `{ windowSize: <number>, oldWindowSize: <number> }` — a processing-latency-SLO-tracked subscriber's sample-window size was revised (EB-368; only subscriptions that already carry `latencySlo` monitoring can be revised — a never-monitored subscription refuses until the subscriber opts in again at subscribe time; the new window reuses the subscribe-time validation (`resolveLatencySloOptions`: a positive integer), so a failed call leaves the live window, the samples, the threshold, the callback, and the latch untouched; only the window moves — the SLO tracker is resized in place (`DeliveryLatencyTracker.resize`: shrinking evicts the oldest samples past the new bound at once so `getStats().subscriberLatencyP99` reflects the new window's p99 and sample count immediately, widening keeps every sample), the `p99ThresholdMs`, the `onLatencySloMiss` callback, and the `alerted` latch are untouched, so shrinking the slow tail out of the window so the p99 drops below the threshold does not clear an already-triggered latch — the latch re-arms only when a later sample evaluation sees the p99 at or below the (live) threshold — and when phase attribution is armed for the subscriber (EB-133) its three phase trackers resize to the same bound so the attribution keeps covering exactly the history the miss was evaluated against; a revision identical to the current window is a no-op with no audit)
 * - `causal_revised`: `{ maxBufferPerSource: <number>, oldMaxBufferPerSource: <number> }` — a causal subscriber's per-source buffer bound was revised (EB-291; only subscriptions that already carry `causal` delivery can be revised — a never-opted-in subscription refuses until the subscriber opts in again at subscribe time; the new bound reuses the subscribe-time validation (a positive integer), so a failed call leaves the live bound untouched; only `causal.maxBufferPerSource` moves — the per-source expectations, the skipped-clock sets, and the already-buffered messages are untouched, so lowering the bound below the live buffer depth sheds on the next arrival from that source (the oldest buffered message is dropped and the expectation advances past it, exactly like the subscribe-time bound) while raising it admits more early arrivals at once, and the new bound governs from the next early arrival, which reads the live value; a revision identical to the current bound is a no-op with no audit)
 * - `barrier_revised`: `{ maxHeldMessages: <number>, oldMaxHeldMessages: <number> }` — a barrier-opted-in subscriber's hold bound was revised (EB-292; only subscriptions that already carry `barrier` delivery can be revised — a never-opted-in subscription refuses until the subscriber opts in again at subscribe time; the new bound reuses the subscribe-time validation (a positive integer), so a failed call leaves the live bound untouched; only `barrier.maxHeldMessages` moves — the epoch tracking (`lastEpoch`, `armedEpoch`) and the already-held messages are untouched, so lowering the bound below the live held count fails open on the next arrival (everything held so far is released at once and the barrier is disarmed, exactly like the subscribe-time bound) while raising it admits more held messages at once, and the new bound governs from the next hold decision, which reads the live value; a revision identical to the current bound is a no-op with no audit)
 * - `window_revised`: `{ windowMs: <number>, oldWindowMs: <number> }` — a windowed subscriber's tumbling-window length was revised (EB-293; only subscriptions that already carry `window` aggregation can be revised — a never-opted-in subscription refuses until the subscriber opts in again at subscribe time; the new length reuses the subscribe-time validation (`resolveWindowOptions`: a positive finite number), so a failed call leaves the live length untouched; only `window.windowMs` moves — the aggregate fold and the extracted field never move (a differing `aggregate` or `field` throws `RangeError` with the live configuration untouched, because changing either would reinterpret the messages already folded into the open window), and the open window is untouched — its `windowStart` / `windowEnd` / accumulators / armed timer keep the old length, so the new length governs only from the next window opening, which reads the live value; a revision identical to the current length is a no-op with no audit)
 * - `delayed_topic_resumed`: `{ topicPattern: <string> }` — the delayed-delivery sweep hold was lifted (EB-250; the `pattern` field carries the rule key)
 * - `delayed_key_held`: `{ topicPattern: <string>, key: <string> }` — the delayed-delivery sweep was held for one key in the pattern (EB-252; the `pattern` field carries the topic pattern)
 * - `delayed_key_resumed`: `{ topicPattern: <string>, key: <string> }` — the delayed-delivery sweep hold for one key in the pattern was lifted (EB-252; the `pattern` field carries the topic pattern)
 * - `delayed_predicate_held`: `{ topicPattern: <string>, delayId: <string> }` — one pending delayed schedule was held by predicate (EB-262; the `pattern` field carries the topic pattern, `topic` the schedule's concrete topic, one event per held schedule)
 * - `delayed_predicate_resumed`: `{ topicPattern: <string>, delayId: <string> }` — one predicate-held delayed schedule was released (EB-262; same field convention as the hold event)
 * - `config_applied`: `{ applied: <string[]> }` — paths applied by
 *   `applyConfig` (EB-91), in application order; empty on a no-op
 * - `config_rolled_back`: `{ failedPath: <string>, applied: <string[]>,
 *   rolledBack: <boolean> }` — a failed `applyConfig` (EB-91) restored
 *   the pre-apply state after `failedPath` threw
 * - `segment_checksum_failed`: `{ path: <archive file>, segmentId }` —
 *   an archived segment failed checksum verification (EB-94) and was
 *   isolated: skipped, never fatal (the `topic` field carries the topic)
 * - `barrier_dropped`: `{ epoch: <discarded epoch>, lastEpoch: <highest
 *   epoch seen> }` — an out-of-order barrier was discarded for epoch
 *   monotonicity (EB-98): never delivered, never arming (the `topic`
 *   field carries the barrier's topic, `subId` the subscriber)
 * - `subscriber_paused`: `{ reason?: <operator reason> }` — delivery to
 *   one subscriber was manually paused (EB-113); `subId`/`pattern`
 *   identify the subscription
 * - `subscriber_resumed`: no detail — a manual subscriber pause (EB-113)
 *   was cleared; `subId`/`pattern` identify the subscription
 * - `subscriber_seek`: `{ mode: 'earliest'|'latest'|'seq'|'timestamp',
 *   topics: [{ topic, from, requested, position, replayed, droppedQueued,
 *   clampedByCommitted? }], invalidatedInflight }` — a manual
 *   consumer-offset seek (EB-130) repositioned the subscriber's
 *   consumption position; `subId`/`pattern` identify the subscription
 * - `subscriber_quarantined`: `{ consecutiveErrors: <count> }` — handler
 *   fault isolation (EB-197) auto-paused delivery after the consecutive
 *   error budget tripped; `subId`/`pattern` identify the subscription
 * - `subscriber_quarantine_resumed`: no detail — a
 *   `resumeQuarantined` (EB-197) cleared the quarantine;
 *   `subId`/`pattern` identify the subscription
 * - `delivery_sample_rate_updated`: `{ from: <old rate>, to: <new rate> }`
 *   — a `setDeliverySampleRate` (EB-152) hot-update changed the
 *   subscriber's delivery-sampling divisor; `subId`/`pattern` identify
 *   the subscription
 * - `header_filter_updated`: `{ rules: <rule count after the update> }`
 *   — a `setHeaderFilter` (EB-153) hot-update replaced the subscriber's
 *   header-filter rules (`0` when the filter was cleared);
 *   `subId`/`pattern` identify the subscription
 * - `subscriber_labels_updated`: `{ previous: <labels before>, labels: <labels after> }`
 *   — a `setSubscriberLabels` (EB-213) hot-update replaced the
 *   subscriber's labels (`{}` when cleared); `subId`/`pattern` identify
 *   the subscription
 * - `client_pressure`: `{ remoteAddress?, queuedBytes, watermarkBytes,
 *   pausedSubscriptions }` — a gateway (EB-99) connection's outbound
 *   queue crossed its client-pressure watermark (EB-161): the
 *   connection's bus subscriptions were paused so a slow client can no
 *   longer grow the gateway process's memory; `pausedSubscriptions` is
 *   the number of subscriptions this event paused
 * - `client_pressure_recovered`: `{ remoteAddress?, queuedBytes,
 *   recoveryWatermarkBytes, resumedSubscriptions }` — the same
 *   connection's outbound queue drained back under the recovery
 *   watermark and the pressure-paused subscriptions were resumed
 * - `tenant_quota_warning`: `{ namespace, pattern, usedMessages,
 *   usedBytes, softMessages?, softBytes?, windowMs }` — a namespace
 *   tenant quota (EB-162) reached its soft threshold; latched per
 *   excursion (the `topic` field carries the concrete topic that
 *   crossed the threshold)
 * - `bridge_unhealthy`: `{ transport, consecutiveFailures,
 *   lastProbeAt? }` — the bridge transport health monitor (EB-168)
 *   reached its consecutive-failure threshold and paused outbound
 *   mirroring; local delivery is unaffected
 * - `bridge_recovered`: `{ transport, consecutiveFailures,
 *   lastProbeAt? }` — a later probe or mirror succeeded and outbound
 *   mirroring resumed
 * - `mirror_throttled`: `{ srcPattern, dstTemplate, messagesPerSec }` —
 *   a mirror frame shed by its rule's traffic throttle (EB-172) before
 *   reaching admission; the `pattern` field carries the source pattern
 * - `mirror_throttle_revised`: `{ srcPattern, throttle: { messagesPerSec, burst } | null, oldThrottle: { messagesPerSec, burst } | null }` — one live mirror rule's traffic throttle was revised (EB-337; `null` means the throttle was cleared back to unlimited; the new value reuses the construction-time validation (`validateMirrorThrottle`: the rate and the burst positive finite numbers, the burst defaulting to one second of budget), so a failed call leaves the live throttle untouched; a rule that already had a throttle is retuned in place (`TokenBucket.retune`: the stored budget is refilled at the old rate up to now and then capped at the new burst, never topped up), a rule that had none starts from a full bucket, and clearing drops the bucket so frames pass through; a revision identical to the current throttle is a no-op with no audit; every live rule sharing the source pattern is revised, each audited once; the `pattern` field carries the source pattern)
 */
export interface SubscriptionAuditEvent {
  v: 1;
  /** Milliseconds on the bus clock. */
  at: number;
  action: SubscriptionAuditAction;
  subId?: string;
  groupId?: string;
  pattern?: string;
  topic?: string;
  /** Who made the change; defaults to `'system'` when the caller supplies none. */
  actor: string;
  detail?: Record<string, unknown>;
  /**
   * Tamper-evident hash chain (EB-198): `prevHash` is the previous
   * event's `hash` (the fixed genesis for the first event), `hash`
   * covers the whole event. Both are always present on events written
   * by `append()`; events recovered from pre-chain journal files carry
   * neither and verify as chain anchors.
   */
  prevHash?: string;
  hash?: string;
}

/** Query filters for `SubscriptionAuditLog.query`. */
export interface SubscriptionAuditQuery {
  /** Inclusive lower bound on `at`. */
  since?: number;
  /** Exclusive upper bound on `at`. */
  until?: number;
  /** One action or a set of actions. */
  action?: SubscriptionAuditAction | SubscriptionAuditAction[];
  /** Return at most this many of the most recent matching entries. */
  limit?: number;
}

/** Options for `SubscriptionAuditLog`. */
export interface SubscriptionAuditOptions {
  /** Opt-in disk directory for the daily JSONL files. */
  dir?: string;
  /** Bounded in-memory index size; oldest entries evicted first. */
  maxEntries?: number;
  /** Clock source; defaults to `Date.now`. */
  now?: () => number;
}

export const SUBSCRIPTION_AUDIT_DEFAULT_MAX_ENTRIES = 1000;

const ALL_ACTIONS: ReadonlySet<string> = new Set<string>([
  'subscribe',
  'unsubscribe',
  'update_pattern',
  'group_join',
  'group_leave',
  'group_paused',
  'group_resumed',
  'rebalance',
  'topic_register',
  'topic_deregister',
  'topic_created',
  'topic_deleted',
  'topic_renamed',
  'topic_forked',
  'topics_merged',
  'topics_split',
  'topic_data_imported',
  'backpressure_dropped',
  'signature_verification_failed',
  'retention_updated',
  'retained_rule_set',
  'retained_rule_cleared',
  'retained_ttl_rule_set',
  'retained_ttl_rule_cleared',
  'retained_cleared',
  'topic_paused',
  'topic_resumed',
  'topic_pause_buffer_limit_revised',
  'delayed_cancelled',
  'delayed_rescheduled',
  'delayed_ttl_extended',
  'recurring_job_cancelled',
  'recurring_job_revised',
  'ack_deadline_extended',
  'ack_timeout_revised',
  'redelivery_backoff_revised',
  'delivery_window_revised',
  'max_inflight_revised',
  'filter_revised',
  'delivery_shaping_revised',
  'health_probe_revised',
  'throttle_revised',
  'batch_revised',
  'retry_ladder_revised',
  'dead_letter_budget_revised',
  'dead_letter_auto_replay_revised',
  'dead_letter_approval_revised',
  'rate_limit_revised',
  'delivery_priority_revised',
  'topic_fairness_revised',
  'lag_monitor_revised',
  'slow_handler_revised',
  'handler_fault_isolation_revised',
  'delivery_latency_revised',
  'delivery_concurrency_revised',
  'lag_forecast_revised',
  'ack_latency_revised',
  'session_window_revised',
  'queue_size_revised',
  'queue_max_bytes_revised',
  'subscriber_max_payload_bytes_revised',
  'subscriber_schema_version_revised',
  'key_range_revised',
  'backpressure_tier_revised',
  'drop_policy_revised',
  'multi_patterns_revised',
  'filter_expr_revised',
  'expiring_soon_threshold_revised',
  'key_hotspot_threshold_revised',
  'key_hotspot_recover_depth_revised',
  'key_hotspot_auto_mitigate_revised',
  'cluster_link_heartbeat_revised',
  'cluster_link_reconnect_backoff_revised',
  'deduplicate_messages_revised',
  'tail_sampling_revised',
  'partition_health_revised',
  'otlp_revised',
  'otlp_metrics_revised',
  'chaos_revised',
  'global_publish_rate_limit_revised',
  'outbox_relay_revised',
  'schedule_skew_revised',
  'admission_latency_revised',
  'bridge_health_revised',
  'bridge_revised',
  'cep_sequence_pattern_revised',
  'cep_sequence_pattern_paused',
  'cep_absence_window_revised',
  'latency_slo_threshold_revised',
  'latency_slo_window_size_revised',
  'causal_revised',
  'barrier_revised',
  'window_revised',
  'delayed_topic_held',
  'delayed_topic_resumed',
  'delayed_key_held',
  'delayed_key_resumed',
  'delayed_predicate_held',
  'delayed_predicate_resumed',
  'partition_rule_set',
  'partition_rule_cleared',
  'partition_expansion',
  'topic_partition_scaled',
  'barrier_dropped',
  'subscriber_paused',
  'subscriber_resumed',
  'subscriber_seek',
  'subscriber_quarantined',
  'subscriber_quarantine_resumed',
  'delivery_sample_rate_updated',
  'header_filter_updated',
  'subscriber_labels_updated',
  'client_pressure',
  'client_pressure_recovered',
  'tenant_quota_warning',
  'bridge_unhealthy',
  'bridge_recovered',
  'mirror_throttled',
  'mirror_throttle_revised',
  'topic_key_rotated',
  'decrypt_failed',
  'auto_remediate_resume',
  'auto_remediate_linger_pruned',
  'auto_remediate_rebalance',
  'auto_remediate_run',
  'auto_remediate_revised',
  'ops_alert_revised',
  'ops_alert_secret_rotated',
  'admission_webhook_revised',
  'publish_audit_revised',
  'priority_lanes_revised',
  'trace_revised',
  'dedup_bloom_revised',
  'lag_slo_revised',
  'trace_archive_revised',
  'idempotency_revised',
  'lineage_revised',
  'drop_journal_revised',
  'pull_buffer_revised',
  'fan_out_chunk_size_revised',
  'subscription_audit_revised',
  'durable_log_max_entries_revised',
  'delayed_capacity_revised',
  'topic_delayed_capacity_revised',
  'key_delayed_capacity_revised',
  'topic_durable_log_max_age_revised',
  'rpc_settled_retention_revised',
  'publish_rate_ring_capacity_revised',
  'namespace_options_revised',
  'composite_patterns_revised',
  'durable_log_max_bytes_revised',
  'durable_log_publish_dedup_max_entries_revised',
  'durable_log_segment_rotation_revised',
  'durable_log_readback_cache_revised',
  'compression_auto_train_revised',
  'publish_circuit_breaker_revised',
  'subscriber_breaker_revised',
  'durable_log_retain_local_segments_revised',
  'durable_log_encryption_key_rotated',
  'durable_log_segment_offload_client_revised',
  'durable_log_key_compaction_revised',
  'state_store_max_keys_revised',
  'processing_latency_slo_alert_revised',
  'slow_start_revised',
  'group_rebalance_debounce_revised',
  'idle_window_revised',
  'idle_activity_revised',
  'group_handoff_linger_revised',
  'group_auto_commit_revised',
  'group_assignor_revised',
  'group_offsets_seeked',
  'group_session_timeout_revised',
  'group_auto_offset_reset_revised',
  'group_auto_offset_reset_cleared',
  'group_default_session_timeout_revised',
  'group_default_session_timeout_cleared',
  'subscription_lease_revised',
  'stream_join_window_revised',
  'stream_join_max_buffered_per_key_revised',
  'stream_join_how_revised',
  'allowed_lateness_revised',
  'auth_token_revised',
  'group_rebalance_triggered',
  'dead_letter_replay_requested',
  'dead_letter_replay_approved',
  'dead_letter_replay_rejected',
]);

function isAuditAction(value: unknown): value is SubscriptionAuditAction {
  return typeof value === 'string' && ALL_ACTIONS.has(value);
}

function utcDay(at: number): string {
  const d = new Date(at);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const day = String(d.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

/** Validates an audit query; throws `RangeError` on illegal values. */
export function assertValidSubscriptionAuditQuery(query: SubscriptionAuditQuery): void {
  const { since, until, action, limit } = query;
  if (since !== undefined && (!Number.isFinite(since) || since < 0)) {
    throw new RangeError('getSubscriptionAudit: since must be a finite non-negative timestamp');
  }
  if (until !== undefined && (!Number.isFinite(until) || until < 0)) {
    throw new RangeError('getSubscriptionAudit: until must be a finite non-negative timestamp');
  }
  if (action !== undefined) {
    const actions = Array.isArray(action) ? action : [action];
    for (const a of actions) {
      if (!isAuditAction(a)) {
        throw new RangeError(`getSubscriptionAudit: unknown action ${JSON.stringify(a)}`);
      }
    }
  }
  if (limit !== undefined && (!Number.isInteger(limit) || limit < 1)) {
    throw new RangeError('getSubscriptionAudit: limit must be a positive integer');
  }
}

/** Validates an actor name; throws `RangeError` on illegal values. */
export function assertValidActor(actor: unknown, where: string): asserts actor is string {
  if (typeof actor !== 'string' || actor.length === 0) {
    throw new RangeError(`${where}: actor must be a non-empty string`);
  }
}

/**
 * Validates `SubscriptionAuditOptions.maxEntries` the way the
 * constructor would (EB-321): a positive integer. Throws `RangeError`
 * on an illegal value — before anything is mutated. Shared by the
 * constructor, `SubscriptionAuditLog.retune`, and
 * `EventBus.setSubscriptionAudit`, so a value that could not be set at
 * construction cannot be revised in later either.
 */
export function assertValidSubscriptionAuditMaxEntries(value: unknown): void {
  if (!Number.isInteger(value) || (value as number) < 1) {
    throw new RangeError('subscriptionAuditMaxEntries must be a positive integer');
  }
}

export class SubscriptionAuditLog {
  private readonly dir: string | undefined;
  private maxEntries: number;
  private readonly now: () => number;
  private readonly entries: SubscriptionAuditEvent[] = [];
  private diskWriteErrors = 0;
  /**
   * Tamper-evident chain head (EB-198): the effective hash of the most
   * recently appended or recovered event — the `prevHash` of the next
   * event. Starts at the genesis; recovery advances it past the on-disk
   * history so the chain continues across restarts.
   */
  private headHash: string = AUDIT_CHAIN_GENESIS;

  constructor(options: SubscriptionAuditOptions = {}) {
    const { dir, maxEntries = SUBSCRIPTION_AUDIT_DEFAULT_MAX_ENTRIES, now = Date.now } = options;
    if (dir !== undefined && (typeof dir !== 'string' || dir.length === 0)) {
      throw new RangeError('subscriptionAuditDir must be a non-empty string');
    }
    assertValidSubscriptionAuditMaxEntries(maxEntries);
    if (typeof now !== 'function') {
      throw new RangeError('subscriptionAudit now must be a function');
    }
    this.dir = dir;
    this.maxEntries = maxEntries;
    this.now = now;
    if (dir !== undefined) {
      this.recover(dir);
    }
  }

  /**
   * Retunes the in-memory retention bound live (EB-321). Only the
   * bound moves — the audit chain itself is untouched (`dir` / `now`
   * and the hash chain head are unchanged), and the entries already
   * recorded are retained, never cleared or rewritten: shrinking does
   * not evict immediately — the next `append` evicts the oldest rows
   * down to the live bound — while a widened bound simply retains
   * more. Validates fail-fast with the construction-time rule, so an
   * illegal value leaves the live bound exactly as it was.
   */
  retune(maxEntries: number): void {
    assertValidSubscriptionAuditMaxEntries(maxEntries);
    this.maxEntries = maxEntries;
  }

  /** The current retention bound — the live value `retune` moves. */
  current(): number {
    return this.maxEntries;
  }

  /**
   * Appends one event. The timestamp is stamped here on the bus clock, so
   * callers pass everything except `v`/`at`. The event is tamper-evident
   * chained (EB-198): `prevHash`/`hash` are attached before the
   * in-memory append and the disk write, so both layers carry the same
   * links. Never throws: disk failures are counted in `writeErrors()`
   * and the in-memory entry always lands.
   */
  append(event: Omit<SubscriptionAuditEvent, 'v' | 'at'> & { at?: number }): void {
    const full: SubscriptionAuditEvent = {
      v: 1,
      at: event.at ?? this.now(),
      action: event.action,
      actor: event.actor,
      ...(event.subId === undefined ? {} : { subId: event.subId }),
      ...(event.groupId === undefined ? {} : { groupId: event.groupId }),
      ...(event.pattern === undefined ? {} : { pattern: event.pattern }),
      ...(event.topic === undefined ? {} : { topic: event.topic }),
      ...(event.detail === undefined ? {} : { detail: event.detail }),
    };
    const linked = attachChainLink(this.headHash, full);
    this.headHash = linked.hash;
    this.entries.push(linked);
    if (this.entries.length > this.maxEntries) {
      this.entries.splice(0, this.entries.length - this.maxEntries);
    }
    if (this.dir !== undefined) {
      this.appendToDisk(linked);
    }
  }

  /**
   * Queries the in-memory index: filters by time window and action, then
   * returns at most `limit` of the most recent matches (oldest first).
   * Throws `RangeError` on illegal query values.
   */
  query(query: SubscriptionAuditQuery = {}): SubscriptionAuditEvent[] {
    assertValidSubscriptionAuditQuery(query);
    const { since, until, action, limit } = query;
    const actions =
      action === undefined ? undefined : new Set(Array.isArray(action) ? action : [action]);
    const matched: SubscriptionAuditEvent[] = [];
    for (const e of this.entries) {
      if (since !== undefined && e.at < since) continue;
      if (until !== undefined && e.at >= until) continue;
      if (actions !== undefined && !actions.has(e.action)) continue;
      matched.push(e);
    }
    if (limit !== undefined && matched.length > limit) {
      return matched.slice(matched.length - limit);
    }
    return matched;
  }

  /** In-memory index size. */
  size(): number {
    return this.entries.length;
  }

  /** Disk append failures since construction (0 when no `dir` configured). */
  writeErrors(): number {
    return this.diskWriteErrors;
  }

  private fileFor(at: number): string {
    return join(this.dir as string, `audit-${utcDay(at)}.jsonl`);
  }

  private appendToDisk(event: SubscriptionAuditEvent): void {
    try {
      appendFileSync(this.fileFor(event.at), JSON.stringify(event) + '\n', 'utf8');
    } catch {
      this.diskWriteErrors += 1;
    }
  }

  /**
   * Back-fills the in-memory ring from existing daily files (oldest file
   * first, newest lines last), skipping corrupt lines. Failures here are
   * silent by design: a damaged audit directory must never prevent the
   * bus from starting.
   */
  private recover(dir: string): void {
    let files: string[];
    try {
      mkdirSync(dir, { recursive: true });
      files = readdirSync(dir)
        .filter((f) => f.startsWith('audit-') && f.endsWith('.jsonl'))
        .sort();
    } catch {
      return;
    }
    const recovered: SubscriptionAuditEvent[] = [];
    for (const file of files) {
      let text: string;
      try {
        text = readFileSync(join(dir, file), 'utf8');
      } catch {
        continue;
      }
      for (const line of text.split('\n')) {
        const trimmed = line.trim();
        if (trimmed === '') continue;
        const event = parseAuditLine(trimmed);
        if (event !== undefined) recovered.push(event);
      }
    }
    if (recovered.length > this.maxEntries) {
      recovered.splice(0, recovered.length - this.maxEntries);
    }
    this.entries.push(...recovered);
    if (recovered.length > 0) {
      // The chain continues across restarts: the next appended event
      // links to the last recovered one (see `headHash`).
      this.headHash = effectiveRecordHash(recovered[recovered.length - 1]);
    }
  }

  /**
   * Verifies the tamper-evident hash chain (EB-198) over the in-memory
   * index: detects rewritten fields (`content-tampered`), deleted or
   * spliced events (`link-broken`), and reports the first break. The
   * oldest retained event is the segment anchor — its link to evicted
   * predecessors cannot be checked (see `src/auditchain.ts`).
   */
  verifyAuditChain(): ChainVerification {
    return verifyChainLinks(this.entries);
  }
}

/**
 * Parses one JSONL audit line; returns `undefined` for corrupt lines
 * (bad JSON, wrong shape) so recovery skips them instead of dying.
 */
export function parseAuditLine(line: string): SubscriptionAuditEvent | undefined {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    return undefined;
  }
  if (typeof parsed !== 'object' || parsed === null) return undefined;
  const o = parsed as Record<string, unknown>;
  if (o['v'] !== 1) return undefined;
  if (!isAuditAction(o['action'])) return undefined;
  if (typeof o['at'] !== 'number' || !Number.isFinite(o['at'])) return undefined;
  if (typeof o['actor'] !== 'string' || o['actor'].length === 0) return undefined;
  const event: SubscriptionAuditEvent = {
    v: 1,
    at: o['at'],
    action: o['action'],
    actor: o['actor'],
  };
  for (const key of ['subId', 'groupId', 'pattern', 'topic'] as const) {
    const value = o[key];
    if (value !== undefined) {
      if (typeof value !== 'string') return undefined;
      event[key] = value;
    }
  }
  if (o['detail'] !== undefined) {
    if (typeof o['detail'] !== 'object' || o['detail'] === null) return undefined;
    event.detail = o['detail'] as Record<string, unknown>;
  }
  // Tamper-evident chain fields (EB-198): optional for backward
  // compatibility with pre-chain journal files; when present they must
  // be well-formed digests, otherwise the line is corrupt.
  for (const key of ['prevHash', 'hash'] as const) {
    const value = o[key];
    if (value !== undefined) {
      if (!isHashString(value)) return undefined;
      event[key] = value;
    }
  }
  return event;
}

