/**
 * Cross-instance topic mirroring (EB-37).
 *
 * Where `EventBus.forward` is a *push* registration on the source bus
 * ("offer every admitted message on these topics to that bus"), `mirror`
 * is the *pull* counterpart registered on the destination bus: "tap this
 * other bus instance's topics into mine". The destination opts in with an
 * explicit rule table, so a bus never receives traffic it did not ask for.
 *
 * Three things distinguish a mirror from a forward:
 *
 * 1. **Topic rename rules** — each rule maps a source pattern to a
 *    destination topic template. `$1`, `$2`, ... expand the pattern's
 *    wildcard captures (`*` captures one segment, `**` captures a run of
 *    segments joined with `.`); `$$` is a literal dollar. Omitting
 *    `rename` mirrors onto the source topic unchanged.
 * 2. **Loop detection** — every mirrored message carries its provenance
 *    (`MirrorProvenance`): the origin bus id, the origin topic, the
 *    origin per-topic sequence number, and the trail of bus ids it has
 *    passed through. A mirror drops any message whose trail already
 *    contains its own bus id, so mirror cycles (A→B→A, A→B→C→A, or a
 *    self-mirror onto its own source topic) terminate after one pass
 *    instead of amplifying forever. Drops are counted per rule as
 *    `loopsPrevented`.
 * 3. **Origin sequence preservation** — the destination bus still draws
 *    its own per-topic sequence numbers (its topic stream must stay
 *    monotonic), but the mirrored message keeps the origin `seq` in
 *    `mirror.seq` and rides the `mirror:<busId>` sequence epoch, so the
 *    built-in gap detector re-baselines on the mirror stream instead of
 *    counting phantom gaps, and downstream consumers can run gap
 *    detection against the origin stream via `msg.mirror.seq`.
 *
 * Mirror-ingressed messages run the destination bus's full admission
 * pipeline (ACL, schema, rate-limit, TTL, ...) via the same internal
 * path as forwarded messages, stamped `routed` so they never trigger
 * the destination's routes or forwards again — one hop per message.
 * Like forwards, they do not carry the source's TTL deadline (the
 * subscription tap happens post-admission); the destination's TTL rules
 * apply. `getStats().mirroredMessages` counts what the mirror path
 * handed to admission; `getStats().mirrors` exposes the live rule
 * table.
 *
 * This module is runtime-dependency-free (it only ever imports the
 * `EventBus` type): the bus wires the rules up in `EventBus.mirror`.
 */

/** One cross-instance mirror rule (see `EventBus.mirror`). */
export interface MirrorRule {
  /**
   * Topic pattern on the source bus. Same wildcard semantics as
   * `subscribe`: `*` matches a single segment, `**` matches zero or more
   * segments, every other segment matches literally. Must be non-empty.
   */
  srcPattern: string;
  /**
   * Destination topic template. `$1`..`$n` expand the pattern's wildcard
   * captures in order (`*` yields one segment, `**` yields its run joined
   * with `.`); `$$` is a literal `$`. Referencing a capture the pattern
   * does not yield throws at `mirror()` time. Defaults to the source
   * topic unchanged.
   */
  rename?: string;
}

/**
 * Provenance stamped on every message admitted through the mirror path
 * (see `BusMessage.mirror`). The origin fields (`busId`, `topic`, `seq`)
 * describe the first bus that mirrored the message; `trail` is the full
 * hop chain including the origin — the loop detector's evidence.
 */
export interface MirrorProvenance {
  /** `busId` of the bus where the message was first mirrored from. */
  busId: string;
  /** Topic the message was published to on the origin bus. */
  topic: string;
  /** Per-topic sequence number assigned by the origin bus. */
  seq: number;
  /** Bus ids the message has passed through, origin first. */
  trail: string[];
}

/** One row of `getStats().mirrors` — a live mirror rule's counters. */
export interface MirrorRuleInfo {
  /** The source pattern this rule taps. */
  srcPattern: string;
  /** The rename template, or `'(source topic)'` when unrenamed. */
  dstTemplate: string;
  /**
   * Messages this rule mirrored — counted when the rule handed the
   * message to the destination's admission pipeline, mirroring the
   * `forwards` convention (destination-side admission rejections still
   * count; loop-prevented drops do not).
   */
  mirrored: number;
  /** Messages dropped because their trail already held this bus's id. */
  loopsPrevented: number;
}

/**
 * The live mirror registration returned by `EventBus.mirror`.
 * `close()` stops every rule (idempotent); the rules stay live until
 * then, even if the handle is dropped.
 */
export interface MirrorHandle {
  /** True after `close()` ran. */
  readonly closed: boolean;
  /** Snapshot of the live per-rule counters — mutating it changes nothing. */
  rules(): MirrorRuleInfo[];
  /** Unsubscribe every rule from the source bus. Idempotent. */
  close(): void;
}

/** A validated mirror rule, ready to match source topics. */
export interface ResolvedMirrorRule {
  srcPattern: string;
  dstTemplate: string;
  /** The destination topic for `topic`, or `null` when it does not match. */
  apply(topic: string): string | null;
}

/** Match `patternSegs` against `topicSegs`, returning wildcard captures. */
function matchCaptures(patternSegs: string[], topicSegs: string[]): string[] | null {
  const captures: string[] = [];
  const rec = (pi: number, ti: number): boolean => {
    if (pi === patternSegs.length) return ti === topicSegs.length;
    const p = patternSegs[pi];
    if (p === '**') {
      // Zero or more segments, greedy-longest with backtracking so the
      // split is deterministic (leftmost `**` takes as much as it can).
      for (let k = topicSegs.length - ti; k >= 0; k -= 1) {
        captures.push(topicSegs.slice(ti, ti + k).join('.'));
        if (rec(pi + 1, ti + k)) return true;
        captures.pop();
      }
      return false;
    }
    if (ti >= topicSegs.length) return false;
    if (p === '*') {
      captures.push(topicSegs[ti]);
      if (rec(pi + 1, ti + 1)) return true;
      captures.pop();
      return false;
    }
    return p === topicSegs[ti] && rec(pi + 1, ti + 1);
  };
  return rec(0, 0) ? captures : null;
}

/** Expand `$1`..`$n` / `$$` in a rename template against the captures. */
function expandTemplate(template: string, captures: string[], where: string): string {
  let out = '';
  let i = 0;
  while (i < template.length) {
    const ch = template[i];
    if (ch !== '$') {
      out += ch;
      i += 1;
      continue;
    }
    const rest = template.slice(i + 1);
    if (rest.startsWith('$')) {
      out += '$';
      i += 2;
      continue;
    }
    const m = /^(\d+)/.exec(rest);
    if (m === null) {
      throw new RangeError(
        `${where}: rename template has a dangling '$' — use '$$' for a literal dollar`,
      );
    }
    const idx = Number(m[1]);
    if (idx < 1 || idx > captures.length) {
      throw new RangeError(
        `${where}: rename template references $${m[1]} but the pattern yields ` +
          `${captures.length} wildcard capture(s)`,
      );
    }
    out += captures[idx - 1];
    i += 1 + m[1].length;
  }
  return out;
}

/** Check a template's `$n` references against the pattern's capture count. */
function validateTemplate(template: string, captureCount: number, where: string): void {
  let i = 0;
  while (i < template.length) {
    const ch = template[i];
    if (ch !== '$') {
      i += 1;
      continue;
    }
    const rest = template.slice(i + 1);
    if (rest.startsWith('$')) {
      i += 2;
      continue;
    }
    const m = /^(\d+)/.exec(rest);
    if (m === null) {
      throw new RangeError(
        `${where}: rename template has a dangling '$' — use '$$' for a literal dollar`,
      );
    }
    const idx = Number(m[1]);
    if (idx < 1 || idx > captureCount) {
      throw new RangeError(
        `${where}: rename template references $${m[1]} but the pattern yields ` +
          `${captureCount} wildcard capture(s)`,
      );
    }
    i += 1 + m[1].length;
  }
}

/**
 * Validates one mirror rule the way `EventBus.mirror` would: throws
 * `RangeError` on the first problem — a broken rule fails at
 * registration time, never mid-mirror.
 */
export function resolveMirrorRule(rule: MirrorRule, where: string): ResolvedMirrorRule {
  if (typeof rule !== 'object' || rule === null) {
    throw new RangeError(`${where}: mirror rule must be an object`);
  }
  const { srcPattern, rename } = rule;
  if (typeof srcPattern !== 'string' || srcPattern.length === 0) {
    throw new RangeError(`${where}: srcPattern must be a non-empty string`);
  }
  if (rename !== undefined && (typeof rename !== 'string' || rename.length === 0)) {
    throw new RangeError(`${where}: rename must be a non-empty string when given`);
  }
  const patternSegs = srcPattern.split('.');
  const captureCount = patternSegs.filter((s) => s === '*' || s === '**').length;
  if (rename !== undefined) validateTemplate(rename, captureCount, where);
  return {
    srcPattern,
    dstTemplate: rename ?? '(source topic)',
    apply(topic: string): string | null {
      const captures = matchCaptures(patternSegs, topic.split('.'));
      if (captures === null) return null;
      return rename === undefined ? topic : expandTemplate(rename, captures, where);
    },
  };
}

/**
 * Validates a whole mirror rule table: throws on the first bad rule, so
 * `EventBus.mirror` never half-registers. Returns `undefined` for an
 * empty table (mirror with no rules is a no-op registration).
 */
export function resolveMirrorRules(
  rules: MirrorRule[] | undefined,
  where: string,
): ResolvedMirrorRule[] | undefined {
  if (rules === undefined) return undefined;
  if (!Array.isArray(rules)) {
    throw new RangeError(`${where}: mirror rules must be an array`);
  }
  return rules.map((rule, i) => resolveMirrorRule(rule, `${where}[${i}]`));
}
