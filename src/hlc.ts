/**
 * Hybrid Logical Clock (EB-131), after Kulkarni et al. ("Logical Physical
 * Clocks", OPODIS 2014).
 *
 * Active-active mirror topologies need a write timestamp that stays
 * meaningful when the two buses' wall clocks disagree: a plain
 * `Date.now()` lets a skewed-but-later write lose to an earlier one,
 * and a pure Lamport clock loses the wall-clock anchor operators
 * expect. The HLC keeps both — a physical component (`pt`, epoch
 * millis from the injected clock) and a logical component (`l`) that
 * ticks forward whenever the physical clock stands still or a remote
 * timestamp arrives from the future.
 *
 * Rules, exactly the paper's:
 * - local event (`tick`): `pt = max(now, last.pt)`; `l` increments when
 *   `pt` did not advance, else resets to 0.
 * - receive (`receive`): `pt = max(now, last.pt, remote.pt)`; `l` is
 *   `max(last.l, remote.l) + 1` when `pt` ties both predecessors,
 *   `last.l + 1` / `remote.l + 1` when it ties exactly one, else 0.
 * - order (`compareHlc`): `pt`, then `l`, then the node id
 *   lexicographically — two distinct writes never compare equal, so
 *   last-writer-wins arbitration needs no extra tiebreak.
 *
 * The clock is monotonic per instance even when the injected clock runs
 * backward, and it never allocates on the hot path beyond the returned
 * timestamp object. The bus injects its own `now` (see
 * `EventBusOptions.now`), so tests drive the clock deterministically.
 * This module is pure: no bus state, no I/O.
 */

/** A hybrid logical timestamp: physical millis, logical ticks, node id. */
export interface HlcTimestamp {
  /** Physical component: epoch milliseconds from the injected clock. */
  pt: number;
  /** Logical component: disambiguates events within the same millisecond. */
  l: number;
  /** Node id: the final tiebreak — two distinct writes never tie. */
  node: string;
}

/**
 * Total order over HLC timestamps: physical time, then logical ticks,
 * then the node id lexicographically. Returns -1, 0 or 1.
 */
export function compareHlc(a: HlcTimestamp, b: HlcTimestamp): -1 | 0 | 1 {
  if (a.pt !== b.pt) return a.pt < b.pt ? -1 : 1;
  if (a.l !== b.l) return a.l < b.l ? -1 : 1;
  if (a.node !== b.node) return a.node < b.node ? -1 : 1;
  return 0;
}

/** True when `value` has the shape of an `HlcTimestamp`. */
export function isHlcTimestamp(value: unknown): value is HlcTimestamp {
  if (value === null || typeof value !== 'object') return false;
  const t = value as Record<string, unknown>;
  return (
    typeof t.pt === 'number' &&
    Number.isFinite(t.pt) &&
    typeof t.l === 'number' &&
    Number.isInteger(t.l) &&
    t.l >= 0 &&
    typeof t.node === 'string' &&
    t.node.length > 0
  );
}

/**
 * Compact text form `"<pt>.<l>.<node>"`, for logs and tests. The node id
 * is emitted verbatim — it must not contain a `.` for `parseHlc` to
 * round-trip (bus ids never do; see `EventBusOptions.busId`).
 */
export function formatHlc(t: HlcTimestamp): string {
  return `${t.pt}.${t.l}.${t.node}`;
}

/**
 * Parses `formatHlc` output. Returns `null` on any malformed input —
 * a corrupt timestamp never throws, it just does not parse.
 */
export function parseHlc(text: string): HlcTimestamp | null {
  if (typeof text !== 'string') return null;
  const first = text.indexOf('.');
  const second = text.indexOf('.', first + 1);
  if (first <= 0 || second <= first + 1 || second === text.length - 1) return null;
  const pt = Number(text.slice(0, first));
  const l = Number(text.slice(first + 1, second));
  const node = text.slice(second + 1);
  const t: HlcTimestamp = { pt, l, node };
  return isHlcTimestamp(t) ? t : null;
}

/** A hybrid logical clock bound to one node id and one physical clock. */
export class HybridLogicalClock {
  private readonly nodeId: string;
  private readonly now: () => number;
  private last: HlcTimestamp;

  /**
   * @param nodeId stable identity of this node (the bus id); must be
   * non-empty — an empty id would make concurrent timestamps from two
   * nodes compare equal.
   * @param now physical clock, epoch millis; the bus injects its own.
   */
  constructor(nodeId: string, now: () => number = Date.now) {
    if (typeof nodeId !== 'string' || nodeId.length === 0) {
      throw new RangeError('HybridLogicalClock: nodeId must be a non-empty string');
    }
    if (typeof now !== 'function') {
      throw new RangeError('HybridLogicalClock: now must be a function');
    }
    this.nodeId = nodeId;
    this.now = now;
    this.last = { pt: 0, l: 0, node: nodeId };
  }

  /** The node's own latest timestamp (a copy — mutating it changes nothing). */
  latest(): HlcTimestamp {
    return { ...this.last };
  }

  /**
   * Stamps a local event: advances the physical component to at least
   * the injected clock and ticks the logical component when physical
   * time did not advance. Monotonic even if the injected clock runs
   * backward.
   */
  tick(): HlcTimestamp {
    const nowMs = this.now();
    const pt = Math.max(nowMs, this.last.pt);
    const l = pt === this.last.pt ? this.last.l + 1 : 0;
    this.last = { pt, l, node: this.nodeId };
    return { ...this.last };
  }

  /**
   * Merges a remote timestamp (a message received from another node):
   * the clock jumps past the remote time so a later local `tick()`
   * orders after the remote write even under clock skew. Ignores
   * malformed input — a corrupt remote timestamp must not move the
   * clock. Returns the merged timestamp.
   */
  receive(remote: HlcTimestamp): HlcTimestamp {
    if (!isHlcTimestamp(remote)) return { ...this.last };
    const nowMs = this.now();
    const pt = Math.max(nowMs, this.last.pt, remote.pt);
    let l: number;
    if (pt === this.last.pt && pt === remote.pt) {
      l = Math.max(this.last.l, remote.l) + 1;
    } else if (pt === this.last.pt) {
      l = this.last.l + 1;
    } else if (pt === remote.pt) {
      l = remote.l + 1;
    } else {
      l = 0;
    }
    this.last = { pt, l, node: this.nodeId };
    return { ...this.last };
  }
}
