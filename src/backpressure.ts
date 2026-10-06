export type DropPolicy = 'drop-oldest' | 'drop-newest';

/**
 * Message priority for shed decisions. Higher values are shed later:
 * when the queue is full under `drop-oldest`, the oldest entry with the
 * lowest priority is dropped first. The default priority is 0.
 */
export type Priority = number;

export interface BoundedQueueOptions<T> {
  capacity: number;
  policy?: DropPolicy;
  /** Fired once when size >= capacity * 0.8; re-arms after the size recedes below the watermark. */
  onHighWaterMark?: (size: number) => void;
}

interface QueueEntry<T> {
  item: T;
  priority: Priority;
}

export class BoundedQueue<T> {
  private entries: QueueEntry<T>[] = [];
  private readonly capacity: number;
  private readonly policy: DropPolicy;
  private readonly onHighWaterMark?: (size: number) => void;
  private highWaterMarkReached = false;
  private dropped = 0;
  private readonly droppedByPriorityCount = new Map<Priority, number>();

  constructor(options: BoundedQueueOptions<T>) {
    if (!Number.isInteger(options.capacity) || options.capacity <= 0) {
      throw new RangeError('capacity must be a positive integer');
    }
    this.capacity = options.capacity;
    this.policy = options.policy ?? 'drop-oldest';
    this.onHighWaterMark = options.onHighWaterMark;
  }

  get size(): number {
    return this.entries.length;
  }

  get droppedCount(): number {
    return this.dropped;
  }

  /** Per-priority drop counts, useful for backpressure observability. */
  get droppedByPriority(): ReadonlyMap<Priority, number> {
    return this.droppedByPriorityCount;
  }

  private get highWaterMark(): number {
    return this.capacity * 0.8;
  }

  /**
   * Enqueues an item. When the queue is full:
   * - `drop-newest`: the incoming item is discarded.
   * - `drop-oldest`: the oldest entry with the lowest priority is shed to make
   *   room — unless the incoming item itself is the strictly lowest-priority
   *   one, in which case the incoming item is discarded instead. High-priority
   *   messages are therefore dropped last.
   */
  push(item: T, priority: Priority = 0): 'accepted' | 'dropped' {
    if (!Number.isFinite(priority)) {
      throw new RangeError('priority must be a finite number');
    }
    if (this.entries.length < this.capacity) {
      this.entries.push({ item, priority });
      this.checkHighWaterMark();
      return 'accepted';
    }
    if (this.policy === 'drop-newest') {
      this.recordDrop(priority);
      return 'dropped';
    }
    // 'drop-oldest' with priority-aware shedding.
    const victimIndex = this.findShedVictimIndex(priority);
    if (victimIndex === -1) {
      // The incoming item is strictly lower priority than everything queued:
      // dropping it protects the higher-priority backlog.
      this.recordDrop(priority);
      return 'dropped';
    }
    this.recordDrop(this.entries[victimIndex].priority);
    this.entries.splice(victimIndex, 1);
    this.entries.push({ item, priority });
    this.checkHighWaterMark();
    return 'accepted';
  }

  /** Removes and returns every queued item in FIFO order. */
  drain(): T[] {
    const items = this.entries.map((entry) => entry.item);
    this.entries = [];
    this.highWaterMarkReached = false;
    return items;
  }

  /**
   * Index of the shed victim among the queued entries: the oldest entry whose
   * priority is minimal. Returns -1 when the incoming item is strictly lower
   * priority than every queued entry (so the incoming item should be dropped).
   */
  private findShedVictimIndex(incomingPriority: Priority): number {
    let minPriority = Infinity;
    for (const entry of this.entries) {
      if (entry.priority < minPriority) minPriority = entry.priority;
    }
    if (incomingPriority < minPriority) return -1;
    return this.entries.findIndex((entry) => entry.priority === minPriority);
  }

  private recordDrop(priority: Priority): void {
    this.dropped += 1;
    this.droppedByPriorityCount.set(
      priority,
      (this.droppedByPriorityCount.get(priority) ?? 0) + 1,
    );
  }

  private checkHighWaterMark(): void {
    if (this.onHighWaterMark == null) return;
    if (!this.highWaterMarkReached && this.entries.length >= this.highWaterMark) {
      this.highWaterMarkReached = true;
      this.onHighWaterMark(this.entries.length);
    }
  }
}
