export type DropPolicy = 'drop-oldest' | 'drop-newest';

export interface BoundedQueueOptions<T> {
  capacity: number;
  policy?: DropPolicy;
  /** Fired once when size >= capacity * 0.8; re-arms after the size recedes below the watermark. */
  onHighWaterMark?: (size: number) => void;
}

export class BoundedQueue<T> {
  private items: T[] = [];
  private readonly capacity: number;
  private readonly policy: DropPolicy;
  private readonly onHighWaterMark?: (size: number) => void;
  private highWaterMarkReached = false;
  private dropped = 0;

  constructor(options: BoundedQueueOptions<T>) {
    if (!Number.isInteger(options.capacity) || options.capacity <= 0) {
      throw new RangeError('capacity must be a positive integer');
    }
    this.capacity = options.capacity;
    this.policy = options.policy ?? 'drop-oldest';
    this.onHighWaterMark = options.onHighWaterMark;
  }

  get size(): number {
    return this.items.length;
  }

  get droppedCount(): number {
    return this.dropped;
  }

  private get highWaterMark(): number {
    return this.capacity * 0.8;
  }

  push(item: T): 'accepted' | 'dropped' {
    if (this.items.length < this.capacity) {
      this.items.push(item);
      this.checkHighWaterMark();
      return 'accepted';
    }
    // Queue is full: apply the drop policy and count the loss.
    this.dropped += 1;
    if (this.policy === 'drop-oldest') {
      this.items.shift();
      this.items.push(item);
      this.checkHighWaterMark();
      return 'accepted';
    }
    // 'drop-newest': the incoming item is discarded.
    return 'dropped';
  }

  /** Removes and returns every queued item in FIFO order. */
  drain(): T[] {
    const items = this.items;
    this.items = [];
    this.highWaterMarkReached = false;
    return items;
  }

  private checkHighWaterMark(): void {
    if (this.onHighWaterMark == null) return;
    if (!this.highWaterMarkReached && this.items.length >= this.highWaterMark) {
      this.highWaterMarkReached = true;
      this.onHighWaterMark(this.items.length);
    }
  }
}
