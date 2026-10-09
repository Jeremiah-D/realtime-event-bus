/**
 * Subscriber batch-delivery throughput benchmark for EventBus (EB-33).
 *
 * Compares end-to-end delivery throughput with and without
 * `SubscribeOptions.batch` under a realistic streaming pattern: messages
 * arrive in small bursts (100 per flush round, like a market-data tick
 * batch). Two handler profiles are measured:
 *
 * - "lean handler": the handler does only tiny per-message work (field
 *   access + accumulate). Batching removes just the function-call
 *   overhead here.
 * - "framed handler": the handler additionally pays a fixed ~2.6µs
 *   per-invocation cost (framing a bulk write / opening a transaction —
 *   the real reason to batch). Batching amortizes that fixed cost over
 *   `maxSize` messages.
 *
 * The batched runs use `maxWaitMs: 0` so every flush delivers whatever is
 * queued (up to `maxSize`) without artificial lingering — the measured
 * difference is the handler-call amortization, not timer behavior. Each
 * path runs 5 trials; the median is reported to smooth out VM noise.
 *
 * Run: `node bench/batch.bench.ts` (Node >= 22, no build step).
 */
import { EventBus } from '../src/bus.ts';
import { cpus } from 'node:os';

const ROUNDS = 200;
const PER_ROUND = 100;
const MESSAGES = ROUNDS * PER_ROUND;
const BATCH_SIZE = 100;
/** Trials per path; the median is reported to smooth out VM noise. */
const TRIALS = 5;

/**
 * Fixed per-handler-invocation cost (~2.6µs on the bench machine):
 * framing a bulk write, opening a transaction, building a request.
 */
function perCallOverhead(): number {
  let x = 0;
  for (let i = 0; i < 600; i += 1) x += Math.sqrt(i);
  return x;
}

async function waitFor(cond: () => boolean): Promise<void> {
  for (let i = 0; i < 10_000 && !cond(); i += 1) {
    // eslint-disable-next-line no-await-in-loop
    await new Promise((resolve) => setImmediate(resolve));
  }
  if (!cond()) throw new Error('timed out waiting for deliveries');
}

async function benchBatch(
  maxSize: number | null,
  framed: boolean,
): Promise<{ ms: number; handlerCalls: number }> {
  const bus = new EventBus();
  let received = 0;
  let handlerCalls = 0;
  let sum = 0;
  let overhead = 0;
  if (maxSize == null) {
    bus.subscribe(
      't',
      (msg) => {
        handlerCalls += 1;
        received += 1;
        sum += (msg.payload as { v: number }).v;
        if (framed) overhead += perCallOverhead();
      },
      { queueSize: PER_ROUND + 10 },
    );
  } else {
    bus.subscribe(
      't',
      (msgs) => {
        handlerCalls += 1;
        for (const m of msgs) {
          received += 1;
          sum += (m.payload as { v: number }).v;
        }
        if (framed) overhead += perCallOverhead();
      },
      { batch: { maxSize, maxWaitMs: 0 }, queueSize: PER_ROUND + 10 },
    );
  }
  const start = process.hrtime.bigint();
  for (let r = 0; r < ROUNDS; r += 1) {
    for (let i = 0; i < PER_ROUND; i += 1) {
      bus.publish('t', { v: r * PER_ROUND + i });
    }
    // eslint-disable-next-line no-await-in-loop
    await Promise.resolve(); // let the scheduled flush drain before the next burst
  }
  await waitFor(() => received === MESSAGES);
  const ms = Number(process.hrtime.bigint() - start) / 1e6;
  if (sum !== ((MESSAGES - 1) * MESSAGES) / 2) throw new Error('payload checksum mismatch');
  if (framed && overhead === 0) throw new Error('overhead not applied');
  return { ms, handlerCalls };
}

async function scenario(label: string, framed: boolean): Promise<{ plainMs: number; batchedMs: number; calls: [number, number] }> {
  const median = (xs: number[]): number => {
    const sorted = [...xs].sort((a, b) => a - b);
    return sorted[Math.floor(sorted.length / 2)];
  };
  const plainTrials: number[] = [];
  const batchedTrials: number[] = [];
  let plainCalls = 0;
  let batchedCalls = 0;
  for (let i = 0; i < TRIALS; i += 1) {
    // eslint-disable-next-line no-await-in-loop
    const p = await benchBatch(null, framed);
    // eslint-disable-next-line no-await-in-loop
    const b = await benchBatch(BATCH_SIZE, framed);
    plainTrials.push(p.ms);
    batchedTrials.push(b.ms);
    plainCalls = p.handlerCalls;
    batchedCalls = b.handlerCalls;
  }
  const plainMs = median(plainTrials);
  const batchedMs = median(batchedTrials);
  const plainRate = MESSAGES / (plainMs / 1000);
  const batchedRate = MESSAGES / (batchedMs / 1000);
  console.log(`--- ${label} ---`);
  console.log(
    `plain:               ${plainMs.toFixed(1)} ms  (${Math.round(plainRate).toLocaleString()} msgs/sec, ${plainCalls} handler calls)`,
  );
  console.log(
    `batch(maxSize=${BATCH_SIZE}): ${batchedMs.toFixed(1)} ms  (${Math.round(batchedRate).toLocaleString()} msgs/sec, ${batchedCalls} handler calls)`,
  );
  console.log(`speedup:             ${(plainMs / batchedMs).toFixed(2)}x`);
  return { plainMs, batchedMs, calls: [plainCalls, batchedCalls] };
}

async function main(): Promise<void> {
  console.log(`environment: Node ${process.version}, ${cpus()[0].model} (${cpus().length} cores)`);
  console.log(
    `pattern:     ${ROUNDS} rounds x ${PER_ROUND} msgs/round, one flush per round, ${MESSAGES} messages`,
  );
  console.log(`trials:      ${TRIALS} per path, median reported`);

  // Warm up the JIT on every path before measuring.
  await benchBatch(null, false);
  await benchBatch(BATCH_SIZE, false);
  await benchBatch(null, true);
  await benchBatch(BATCH_SIZE, true);

  await scenario('lean handler (per-message work only)', false);
  await scenario('framed handler (+ ~2.6us fixed per-call cost)', true);
}

await main();
