/**
 * Preset-dictionary compression benchmark for per-topic deflate
 * (`setTopicCompression(..., { dictionary })`).
 *
 * Compares wire size and deflate CPU for SMALL JSON payloads — a
 * market-data tick shape (~130 bytes serialized) — with and without a
 * preset dictionary built from a representative sample corpus. Small
 * messages carry too little redundancy for deflate to find on its own,
 * so the dictionary is where the ratio win comes from.
 *
 * Run: `node bench/compress-dict.bench.ts` (Node >= 22, no build step).
 * Numbers below were measured on this machine; see README's compression
 * section for the published table.
 */
import { deflateSync, inflateSync } from 'node:zlib';
import { Buffer } from 'node:buffer';

/** One market-data tick: the small-JSON shape this feature targets. */
function tick(i: number): Record<string, unknown> {
  return {
    symbol: i % 3 === 0 ? 'BTCUSDT' : i % 3 === 1 ? 'ETHUSDT' : 'SOLUSDT',
    venue: 'binance-spot',
    side: i % 2 === 0 ? 'buy' : 'sell',
    price: 97123.45 + (i % 100) * 0.01,
    qty: 0.001 * (1 + (i % 7)),
    trade_id: 8_412_000_000 + i,
    ts: 1_759_400_000_000 + i * 37,
    taker: i % 5 === 0,
  };
}

function percentile(sorted: number[], p: number): number {
  const rank = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, Math.min(sorted.length - 1, rank))];
}

/** Build a dictionary from the first N serialized samples, like a user would. */
function buildDictionary(samples: string[]): Buffer {
  const joined = samples.join('\n');
  // Cap at the 32 KiB zlib limit, exactly like setTopicCompression enforces.
  return Buffer.from(joined.slice(0, 32 * 1024), 'utf8');
}

function bench(label: string, samples: string[], dictionary?: Buffer): void {
  const WARMUP = 500;
  const N = 5_000;
  for (let i = 0; i < WARMUP; i += 1) {
    deflateSync(samples[i % samples.length], dictionary ? { dictionary } : undefined);
  }
  const sizes: number[] = [];
  const timesNs: number[] = [];
  let before = 0;
  for (let i = 0; i < N; i += 1) {
    const s = samples[(WARMUP + i) % samples.length];
    before += Buffer.byteLength(s, 'utf8');
    const t0 = process.hrtime.bigint();
    const out = deflateSync(s, dictionary ? { dictionary } : undefined);
    const t1 = process.hrtime.bigint();
    sizes.push(out.length);
    timesNs.push(Number(t1 - t0));
    // Sanity: every sample must round-trip byte-identically.
    const back = (
      dictionary ? inflateSync(out, { dictionary }) : inflateSync(out)
    ).toString('utf8');
    if (back !== s) throw new Error(`${label}: round-trip mismatch on sample ${i}`);
  }
  const after = sizes.reduce((a, b) => a + b, 0);
  timesNs.sort((a, b) => a - b);
  console.log(
    `${label}: ratio ${(after / before).toFixed(3)} ` +
      `(wire ${after}B / raw ${before}B), ` +
      `deflate p50 ${(percentile(timesNs, 50) / 1e3).toFixed(2)}µs ` +
      `p99 ${(percentile(timesNs, 99) / 1e3).toFixed(2)}µs`,
  );
}

function main(): void {
  const CORPUS = 2_000;
  const corpus = Array.from({ length: CORPUS }, (_, i) => JSON.stringify(tick(i)));
  const rawBytes = corpus.reduce((a, s) => a + Buffer.byteLength(s, 'utf8'), 0);
  console.log(`tick corpus: ${CORPUS} messages, mean ${(rawBytes / CORPUS).toFixed(0)} bytes serialized`);
  const dictionary = buildDictionary(corpus.slice(0, 300));
  console.log(`dictionary: ${dictionary.length} bytes (300 samples, capped at 32 KiB)`);
  bench('no dictionary      ', corpus);
  bench('with dictionary    ', corpus, dictionary);
}

main();
