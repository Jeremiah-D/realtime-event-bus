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
  `market.btc.trades`; a bare `*` or `**` matches every topic). `publish()` fans out to all matching subscribers and returns how
  many accepted the message. Each subscriber gets an independent bounded
  queue; delivery is batched on a microtask so slow consumers exert real
  backpressure instead of blocking publishers.
- **`src/backpressure.ts` — `BoundedQueue<T>`**: fixed-capacity FIFO queue
  with `drop-oldest` / `drop-newest` policies, a drop counter, and a
  high-water-mark callback that fires once at 80% capacity and re-arms after
  the queue recedes.
- **`src/reconnect.ts` — `ReconnectController`**: connection state machine
  (`idle → connecting → connected → backoff → connecting …`) with exponential
  backoff plus jitter, a configurable delay cap, and `onGiveUp` after
  `maxAttempts` failures. The connect function is injectable for testing.

## Run

Requires Node.js ≥ 22 (uses native TypeScript type stripping — no build step,
no dependencies).

```bash
npm test
```

## Test

`npm test` runs `node --test test/`. Coverage includes:

- `test/bus.test.ts` — exact, single-segment (`*`) and multi-level (`**`)
  wildcard delivery, non-matching topics, fan-out counts, unsubscribe, and
  queue-full drop semantics.
- `test/backpressure.test.ts` — both drop policies, one-shot high-water-mark
  behavior, drain ordering.
- `test/reconnect.test.ts` — exponentially increasing backoff delays within
  the jitter window, give-up after `maxAttempts`, attempt-counter reset on
  successful connect, and the disconnect → backoff transition.
