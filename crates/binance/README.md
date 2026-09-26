# clob-binance — Binance USD-M futures feed handler

A correctness-first depth feed handler for Binance USD-M futures (`@depth@100ms`
diff stream + REST snapshot), with a **deterministic replay harness** for testing
and latency work.

Unlike Hyperliquid's `l2Book` (self-contained snapshots, no resync), Binance sends
**incremental deltas** that must be spliced onto a REST snapshot and gap-checked by
sequence number. That makes this crate the home of the two things a real feed
handler is judged on:

1. **Correctness** — never expose a book that has silently diverged from the
   exchange (resync state machine + an invariant enforced at the boundary).
2. **Reproducibility** — record the wire and replay it deterministically, so both
   correctness bugs and latency numbers are testable off a fixed input.

---

## Correctness: the resync state machine

All sequence reasoning lives in [`sequencer.rs`](src/sequencer.rs), a **pure state
machine** that sees only update ids — it knows nothing about price levels, the
`OrderBook`, or IO. It only ever returns a verdict.

```
Connecting ──begin()──▶ Buffering ──splice──▶ Live
                            ▲                   │
                            └────── gap ────────┘   (pu doesn't chain → Invalidate)
```

Canonical Binance USD-M futures algorithm:

- **Buffering**: stash every delta verbatim; don't gap-check yet.
- **Splice** (`on_snapshot(lastUpdateId)`):
  1. drop buffered deltas fully covered by the snapshot (`u < lastUpdateId`);
  2. the first survivor must bridge it: `U <= lastUpdateId + 1 <= u`
     → **Live** (anchor `last_u`); `U > lastUpdateId+1` → **Retry** (gap between
     snapshot and buffer, fetch newer); buffer emptied → **NeedMore** (snapshot
     ahead, keep it and wait for more deltas).
- **Live**: each delta's `pu` must chain onto the last applied `u`. A mismatch is a
  **gap** → drop the book, count a resync, go back to Buffering. Never patch a
  holey book.

### The boundary invariant

The consumer sees a book **only while `Live`**. During Buffering/recovery the venue
emits `VenueBookUpdate::Invalidate` — an explicit "no valid book" — rather than a
half-built one. This is the one rule that makes the feed trustworthy downstream.

### Feed-quality metric

`resync_count` (on the sequencer) increments on every gap; the venue also emits a
`resync_events` counter (`metrics` crate) at the IO boundary. On a long run this is
the single number that answers *"did the book ever diverge?"* — `0` means it didn't.
(The metric lives at the venue layer so the sequencer stays pure and dependency-free.)

---

## Reproducibility: recorder + deterministic replay

The same venue pipeline consumes a live socket or a recorded file, abstracted
behind one trait:

```rust
trait FrameSource {
    async fn connect(&mut self) -> Result<(), VenueError>;
    async fn next(&mut self) -> Result<String, VenueError>;
}
```

- **Recorder** ([`capture.rs`](src/capture.rs)): appends raw WS frames verbatim,
  one per line, prefixed with a monotonic `recv_timestamp_ns` — JSONL, no
  normalization, so replay is an honest input to the parser. Enabled via
  `BINANCE_CAPTURE` / `BINANCE_SNAP` / `BINANCE_CAPTURE_N`.
- **Replay** ([`sources/replay.rs`](src/sources/replay.rs)): reads that file back
  through the identical parse/sequence/apply path. Speed control via `ReplayClock`:
  `--speed max` (no pacing — compute-path benching), `--speed one` (respect
  `recv_ts` — reproduce resync races/timings), `--speed xN` (N× real time).

### A cancellation-safety bug worth noting

The replay reader runs on a background task feeding a **bounded** `mpsc`, and
`FrameSource::next` just `recv`s. This isn't incidental: `read_line` is **not
cancellation-safe**, and it used to sit directly in a `tokio::select!` branch. When
the racing snapshot task won, the in-flight read was dropped mid-line, silently
discarding buffered bytes and corrupting stream alignment — surfacing as
`invalid replay line frame` at a *random* line each run. `mpsc::Receiver::recv`
*is* cancel-safe, so moving the read off the `select!` branch fixed it. Bounded
channel = constant memory (no loading the whole file), pacing preserved via
back-pressure.

---

## Source selection is runtime, not a cargo feature

Live vs replay is chosen at runtime (`--source live|replay`) via static-dispatch
enum wrappers (`Frames` / `Snaps` in [`sources/mod.rs`](src/sources/mod.rs)) — both
paths always compile, no `dyn`. Cargo features were deliberately *not* used: they're
additive booleans, so a "live *or* replay" feature pair breaks under feature
unification (both end up enabled → duplicate definitions). Source choice is a
runtime concern; features are for optional deps / slim builds.

---

## Tests

```bash
cargo test -p clob-binance                 # unit + integration
cargo test -p clob-binance --test determenism -- --nocapture
```

- **Sequencer FSM** ([`sequencer.rs`](src/sequencer.rs), unit): every branch —
  buffering, in-order apply, gap→invalidate (+ `resync_count`), and all three
  splice verdicts (`Live` / `Retry` / `NeedMore`), plus recovery after a stale
  snapshot. Pure numbers, no socket, no JSON.
- **Determinism** ([`tests/determenism.rs`](tests/determenism.rs)): replaying the
  same recording twice yields an identical final book (full-state SHA-256 hash).
- **Reconciliation vs the exchange** (same file): build the book from
  `snapshot@t0 + deltas`, then compare the **top-100** against an independent REST
  snapshot pulled mid-stream (`snapshot@t1`). Because Binance's `lastUpdateId` sits
  *inside* a delta event, the book overshoots it by one event; the reference is
  brought to the same point by applying that same bridge event on top of
  `snapshot@t1`, making the comparison exact. Verified by mutation testing (perturb
  a price in the reference → the test goes red).

---

## Run

```bash
# replay a recording at max speed
cargo run -p clob-binance -- --source replay --speed max

# replay the existing length-delimited SBE capture and print latency buckets
cargo run --release -p clob-binance -- \
  --source replay --format sbe \
  --replay-file crates/binance/replay/output.sbe

# run the same SBE capture through the preallocated price ladder
# (a preparatory scan finds lossless replay bounds outside the timed section)
cargo run --release -p clob-binance -- \
  --source replay --format sbe --book ladder --tick-size 0.1 \
  --min-book-price 50000 --max-book-price 120000 \
  --replay-file crates/binance/replay/output.sbe

# compare JSON using the exact capture from which output.sbe was generated
cargo run --release -p clob-binance -- \
  --source replay --format json \
  --replay-file crates/binance/replay/deltas.jsonl \
  --snapshot-file crates/binance/replay/snapshot.jsonl \
  --speed max

# live feed
cargo run -p clob-binance -- --source live

# record a sample while running live
BINANCE_CAPTURE=deltas.jsonl BINANCE_SNAP=snap.jsonl BINANCE_CAPTURE_N=50000 \
  cargo run -p clob-binance -- --source live
```

---

## Layout

| file | role |
|---|---|
| `sequencer.rs` | pure resync FSM (ids only, no IO/book) — the correctness core |
| `venue.rs` | orchestrates source + sequencer + book; enforces the Live invariant |
| `sources/live.rs` | `BinanceWs` (WS diff stream) + `BinanceRest` (REST snapshot) |
| `sources/replay.rs` | `Replay` / `ReplaySnap` + `ReplayClock` (speed control) |
| `sources/mod.rs` | `Frames` / `Snaps` static-dispatch source selection |
| `capture.rs` | raw-frame recorder (JSONL + `recv_ts_ns`) |
| `model.rs` | wire types + digit-by-digit fixed-point parsing |

## Controlled four-way order-book study

For comparable JSON/SBE × BTreeMap/PriceLadder numbers, use the separate
`orderbook-study` release binary through `scripts/orderbook_study.py` from the
repository root. It supports JSON + ladder, preloads input, uses identical
in-loop price filters, separates normalization and refuses differing final
states. The legacy live/sequenced JSON and SBE replay commands above have
different workloads and are not a valid four-way performance comparison.
See [the full methodology and results](../../BENCHMARKS.md), including the
compressed reproducible fixture and all raw measurements.
