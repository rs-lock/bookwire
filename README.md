# Bookwire

**An experimental Binance market-data feed handler and reproducible order-book
performance study in Rust.**

Bookwire investigates the latency and memory costs of decoding market-data
messages, normalizing price-level updates, and maintaining an L2 order book.
It includes a Binance USD-M futures JSON connector and an offline experiment
comparing JSON/SBE decoding with `BTreeMap`/`PriceLadder` storage. The repository
contains source code, a recorded input fixture, raw measurements, correctness
checks, and scripts for reproducing the analysis.

**Binance is the only implemented exchange connector.** The project is a
research prototype. Shared-memory publication, UDP export, persistent market-data
storage, and cross-exchange aggregation are not implemented.

[Study report](BENCHMARKS.md) · [Complete results](benchmarks/orderbook/RESULTS.md) ·
[Artifact guide](benchmarks/README.md)

## Research questions

1. How do wire decoding and book storage contribute to local processing latency?
2. What latency–memory trade-off does a dense price ladder offer compared with
   an ordered tree on the same sequence of updates?
3. How much do message size, allocation behavior, and first-touch page faults
   explain the observed latency tails?

The experiment evaluates these questions on one BTC recording and one machine.
It does not measure exchange-to-consumer network latency or establish live
performance guarantees.

## Implemented scope

| Component | Current implementation |
|---|---|
| Exchange input | Binance USD-M futures: WebSocket depth messages and REST snapshots; the live executable selects BTCUSDT. |
| Live book maintenance | JSON parsing, snapshot/delta sequencing, normalized updates and an in-process `BTreeMap` book. |
| Book representations | `OrderBook` with two `BTreeMap`s; `PriceLadder` with dense quantity arrays, occupancy bitmaps and cached best prices. |
| Replay experiment | All four JSON/SBE × tree/ladder combinations through a separate synchronous, preloaded harness. |
| Validation | Ordered update equality across formats, full final-level equality, state hashes, filter counters and BBO checks. |
| Diagnostics | Separate allocator instrumentation, bitmap/page-offset analysis and a first-touch page-fault experiment. |

The SBE input is generated from the JSON recording using a **spot-style SBE
layout populated with futures level data**. It omits futures `pu` and transaction
time `T`. It is not a captured live SBE feed, and no live SBE connector is claimed.

Crates named `clob-hyperliquid`, `clob-lighter` and `clob-aggregator` are empty
scaffolds. The `clob-api` and `clob-supervisor` crates are outside the implemented
feed/replay pipeline. Their presence does not represent additional connectors,
a serving API or a working supervisor.

## Experimental design

The measured path is:

```text
Preloaded JSON or SBE bytes
    → parse/decode
    → normalize into BookUpdate values
    → inclusive price filter + book mutation
    → updated in-process book
```

The live connector additionally uses snapshot and sequence handling. Those steps
are deliberately excluded from all four experimental variants.

- **Input:** 49,999 depth messages containing 2,536,222 ordered updates. One
  subscription acknowledgement is excluded before measurement.
- **Initial state:** an empty book for every variant and run. The experiment
  compares delta processing; it does not reconstruct a complete exchange snapshot.
- **Price window:** $50,000–$120,000 inclusive, with a $0.10 tick. Both bounds are
  checked inside the measured apply loop, after all updates are normalized.
- **Representation:** prices and quantities use integer fixed-point scale
  `10^8`. Ladder index is `(price - min_price) / tick_size`; each side has
  700,001 slots and one bitmap bit per slot.
- **Timing:** release builds only; file I/O and raw-frame allocation occur before
  timing. Parse/decode, normalize and apply have separate timestamps.
  `tick_to_book` spans those three stages. Temporary-vector cleanup is excluded.
- **Repetition:** five measured runs per variant, each after a full untimed warmup;
  variants execute sequentially in rotating order.
- **Statistics:** median of five per-run nearest-rank percentiles. Reported ranges
  are minimum–maximum across runs, not confidence intervals.

Validation must succeed before benchmarking. Every measured run must reproduce
the reference counters and final state hash before exporting its samples.
All four variants finish with 9,220 bid levels and 8,213 ask levels, filter
152,606 updates below the window and 61 above it, and report zero ladder
`out_of_range` updates. The full state hash and top levels are in
[correctness.json](benchmarks/orderbook/correctness.json).

## Order Book Performance

Measured **2026-09-24 UTC** on an Intel Core i7-7700, Linux x86-64, Rust 1.98.0.
Processes used `taskset -c 3,7`; Linux reports these logical CPUs as isolated.
They are **SMT siblings of one physical core**. The mask permits migration
between them and does not eliminate interrupts or frequency variation.

Times below are **microseconds**. Book heap is the requested allocation size;
it is not process RSS. All rows use the same input and correctness checks.

| Variant | tick-to-book p50 | p99 | p99 range across runs | Requested book heap |
|---|---:|---:|---:|---:|
| JSON + BTreeMap | 7.077 | 62.678 | 61.335–64.372 | ~0.478 MiB |
| JSON + PriceLadder | 4.991 | 39.016 | 38.370–40.961 | 10.848 MiB |
| SBE + BTreeMap | 2.460 | 29.452 | 29.049–30.257 | ~0.478 MiB |
| SBE + PriceLadder | 0.815 | 9.015 | 8.290–12.412 | 10.848 MiB |

On this workload, the ladder lowers median and p99 processing latency while
requesting approximately 22.7 times more book heap. Large messages contribute
substantially to aggregate p99. A separate diagnostic identifies 65 minor page
faults in one repeatable ladder outlier; preparing pages before replay removes
those faults. This does not explain every tail: the largest recorded total is
506.504 µs, mostly in JSON parsing, with no established causal attribution.

[The full report](BENCHMARKS.md) includes p50/p90/p99/p99.9/max for every stage,
message-size buckets, per-update ratios, run dispersion, allocation counts, RSS,
and the distinction between allocated memory and resident pages.

## Reproduce the study

Run commands from the repository root. The recorded environment uses Rust/Cargo
1.98.0, Python 3 with only standard-library modules, Linux `/proc`, `gzip`, and
`taskset` from util-linux. The declared older Rust version has not been verified
as a minimum supported toolchain for this study. See
[environment.json](benchmarks/orderbook/environment.json) for exact versions,
build settings, source/input fingerprints and executed commands.

Restore missing replay inputs without overwriting local captures:

```bash
mkdir -p crates/binance/replay
[ -f crates/binance/replay/deltas.jsonl ] || gzip -dc benchmarks/orderbook/fixtures/deltas.jsonl.gz > crates/binance/replay/deltas.jsonl
[ -f crates/binance/replay/output.sbe ] || python3 scripts/json_to_sbe.py crates/binance/replay/deltas.jsonl crates/binance/replay/output.sbe
```

Rebuild the published tables from saved samples, without rerunning measurements:

```bash
python3 scripts/orderbook_study.py --out benchmarks/orderbook --summarize-only
```

Run a new experiment in a fresh output directory:

```bash
taskset -c 3,7 python3 scripts/orderbook_study.py --cpu 3,7 --out benchmarks/orderbook-local
```

The runner builds release binaries, validates equivalence, performs all 20 timed
runs, writes raw samples and tables, and separately measures allocations. It
refuses to overwrite an existing study directory. CPU numbers are host-specific;
choose available CPUs on another machine and record the changed conditions.
Numerical results will vary with hardware and system state.

## Run the Binance live connector

```bash
cargo run --release --locked -p clob-binance -- --source live
```

This connects to Binance USD-M futures for BTCUSDT and maintains an in-process
tree book. It requires access to the configured Binance REST/WebSocket endpoints.
The live command does not run the controlled replay study or publish a book to
shared memory, UDP, or a database.

## Validate the research components

```bash
cargo fmt --check
cargo test --locked -p clob-core -p clob-venue -p clob-binance
cargo check --locked -p clob-core -p clob-venue -p clob-binance
```

Tests cover book operations, ladder/tree agreement, SBE decoding, sequencing,
price-filter boundaries and replay determinism. The full-recording equivalence
check is an additional gate in the study runner. The scope of the original
workspace checks is recorded in
[verification.json](benchmarks/orderbook/verification.json).

## Repository organization

| Location | Contents |
|---|---|
| [`crates/core`](crates/core) | Integer price/quantity types, tree book and price ladder. |
| [`crates/venue`](crates/venue) | Venue interfaces and normalized book updates. |
| [`crates/binance`](crates/binance) | The implemented connector, decoders, sequencer, replay harness and diagnostics. |
| [`scripts/orderbook_study.py`](scripts/orderbook_study.py) | Sequential experiment runner and percentile aggregation. |
| [`scripts/orderbook_tail_diagnostics.py`](scripts/orderbook_tail_diagnostics.py) | Untimed structural analysis of the recorded updates. |
| [`scripts/json_to_sbe.py`](scripts/json_to_sbe.py) | Reproducible fixture conversion. |
| [`benchmarks/orderbook`](benchmarks/orderbook) | Input fixture, 20 compressed raw runs, metadata, results and diagnostics. |
| [`BENCHMARKS.md`](BENCHMARKS.md) | Detailed methodology, results and interpretation. |

## Limitations

This is a single-recording, single-machine study with a fixed price window.
Replay bypasses network transport, live sequencing and snapshot recovery; it
also excludes queueing under externally imposed load. JSON and SBE carry
unequal metadata even though their ordered book updates are identical. Small
size buckets contain too few observations for stable extreme-percentile
estimates. CPU isolation alone does not establish the cause of every outlier.

The results support conclusions about these implementations under the stated
conditions. They do not establish production readiness, exchange-to-consumer
latency, or performance on other markets and machines.

## Referencing the results

When referring to this work, identify Bookwire, the repository revision, the
recorded experiment date, and the input hashes in `environment.json`. Distinguish
recomputed tables from new measurements, and include the machine and affinity
settings for any new run. This repository presents an empirical software study;
it does not claim institutional affiliation or peer review.
