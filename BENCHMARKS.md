# Order Book Performance

This report publishes one measurement set: five runs per variant on the
isolated CPU affinity mask **`3,7`**. See [the artifact guide](benchmarks/README.md)
for the retained input, raw samples, metadata and diagnostics.

This is an offline, preloaded replay study of four combinations: JSON or SBE
parsing followed by `BTreeMap` or `PriceLadder`. It measures local processing,
not network end-to-end latency, exchange latency, or live trading performance.

## Scope and correctness

The study uses 49,999 depth messages with 2,536,222 ordered `BookUpdate`s from
the same BTC recording. One subscription acknowledgement is excluded from both
paths before measurement. The validation pass compares every update, including
side, fixed-point price, quantity, ordering, and updates later filtered out. It
also compares first/final update IDs and event times (JSON milliseconds converted
to SBE microseconds). Invalid JSON, unknown non-depth messages, truncated SBE
framing, unequal counts or updates cause failure rather than silent omission.

All variants start from an **empty book**, apply every recorded delta, and use
an inclusive **$50,000–$120,000** window with **$0.10 ticks**. This is a controlled
delta-processing workload, not reconstruction of the complete exchange book:
the captured snapshot is intentionally unused, and the futures sequencer is
bypassed in all four variants. Consequently the resulting levels need not be a
valid full exchange snapshot even though the implementations agree exactly.

Before timing, validation compares the full ordered final level vectors across
all four combinations, all counters, and the best bid/ask after **each message**.
Each warmup and measured run must reproduce the reference final state hash,
counts and top five levels before its samples can be exported. A mismatch aborts
the study. SHA-256 uses `OrderBook::state_hash`: ascending prices per side,
side tags and fixed-width little-endian price/quantity bytes with separators.
Ladder levels are copied into a canonical tree outside timing to use that same
hash encoding; the separate full-vector equality check does not rely on a hash.

| Check | All four variants |
|---|---:|
| Messages | 49,999 |
| Updates before filtering | 2,536,222 |
| Filtered below $50k | 152,606 |
| Filtered above $120k | 61 |
| Applied updates, including zero-quantity deletes | 2,383,555 |
| Final bid / ask levels | 9,220 / 8,213 |
| Ladder `out_of_range` | 0 |
| Best bid / ask | $79,918.8 / $79,918.9 |

Final state SHA-256:
`9170a0fe956f075d1891f169497d62cc279c48f53fcb08feb5726b6f330e1345`.

| Rank | Bid price | Bid quantity | Ask price | Ask quantity |
|---|---:|---:|---:|---:|
| 1 | 79,918.8 | 6.759 | 79,918.9 | 1.782 |
| 2 | 79,918.7 | 0.088 | 79,919.0 | 0.004 |
| 3 | 79,918.6 | 0.025 | 79,919.1 | 0.001 |
| 4 | 79,918.5 | 0.002 | 79,919.7 | 0.004 |
| 5 | 79,918.3 | 0.066 | 79,919.8 | 0.129 |

## Data structures

`OrderBook` holds two `BTreeMap<Price, Size>` trees. Only occupied levels are
stored. Searches, inserts and deletes traverse ordered nodes; inserts/deletes
can allocate/free nodes and move keys when nodes split, merge or rebalance.
Memory scales with occupied levels and node occupancy. The measured apply path
calls `upsert`; it does not ask the tree for BBO after every update.

`PriceLadder` has one dense quantity array and one occupancy bitmap per side.
Prices and quantities use exact integer fixed-point **scale 10^8**: `$0.1` is
10,000,000 units. Slot index is `(price - 50000 * 10^8) / 10000000`, after
range/alignment checks. The inclusive range gives **700,001 slots per side**.
A quantity of zero deletes a level and clears one bitmap bit. Each bitmap word
represents 64 slots. The ladder caches each side's best index; deleting its best
level searches bitmap words for the next occupied bit. This is word scanning,
not a hierarchical constant-time bitmap. Large gaps can require many word reads.
The ladder therefore maintains BBO during upserts whereas the tree retrieves
BBO on demand: these are the existing implementations, not identical instruction
sequences. No separate BBO read is included in latency.

The common apply loop counts every update, checks lower and upper bounds, then
calls the selected book. **Filtering is timed** and happens after decoding and
normalizing all levels for both formats. No variant receives a prefiltered
corpus. Ladder's own alignment/range checks remain timed too.

## Measurement boundaries

Only release binaries are measured (`opt-level=3`, `debug=true`, `lto="thin"`,
`codegen-units=1`, default target CPU). The binary rejects debug builds. The
runner builds, validates, then executes five independent runs per variant in
rotating order, strictly sequentially. Each process executes one full untimed
warmup, drops that book, then times a fresh empty book. This warms code, input
and allocator state but does **not** promise every newly allocated ladder page
is resident. Book construction is outside latency; first touches during updates
are included. No CPU-intensive verification or builds run alongside the study.

All JSON and SBE bytes are read, framed and validated into memory before the
warmup; file I/O, capture timestamps, length prefixes and raw-buffer allocation
are outside timers. JSON retains its stream envelope. The common normalizer
allocates one `Vec<BookUpdate>` with exact requested capacity and copies bids
then asks. No decoded updates are precomputed for measured runs.

Four `Instant` timestamps per message delimit:

1. **parse/decode**: production JSON deserialization (including decimal-to-integer
   conversion and its two growing level vectors) or the existing SBE decoder
   (including its two exactly sized level vectors).
2. **normalize**: allocate and fill the common `BookUpdate` vector.
3. **apply**: shared counter/filter loop and book mutation.
4. **tick_to_book**: the entire contiguous interval spanning all three stages.

Timer overhead is retained, not subtracted. Temporary-vector destruction,
sample recording and result validation occur after the last timestamp in both
paths. They can affect the next message through allocator/cache state but are
not part of reported tick-to-book latency. This is completion-to-book timing,
not total CPU service cost including cleanup. Per-update latency is each
message's tick-to-book divided by its **original**, unfiltered update count;
percentiles give messages equal weight. It is not a separately timed individual
upsert and does not give updates equal statistical weight.

Percentiles use nearest rank on raw nanosecond samples (no histogram rounding).
Tables report the median of five per-run percentiles, with min–max across runs
reported separately; they do not pool all runs or treat min–max as a confidence
interval. Stage percentile sums need not equal the total percentile. Stage
shares below instead use sums of measured intervals.

No price-bound inference is necessary: endpoints are explicit. Input validation
is an untimed preparatory pass over the data. The old SBE harness's optional
price-bound scan was also untimed; it is not used in this study.

## Why the previous harness was unsuitable for this comparison

The legacy `clob-binance` JSON replay uses the live venue path: snapshot splice,
futures `pu` continuity checks, pending events and an async producer. Its SBE
replay starts empty and bypasses that machinery. Equal input filenames do not
make these paths equal workloads. JSON also rejected `--book ladder`, and its
range options did not implement the SBE filter. SBE's reported `apply` interval
included normalization, so it was not an isolated tree/ladder mutation cost.
The old tables lacked p90, independent run dispersion and a strict state gate.

The new `orderbook-study` binary replaces that harness **for this study**, adds
JSON + ladder via the same replay path as the other combinations, preloads all
frames, separates normalization, checks correctness and exports raw samples.
The legacy/live executable remains compatible; its existing flags and old
measurements must not be mixed with the tables here.

The SBE fixture is **futures level data encoded in a spot SBE-style
DepthDiffStreamEvent layout (template 10003)**. It omits futures `pu` and `T`;
its schema ID/version are converter constants, not proof of compatibility with
an independently captured exchange SBE stream. The existing decoder does not
validate schema ID/version. This is a comparison of these local decoders on
logically equivalent book updates, not equal metadata or a futures SBE service.

## Machine and run record

Measured **2026-09-24 22:13:50–22:15:24 UTC** (00:13–00:15 Sep 25,
Europe/Amsterdam). Intel Core i7-7700, 4 cores / 8 logical CPUs, 3.6 GHz base,
4.2 GHz advertised maximum, 8 MiB shared L3, 15.1 GiB RAM; Linux
7.0.0-31-generic x86-64. Rust **1.98.0 (88d9e12ae 2026-08-18)**, LLVM 22.1.8.

`taskset -c 3,7` succeeded; `/proc/self/status` confirms affinity `3,7` in
every result. Linux reports both logical CPUs as isolated. They are **two SMT
threads of one physical core**, not separate physical cores. The scheduler may
move the single replay thread between them. Variants ran sequentially. The
`powersave` governor, turbo/frequency changes, interrupts and other system
activity were not controlled; isolation does not prove an interference-free run.

[Environment, source/input SHA-256 and exact executed commands](benchmarks/orderbook/environment.json)
are retained alongside [correctness](benchmarks/orderbook/correctness.json),
compressed per-message CSV and per-process JSON. The data directory was renamed
from `benchmarks/orderbook-isolated` to `benchmarks/orderbook` after measurement;
recorded commands retain their original paths for provenance. Build/test console
logs were removed; structured verification results remain. Post-measurement runner changes trim generated Markdown and restrict metadata
to repository-relative source paths and a dirty-worktree flag. Local usernames
and unrelated filenames were removed from the saved environment metadata;
its redaction notes document this change. Timings, input hashes, the Rust harness
and book implementations are unchanged.

## Results

Each row represents **five runs of 49,999 messages** (249,995 measured samples
per variant), after a complete untimed warmup in each process. Values below are
**microseconds** except the explicitly labeled per-update column. “Max” is the
median of five run maxima, not the largest observed event. Raw observations and
min–max ranges for **every stage, quantile and size bucket** are in
[RESULTS.md](benchmarks/orderbook/RESULTS.md).

| Variant | p50 | p90 | p99 | p99.9 | Max (median) | p99 range across runs | p50 ns/update |
|---|---:|---:|---:|---:|---:|---:|---:|
| JSON + BTreeMap | 7.077 | 15.091 | 62.678 | 177.199 | 298.842 | 61.335–64.372 | 189.9 |
| JSON + PriceLadder | 4.991 | 9.441 | 39.016 | 109.721 | 210.527 | 38.370–40.961 | 134.5 |
| SBE + BTreeMap | 2.460 | 5.776 | 29.452 | 87.673 | 144.295 | 29.049–30.257 | 68.5 |
| SBE + PriceLadder | 0.815 | 1.600 | 9.015 | 23.901 | 112.041 | 8.290–12.412 | 21.9 |

The largest observed tick-to-book event is **506.504 µs**, JSON + ladder run 2,
message 41305 (142 updates): 503.876 µs occurs in parse, 0.294 µs in normalize,
and 2.334 µs in apply. Run 1 also has a 299.058 µs event with just 32 updates,
mostly in parse. Both remain in the data. Without a scheduler/hardware trace,
we cannot identify their cause or claim that CPU isolation removes such events.

Observed run-maximum ranges are **283.196–362.100 µs** for JSON/tree,
**196.196–506.504 µs** for JSON/ladder, **140.268–148.960 µs** for SBE/tree,
and **108.918–133.807 µs** for SBE/ladder.

### Stage separation (microseconds)

| Variant | Parse p50 / p99 | Normalize p50 / p99 | Apply p50 / p99 | Sum-based parse / normalize / apply shares |
|---|---:|---:|---:|---:|
| JSON + BTreeMap | 4.479 / 33.194 | 0.067 / 0.599 | 2.407 / 30.008 | 60.5% / 1.1% / 38.4% |
| JSON + PriceLadder | 4.402 / 33.004 | 0.061 / 0.556 | 0.483 / 7.834 | 87.1% / 1.4% / 11.4% |
| SBE + BTreeMap | 0.296 / 2.502 | 0.058 / 0.430 | 2.118 / 26.912 | 10.9% / 2.4% / 86.7% |
| SBE + PriceLadder | 0.276 / 2.308 | 0.055 / 0.377 | 0.477 / 6.068 | 32.4% / 6.3% / 61.4% |

Shares are medians of each run's summed stage time divided by its summed total;
rounding and separate medians can prevent an exact sum of 100%. Parse times
are not assumed identical across book types: allocations, cache state and system
noise couple adjacent stages. Percentiles do not identify causal contributions.

### Dependence on message size

Median per-run tick-to-book p99, **microseconds**. Counts include all original
updates before filtering, including zero-quantity deletes.

| Updates/message | Messages/run | JSON tree | JSON ladder | SBE tree | SBE ladder |
|---|---:|---:|---:|---:|---:|
| 1–50 | 38,909 | 21.396 | 18.488 | 5.573 | 1.539 |
| 51–200 | 10,023 | 35.381 | 25.628 | 19.975 | 4.074 |
| 201–500 | 693 | 93.585 | 60.500 | 47.073 | 11.663 |
| 501–1000 | 299 | 176.839 | 117.217 | 97.393 | 28.260 |
| 1001+ | 75 | 298.842 | 196.196 | 144.295 | 54.851 |

The 1,067 messages with >200 updates are only **2.13%** of the corpus, but
account for **100%** of the slowest 1% for JSON/tree, **96.2–99.0%** for
JSON/ladder, **98.0–99.8%** for SBE/tree, and **59.0–95.8%** for SBE/ladder.
All 75 messages with 1001+ updates appear in the slowest 1% in every run.
SBE/ladder run 5 has only 59% large messages in its slowest 1%, so size alone
does not explain every tail; the raw samples preserve this variability.
This directly demonstrates the effect of message-size mixture on aggregate p99.
The 1001+ bucket has just 75 observations/run: nearest-rank p99 and p99.9 are
both the maximum. These are descriptive statistics, not stable rare-event
estimates. Full-corpus p99.9 has roughly 50 observations beyond its threshold.

## Memory and allocation evidence

Both representations finish with **17,433 occupied levels**. Ladder reserves
700,001 price slots **per side** (1,400,002 quantities in total), so only about
1.25% of its quantity slots end occupied.

| Representation | Quantity arrays | Bitmap arrays | Requested book heap | Occupied levels |
|---|---:|---:|---:|---:|
| BTreeMap | n/a | n/a | 500,832 B (0.478 MiB) | 17,433 |
| PriceLadder | 11,200,016 B | 175,008 B | 11,375,024 B (10.848 MiB) | 17,433 |

The bitmap has 10,938 u64 words/side. Ladder heap size is the exact logical size
of its four arrays; it excludes the small inline struct, allocator metadata and
page rounding. Tree memory is measured with a separate counting-allocator build:
net requested bytes from book construction and apply, excluding parser vectors,
normalization, samples and final-state copies. It is an **approximation of actual
allocator footprint**, not a portable Rust node-layout guarantee or an RSS value.
The same tree byte count was observed with both wire formats. Key/value payload
alone is 278,928 B; nodes, links and unused node slots account for additional
requested space. Ladder's logical book heap is approximately 22.7 times larger.

| Variant | Parse allocation/reallocation calls | Normalize calls | Apply allocation/reallocation calls | Process RSS after replay, MiB (median; range) |
|---|---:|---:|---:|---:|
| JSON + BTreeMap | 369,549 | 49,999 | 15,239 | 113.66; 113.64–113.70 |
| JSON + PriceLadder | 369,549 | 49,999 | 0 | 116.62; 116.60–116.62 |
| SBE + BTreeMap | 99,998 | 49,999 | 15,239 | 113.67; 113.66–113.70 |
| SBE + PriceLadder | 99,998 | 49,999 | 0 | 116.60; 116.56–116.62 |

These allocation counters are **compiled out** of the normal timing binary.
They count successful `alloc`, `alloc_zeroed` and `realloc` calls, not bytes moved
or time spent inside the allocator. JSON's growing vectors require more calls
than the SBE decoder's two exact-capacity vectors. Normalization allocates once
per message in every variant. Tree apply makes 15,239 allocation/reallocation
calls; ladder apply makes none. The independent structural replay counts
244,091 insertions of previously empty levels and 226,658 deletions. It does not
count Rust BTreeMap node splits/merges, and not every level insertion allocates.

RSS is **whole-process resident memory**, including both preloaded corpora,
recorded samples, code and allocator caches. It cannot be attributed entirely
to the book. `/proc/self/status` snapshots before construction and after replay
also retain VmSize and VmHWM; HWM includes the larger temporary buffers used
while loading. The warmup can leave allocator pages resident. Consequently an
RSS delta is not an isolated book allocation measurement.

A zero-filled allocation can reserve virtual address space backed initially by
shared zero pages; only writes need private physical pages. The untimed structural
model touches **664 logical 4 KiB quantity-page offsets** (~2.59 MiB), including
zero-quantity writes, out of the ~10.68 MiB quantity capacity. Actual addresses
have alignment/allocator/page effects, so this is not a resident-page census.
The full arrays need not contribute their logical size to RSS until touched.
Calling `clear_side` explicitly writes the **entire** quantity array and bitmap;
that increases touched memory even when the occupied-level count is small.

## Tail investigation: measured evidence versus hypotheses

* **Message size** is the dominant aggregate p99 driver, as the bucket and
  slowest-1% counts above demonstrate. JSON parsing dominates total time for
  JSON + ladder; tree mutation dominates SBE + tree. Removing a parse bottleneck
  exposes apply, normalization and first-touch costs rather than eliminating them.
* **Vec allocation** is present in both decoders and normalization. JSON's
  additional growth calls are directly counted. Their exact contribution to a
  particular percentile is not identified by aggregate counts; a reuse-arena
  experiment would be required to isolate it.
* **Tree mutations and locality:** there are real allocator calls and substantial
  insertion/deletion churn. Node traversal and key movement are plausible sources
  of extra work compared with indexed arrays. Pointer chasing, cache misses and
  branch misses were **not measured by hardware counters in this study**; they
  must not be presented as proven percentages of p99 or as the cause of a
  specific outlier. An 8 MiB L3 also means the wide ladder is not automatically
  “all in cache”; its actual touched working set matters more than capacity.
* **Bitmap clearing:** each deletion clears one bit. No `clear_side` or full
  bitmap reset occurs in the measured replay. The structural model records
  6,323 best-level deletes and only 6,534 bitmap-word reads during best searches
  across the entire corpus. Long empty-word scans are therefore not the dominant
  explanation for this recording. This does not bound the worst case on another
  price distribution.
* **Page faults explain a repeatable ladder tail.** Message **40099** (one-based,
  303 updates) has zero best deletions/bitmap-search reads but reaches **65 new
  logical quantity pages**. Its apply time is 112.7–138.9 µs in the five JSON
  ladder runs and 107.1–131.8 µs in the five SBE ladder runs. A separate instrumented
  replay observed **65 minor faults** around that apply. Writing zeros through
  both sides before replay preserves the final full state and removes these
  faults: instrumented apply falls from **141.113 to 9.608 µs** in that diagnostic.
  These diagnostic times include a different cache/instrumentation context and
  are **not** substituted into the main table. First-touch page preparation
  changes the workload and memory residency, so it must be reported explicitly.
* Main-loop process counters record 978 minor faults for tree, 1,634–1,651 for JSON
  ladder and 1,632 for SBE ladder, with **zero major faults** in every run.
  These counters cover the loop **including sample recording**, not only timer
  intervals; the roughly 4 MB sample vector explains much of the common fault
  baseline. The separate apply-bracketed diagnostic observes 674 minor faults
  for a fresh ladder versus zero after page preparation, providing more specific
  evidence. Reading `/proc/self/stat` is outside its apply timer but perturbs
  caches; this is a diagnostic intervention, not a sixth performance run.
* **Unexpected/worse individual observations remain visible.** Ladder has rare
  first-touch events on small/medium messages. Its median run-maximum ns/update
  is 2,357.4 versus 1,452.1 for JSON/tree; for SBE it is 1,134.1 versus 1,128.5
  for tree. The general speed difference does not establish dominance for every
  message. The parse outliers above are retained without an invented causal
  explanation, and the full five-run spread is published.

[Structural counters](benchmarks/orderbook/structural-diagnostics.json) reproduce
the full reference state hash. [Per-message fault diagnostics](benchmarks/orderbook/page-diagnostics.json)
record both empty-book policies and verify complete final state equality.

## Reproduction

Run from the repository root. Python scripts use only the standard library.
The compressed public JSON fixture is included; it is a recorded public market
data stream, not synthetic random updates. The existing converter recreates the
measured SBE file **byte for byte**, verified by SHA-256:
`4f87671a85289e9bfc704a17e9e935ff6be3ed3ea5e6afc6e441ecee0ca4e71e`.
Snapshot data is not needed. Restore only missing files to preserve local captures:

```bash
mkdir -p crates/binance/replay
[ -f crates/binance/replay/deltas.jsonl ] || gzip -dc benchmarks/orderbook/fixtures/deltas.jsonl.gz > crates/binance/replay/deltas.jsonl
[ -f crates/binance/replay/output.sbe ] || python3 scripts/json_to_sbe.py crates/binance/replay/deltas.jsonl crates/binance/replay/output.sbe

# Use the recorded isolated affinity mask when available on the host.
taskset -pc $$
taskset -c 3,7 python3 scripts/orderbook_study.py --cpu 3,7 --out benchmarks/orderbook-local

# Regenerate published tables from retained raw measurements, without rerunning.
python3 scripts/orderbook_study.py --out benchmarks/orderbook --summarize-only

# Independent structural and OS page-fault investigations.
taskset -c 3,7 python3 scripts/orderbook_tail_diagnostics.py --out benchmarks/orderbook-local
cargo build --release -p clob-binance --bin orderbook-pages
taskset -c 3,7 target/release/orderbook-pages > benchmarks/orderbook-local/page-diagnostics.json

taskset -c 3,7 cargo fmt --check
taskset -c 3,7 cargo test --workspace
taskset -c 3,7 cargo check --workspace
git diff --check
```

For one manually measured combination after validation:

```bash
cargo build --release -p clob-binance --bin orderbook-study
taskset -c 3,7 target/release/orderbook-study --validate --reference /tmp/book-correctness.json
taskset -c 3,7 target/release/orderbook-study --format json --book ladder --reference /tmp/book-correctness.json --output /tmp/json-ladder.csv
```

Replace `json` with `sbe` or `ladder` with `btree` to select the other paths.
The runner refuses to overwrite an existing measurement directory containing
`environment.json`. It compiles the separate `study-alloc` feature for diagnostic
runs and then restores the uninstrumented release binary. Never compare latency
from an allocator-instrumented build. The original `cargo run --release -p
clob-binance` still selects the live executable through `default-run`.

## Limits and next experiment

One recording, one machine, one fixed range and five runs do not establish
production latency guarantees. The empty starting book, lack of sequencer and
snapshot work, preloaded corpora, warm caches, burst-free closed-loop input and
excluded vector cleanup differ from a live feed. Offered-load queueing,
coordinated omission, socket/TLS/WebSocket costs, reconnects and snapshot resets
are outside scope. Spot-layout SBE omits futures metadata; JSON parses more
metadata and decimal strings. The price window removes ~6.02% of updates and
excludes book levels outside it. Narrower/wider windows or a different market
regime could change both tail latency and memory use.

Timer cost is substantial relative to the smallest SBE stages. Independent
round ordering reduces but does not eliminate thermal/frequency/scheduler bias;
The allowed CPUs are isolated SMT siblings, but migration within the mask,
interrupts and other host activity are not eliminated. The highest percentiles of sparse
buckets are maxima rather than strong estimates. Public raw samples preserve
these limitations instead of hiding anomalous runs.

The next useful experiment is a controlled **default versus explicitly
prefaulted ladder** comparison, including construction/reset cost and RSS, on
one pinned logical CPU with repeated fresh recordings. Add region-scoped `perf` counters
and scheduling traces to test cache-miss, tree-node and interruption hypotheses;
then validate the preferred implementation behind the full live snapshot/
sequencer path at a specified offered message rate before making live claims.

Working-tree verification passed under `taskset -c 3,7`: `cargo fmt --check`,
`cargo test --workspace` (35 tests), and `cargo check --workspace`; diff checks
also passed. [Verification scope](benchmarks/orderbook/verification.json) records
that pre-existing API/supervisor changes were deliberately excluded from staging.
The local supervisor entry point is needed to build that binary, so this is not
a staged-only workspace test claim. Existing compiler warnings remain.
