# Order-book measurement artifacts

Start with [the study report](../BENCHMARKS.md). These files retain the evidence
behind the published percentiles; they are data, not additional benchmark code.

[`orderbook/`](orderbook/) contains the single published measurement set:
CPU affinity `3,7`, four combinations, five measured runs each, one full untimed
warmup per process. Every table in the order-book report uses this dataset.

[`orderbook/fixtures/deltas.jsonl.gz`](orderbook/fixtures/deltas.jsonl.gz) is the
shared compressed input (~10.5 MiB). The converter recreates the measured SBE
input, so a second binary fixture is unnecessary.

## File types in each run directory

| Files | Purpose | Why retain them? |
|---|---|---|
| `json-btree-1.csv.gz`, etc. | One compressed CSV per variant/run; 20 files. Rows contain update count and parse/normalize/apply/total nanoseconds. | Recompute every percentile and inspect individual outliers without rerunning. |
| Matching `*-1.json` … `*-5.json` | Correctness counters/hash, process RSS/virtual-memory snapshots, page faults and actual affinity. | Verify that each measured run used the same workload and state. |
| `summary.json` | Machine-readable aggregate percentiles, spread and tail composition. | Reuse results in tables or analysis. |
| `RESULTS.md` | Generated human-readable tables for all stages and message-size buckets. | Browse the complete results. |
| `correctness.json` | Reference counts, complete-state hash, top levels and bucket sizes. | Gate timing on implementation equivalence. |
| `environment.json` | Machine/compiler/profile metadata, source/input fingerprints and commands. | Identify exactly what was measured and how. |
| `*-alloc.json` | Four separate allocator-instrumented diagnostic runs. | Explain allocation counts and approximate tree memory; excluded from latency tables. |
| `structural-diagnostics.*` | Untimed bitmap/page-offset/occupancy analysis. | Investigate repeated outliers and validate the model's final state hash. |
| `page-diagnostics.json` | Separate default/prefaulted ladder experiment. | Check the page-fault explanation without replacing baseline measurements. |
| `diagnostic-provenance.json`, `integrity.txt` | Diagnostic commands/source hashes and artifact checks. | Audit the supplementary experiments and saved data. |

Console `*.log` files have been removed and are ignored on future runs. They duplicate structured JSON or
contain build/test console output; they are not needed to regenerate the tables.
The raw samples and fixture account for most of the stored bytes, not the JSON
metadata. Removing them would make the published numbers harder to audit.

Rebuild tables from retained measurements:

```bash
python3 scripts/orderbook_study.py --out benchmarks/orderbook --summarize-only
```

Run a new study without overwriting the saved result set:

```bash
taskset -c 3,7 python3 scripts/orderbook_study.py --cpu 3,7 --out benchmarks/orderbook-local
```

CPU 3 and CPU 7 are two SMT threads of one physical core on this host. The mask
allows the scheduler to move a thread between them; it does not pin execution
to one logical CPU or make the single-threaded replay use two cores in parallel.

The dataset was originally written to `benchmarks/orderbook-isolated` and later
renamed to `benchmarks/orderbook`. `environment.json` and diagnostic provenance
retain the original executed paths; reproduction commands above use the current
layout. No timings or raw samples were changed during this cleanup.
