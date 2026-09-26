#!/usr/bin/env python3
"""Sequential release replay study, raw samples + reproducible percentile tables.
Python standard library only. Run from repository root.
"""
import argparse
import csv
import datetime
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import shutil
import statistics as stats
import subprocess

VARIANTS = [(f, b) for f in ('json', 'sbe') for b in ('btree', 'ladder')]
METRICS = ['parse_ns', 'normalize_ns', 'apply_ns', 'tick_to_book_ns', 'ns_per_update']
BUCKETS = ['all', '1–50', '51–200', '201–500', '501–1000', '1001+']
QUANTILES = [('p50', .5), ('p90', .9), ('p99', .99), ('p99.9', .999), ('max', 1)]

def run(cmd, **kwargs):
    print('+', ' '.join(map(str, cmd)), flush=True)
    return subprocess.run(list(map(str, cmd)), check=True, **kwargs)

def capture(cmd):
    p = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return p.stdout

def digest(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()

def quantile(values, q):
    return sorted(values)[max(0, math.ceil(len(values)*q)-1)]

def load(p):
    with gzip.open(p, 'rt') as f:
        rows = [{k:int(v) for k,v in r.items()} for r in csv.DictReader(f)]
    for r in rows:
        r['ns_per_update'] = r['tick_to_book_ns']/r['updates']
    return rows

def bucket(r):
    n = r['updates']
    return 1 if n <= 50 else 2 if n <= 200 else 3 if n <= 500 else 4 if n <= 1000 else 5

def summarize(out):
    summary = {}
    lines = ['# Raw replay results', '', 'Nanoseconds; median of five per-run nearest-rank percentiles. Brackets show minimum–maximum across runs, not confidence intervals. Per-update values are message latency / original update count, with equal message weight. Diagnostic allocator/perf runs are excluded.', '']
    for fmt, book in VARIANTS:
        key = f'{fmt}-{book}'
        runs = [load(out/f'{key}-{i}.csv.gz') for i in range(1, 6)]
        tables = {}
        lines += [f'## {key}', '', '| Bucket (messages/run) | Stage | p50 | p90 | p99 | p99.9 | max |', '|---|---|---:|---:|---:|---:|---:|']
        for bi, label in enumerate(BUCKETS):
            rr = [[r for r in rows if bi == 0 or bucket(r) == bi] for rows in runs]
            if not rr[0]:
                continue
            tables[label] = {}
            for m in METRICS:
                result = {}
                for name, q in QUANTILES:
                    vals = [quantile([r[m] for r in rows], q) for rows in rr]
                    result[name] = {'median':stats.median(vals), 'min':min(vals), 'max':max(vals)}
                tables[label][m] = result
                cells = [f'{x["median"]:,.1f} [{x["min"]:,.1f}–{x["max"]:,.1f}]' for x in result.values()]
                lines.append(f'| {label} ({len(rr[0])}) | {m} | ' + ' | '.join(cells) + ' |')
        tails = []
        for rows in runs:
            threshold = quantile([r['tick_to_book_ns'] for r in rows], .99)
            tail = [r for r in rows if r['tick_to_book_ns'] >= threshold]
            total = sum(r['tick_to_book_ns'] for r in rows)
            tails.append({'threshold_ns':threshold,'count':len(tail),'bucket_counts':[sum(bucket(r)==b for r in tail) for b in range(1,6)],'stage_shares':{m:sum(r[m] for r in rows)/total for m in METRICS[:3]},'tail_stage_shares':{m:sum(r[m] for r in tail)/sum(r['tick_to_book_ns'] for r in tail) for m in METRICS[:3]}})
        summary[key] = {'tables':tables,'tails':tails}
        lines += ['', 'Overall top 1% message counts by size bucket (one row per run):', '', '```json', json.dumps(tails, indent=2), '```', '']
    (out/'summary.json').write_text(json.dumps(summary, indent=2)+'\n')
    (out/'RESULTS.md').write_text('\n'.join(lines).rstrip()+'\n')
    return summary

def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--out', type=Path, default=Path('benchmarks/orderbook'))
    ap.add_argument('--cpu', default=str(min(os.sched_getaffinity(0))), help='CPU or comma-separated CPU list, e.g. 3,7')
    ap.add_argument('--summarize-only', action='store_true')
    args = ap.parse_args(); out = args.out
    if args.summarize_only:
        summarize(out); return
    out.mkdir(parents=True, exist_ok=True)
    if (out/'environment.json').exists():
        raise SystemExit('Choose a fresh --out directory to preserve prior measurements')
    cpus = [int(cpu) for cpu in args.cpu.split(',')]
    assert cpus and set(cpus).issubset(os.sched_getaffinity(0)), 'Run under the requested taskset affinity first'
    prefix = ['taskset', '-c', str(args.cpu)] if shutil.which('taskset') else []
    files = list(Path('crates/binance/replay').glob('*')) + list(Path('crates').rglob('*.rs')) + [Path('Cargo.toml'),Path('Cargo.lock'),Path('crates/binance/Cargo.toml'),Path(__file__).resolve().relative_to(Path.cwd().resolve())]
    env = {'date_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(), 'platform':platform.platform(), 'rustc':capture(['rustc','-Vv']), 'cargo':capture(['cargo','-V']), 'lscpu':capture(['lscpu']), 'meminfo':Path('/proc/meminfo').read_text(), 'governor':Path(f'/sys/devices/system/cpu/cpu{cpus[0]}/cpufreq/scaling_governor').read_text(), 'affinity_requested':args.cpu,'isolated_cpus':Path('/sys/devices/system/cpu/isolated').read_text().strip(),'thread_siblings':{str(cpu):Path(f'/sys/devices/system/cpu/cpu{cpu}/topology/thread_siblings_list').read_text().strip() for cpu in cpus},'taskset_used':bool(prefix),'source_and_fixture_sha256':{str(p):digest(p) for p in files},'git_head':capture(['git','rev-parse','HEAD']).strip(),'git_dirty':bool(capture(['git','status','--porcelain','--untracked-files=no']).strip()),'profile':'release: opt-level=3 (default), debug=true, lto=thin, codegen-units=1; default target CPU; no RUSTFLAGS override', 'RUSTFLAGS':os.environ.get('RUSTFLAGS'),'commands':[]}
    def execute(cmd, log):
        env['commands'].append(list(map(str,cmd)))
        (out/'environment.json').write_text(json.dumps(env, indent=2)+'\n')
        with open(out/log,'w') as f:
            run(cmd, stdout=f, stderr=subprocess.STDOUT)
    execute(['cargo','build','--release','-p','clob-binance','--bin','orderbook-study'], 'build.log')
    binary = Path('target/release/orderbook-study')
    common = ['--reference',out/'correctness.json']
    execute(prefix+[binary,'--validate']+common, 'validation.log')
    # Rotate variant order across rounds to reduce monotonic thermal/order bias.
    # Every invocation also executes one full untimed warmup internally.
    for i in range(1,6):
        order = VARIANTS[(i-1)%4:] + VARIANTS[:(i-1)%4]
        for fmt,book in order:
            stem = f'{fmt}-{book}-{i}'
            p = out/(stem+'.csv')
            execute(prefix+[binary,'--format',fmt,'--book',book,'--output',p]+common, stem+'.log')
            with open(p,'rb') as src, gzip.open(str(p)+'.gz','wb') as dst:
                shutil.copyfileobj(src,dst)
            p.unlink()
    summarize(out)
    # Separate instrumentation build, never incorporated into latency tables.
    execute(['cargo','build','--release','-p','clob-binance','--bin','orderbook-study','--features','study-alloc'],'build-alloc.log')
    for fmt,book in VARIANTS:
        stem = f'{fmt}-{book}-alloc'
        execute(prefix+[binary,'--format',fmt,'--book',book,'--output',out/(stem+'.csv')]+common,stem+'.log')
        (out/(stem+'.csv')).unlink()
    # Restore the uninstrumented binary for manual reproduction.
    execute(['cargo','build','--release','-p','clob-binance','--bin','orderbook-study'],'build-restore.log')
    env['finished_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    (out/'environment.json').write_text(json.dumps(env,indent=2)+'\n')

if __name__ == '__main__':
    main()
