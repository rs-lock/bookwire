//! Controlled, preloaded replay study. Never used by the live feed.
use std::{fs, hint::black_box, io::Write, path::PathBuf, time::Instant};

use clap::Parser;
use clob_binance::{
    model::{Frame, Price as WirePrice, Size as WireSize},
    sbe,
};
use clob_core::{
    OrderBook, Price, SCALE, Side, Size,
    price_ladder::{LadderUpdate, PriceLadder},
};
use clob_venue::BookUpdate;
use serde_json::{Value, json};

const MIN: i64 = 50_000 * SCALE;
const MAX: i64 = 120_000 * SCALE;
const TICK: i64 = SCALE / 10;
type Levels = Vec<(Price, Size)>;
type State = (Levels, Levels);
type WireLevels = Vec<(WirePrice, WireSize)>;

#[cfg(feature = "study-alloc")]
mod alloc {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed};
    pub static CALLS: AtomicU64 = AtomicU64::new(0);
    pub static BYTES: AtomicI64 = AtomicI64::new(0);
    pub struct Counting;
    unsafe impl GlobalAlloc for Counting {
        unsafe fn alloc(&self, l: Layout) -> *mut u8 {
            let p = unsafe { System.alloc(l) };
            if !p.is_null() {
                CALLS.fetch_add(1, Relaxed);
                BYTES.fetch_add(l.size() as i64, Relaxed);
            }
            p
        }
        unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
            let p = unsafe { System.alloc_zeroed(l) };
            if !p.is_null() {
                CALLS.fetch_add(1, Relaxed);
                BYTES.fetch_add(l.size() as i64, Relaxed);
            }
            p
        }
        unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
            BYTES.fetch_sub(l.size() as i64, Relaxed);
            unsafe { System.dealloc(p, l) }
        }
        unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
            let q = unsafe { System.realloc(p, l, n) };
            if !q.is_null() {
                CALLS.fetch_add(1, Relaxed);
                BYTES.fetch_add(n as i64 - l.size() as i64, Relaxed);
            }
            q
        }
    }
    pub fn snapshot() -> (u64, i64) {
        (CALLS.load(Relaxed), BYTES.load(Relaxed))
    }
}
#[cfg(feature = "study-alloc")]
#[global_allocator]
static ALLOC: alloc::Counting = alloc::Counting;
fn allocations() -> (u64, i64) {
    #[cfg(feature = "study-alloc")]
    {
        alloc::snapshot()
    }
    #[cfg(not(feature = "study-alloc"))]
    {
        (0, 0)
    }
}

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "crates/binance/replay")]
    input: PathBuf,
    #[arg(long)]
    validate: bool,
    #[arg(long, default_value = "json", value_parser = ["json", "sbe"])]
    format: String,
    #[arg(long, default_value = "btree", value_parser = ["btree", "ladder"])]
    book: String,
    #[arg(long, default_value = "benchmarks/orderbook/correctness.json")]
    reference: PathBuf,
    #[arg(long, default_value = "benchmarks/orderbook/samples.csv")]
    output: PathBuf,
}

struct Corpus {
    json: Vec<Vec<u8>>,
    sbe: Vec<Vec<u8>>,
    skipped: usize,
}
fn load(path: &std::path::Path) -> Corpus {
    let mut json_frames = Vec::new();
    let mut skipped = 0;
    for line in fs::read_to_string(path.join("deltas.jsonl"))
        .unwrap()
        .lines()
    {
        let raw = if line.starts_with('{') {
            line
        } else {
            line.split_once(' ').expect("capture prefix").1
        };
        let obj: Value =
            serde_json::from_str(raw).expect("valid JSON, never silently skip corruption");
        if obj.get("result").is_some() && obj.get("id").is_some() {
            skipped += 1;
            continue;
        }
        let _: Frame = serde_json::from_str(raw).expect("depth frame");
        json_frames.push(raw.as_bytes().to_vec());
    }
    let bytes = fs::read(path.join("output.sbe")).unwrap();
    let mut rest = bytes.as_slice();
    let mut sbe_frames = Vec::new();
    while !rest.is_empty() {
        assert!(rest.len() >= 4, "truncated length");
        let n = u32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
        rest = &rest[4..];
        assert!(n >= 8 && n <= rest.len(), "invalid SBE frame length");
        sbe_frames.push(rest[..n].to_vec());
        rest = &rest[n..];
    }
    assert!(!json_frames.is_empty());
    assert_eq!(json_frames.len(), sbe_frames.len(), "message counts");
    Corpus {
        json: json_frames,
        sbe: sbe_frames,
        skipped,
    }
}
fn decode(raw: &[u8], format: &str) -> (WireLevels, WireLevels) {
    if format == "json" {
        let d: Frame = serde_json::from_slice(raw).unwrap();
        (d.data.bids, d.data.asks)
    } else {
        let d = sbe::decode(raw).unwrap();
        (d.bids, d.asks)
    }
}
fn normalize(bids: &WireLevels, asks: &WireLevels) -> Vec<BookUpdate> {
    let mut out = Vec::with_capacity(bids.len() + asks.len());
    for (side, levels) in [(Side::Bid, bids), (Side::Ask, asks)] {
        out.extend(levels.iter().map(|(p, s)| BookUpdate {
            side,
            price: Price(p.0),
            size: Size(s.0),
        }));
    }
    out
}
enum Storage {
    Tree(OrderBook),
    Ladder(PriceLadder),
}
struct Book {
    storage: Storage,
    below: u64,
    above: u64,
    out: u64,
    updates: u64,
}
impl Book {
    fn new(kind: &str) -> Self {
        Self {
            storage: if kind == "btree" {
                Storage::Tree(OrderBook::default())
            } else {
                Storage::Ladder(PriceLadder::covering(Price(MIN), Price(MAX), TICK).unwrap())
            },
            below: 0,
            above: 0,
            out: 0,
            updates: 0,
        }
    }
    fn apply(&mut self, updates: &[BookUpdate]) {
        for u in updates {
            self.updates += 1;
            if u.price.0 < MIN {
                self.below += 1;
                continue;
            }
            if u.price.0 > MAX {
                self.above += 1;
                continue;
            }
            match &mut self.storage {
                Storage::Tree(b) => b.upsert(u.side, u.price, u.size),
                Storage::Ladder(b) => {
                    if b.upsert(u.side, u.price, u.size).unwrap() == LadderUpdate::OutOfRange {
                        self.out += 1;
                    }
                }
            }
        }
    }
    fn state(&self) -> State {
        match &self.storage {
            Storage::Tree(b) => b.top_n(usize::MAX),
            Storage::Ladder(b) => (
                b.view().levels(Side::Bid).collect(),
                b.view().levels(Side::Ask).collect(),
            ),
        }
    }
    fn best(&self) -> (Option<(Price, Size)>, Option<(Price, Size)>) {
        match &self.storage {
            Storage::Tree(b) => (
                b.best_bid().map(|(p, s)| (*p, *s)),
                b.best_ask().map(|(p, s)| (*p, *s)),
            ),
            Storage::Ladder(b) => (b.best_bid(), b.best_ask()),
        }
    }
    fn summary(&self) -> Value {
        assert_eq!(self.out, 0);
        let (bids, asks) = self.state();
        let mut canonical = OrderBook::default();
        canonical.replace_bids(&bids);
        canonical.replace_asks(&asks);
        let hash: String = canonical
            .state_hash()
            .iter()
            .map(|x| format!("{x:02x}"))
            .collect();
        let levels =
            |v: &[(Price, Size)]| v.iter().map(|(p, s)| json!([p.0, s.0])).collect::<Vec<_>>();
        json!({"updates":self.updates,"filtered_below":self.below,"filtered_above":self.above,"out_of_range":self.out,"bid_levels":bids.len(),"ask_levels":asks.len(),"state_sha256":hash,"top5_bids":levels(&bids[..bids.len().min(5)]),"top5_asks":levels(&asks[..asks.len().min(5)])})
    }
}
fn validate(c: &Corpus) -> Value {
    let mut books = [
        Book::new("btree"),
        Book::new("ladder"),
        Book::new("btree"),
        Book::new("ladder"),
    ];
    let mut buckets = [0usize; 5];
    for (j, s) in c.json.iter().zip(&c.sbe) {
        let jd: Frame = serde_json::from_slice(j).unwrap();
        let sd = sbe::decode(s).unwrap();
        assert_eq!(
            (
                jd.data.first_u,
                jd.data.final_u,
                jd.data.message_time * 1000
            ),
            (sd.first_u, sd.final_u, sd.event_time_us)
        );
        let ju = normalize(&jd.data.bids, &jd.data.asks);
        let su = normalize(&sd.bids, &sd.asks);
        assert_eq!(
            ju, su,
            "all updates, including filtered ones, must match in order"
        );
        assert!(!ju.is_empty(), "empty messages need a separate bucket");
        buckets[bucket(ju.len())] += 1;
        books[0].apply(&ju);
        books[1].apply(&ju);
        books[2].apply(&su);
        books[3].apply(&su);
        for b in &books[1..] {
            assert_eq!(books[0].best(), b.best(), "BBO after each message");
        }
    }
    let state = books[0].state();
    let summary = books[0].summary();
    for b in &books[1..] {
        assert_eq!(state, b.state(), "strict equality of every final level");
        assert_eq!(summary, b.summary());
    }
    json!({"messages":c.json.len(),"skipped_control_frames":c.skipped,"buckets":buckets,"book":summary})
}
fn bucket(n: usize) -> usize {
    match n {
        0..=50 => 0,
        51..=200 => 1,
        201..=500 => 2,
        501..=1000 => 3,
        _ => 4,
    }
}
fn proc_status() -> String {
    fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .filter(|l| {
            l.starts_with("VmRSS:")
                || l.starts_with("VmSize:")
                || l.starts_with("VmHWM:")
                || l.starts_with("Cpus_allowed_list:")
        })
        .collect::<Vec<_>>()
        .join("\n")
}
fn faults() -> (u64, u64) {
    let s = fs::read_to_string("/proc/self/stat").unwrap();
    let fields: Vec<_> = s.rsplit_once(')').unwrap().1.split_whitespace().collect();
    (fields[7].parse().unwrap(), fields[9].parse().unwrap())
}
fn main() {
    assert!(!cfg!(debug_assertions), "use --release");
    let a = Args::parse();
    let c = load(&a.input);
    if a.validate {
        let result = validate(&c);
        fs::write(&a.reference, serde_json::to_string_pretty(&result).unwrap()).unwrap();
        println!("{result}");
        return;
    }
    let expected: Value =
        serde_json::from_slice(&fs::read(&a.reference).expect("run --validate before measuring"))
            .unwrap();
    // The raw corpora are fully resident; no file access or logging in the loop.
    let frames = if a.format == "json" { &c.json } else { &c.sbe };
    assert_eq!(expected["messages"].as_u64().unwrap(), frames.len() as u64);
    // One complete untimed warmup per measured process, starting empty.
    {
        let mut warm = Book::new(&a.book);
        for raw in frames {
            let (bids, asks) = decode(black_box(raw), &a.format);
            warm.apply(&normalize(&bids, &asks));
        }
        assert_eq!(warm.summary(), expected["book"]);
    }
    let mut samples = Vec::with_capacity(frames.len());
    let before = proc_status();
    let mem0 = allocations().1;
    let mut book = Book::new(&a.book);
    let mut book_bytes = allocations().1 - mem0;
    let mut stage_allocs = [0u64; 3];
    let f0 = faults();
    for raw in frames {
        let c0 = allocations();
        let t0 = Instant::now();
        let (bids, asks) = decode(black_box(raw), &a.format);
        let t1 = Instant::now();
        let c1 = allocations();
        let updates = normalize(&bids, &asks);
        let t2 = Instant::now();
        let c2 = allocations();
        book.apply(black_box(&updates));
        let t3 = Instant::now();
        let c3 = allocations();
        book_bytes += c3.1 - c2.1;
        stage_allocs[0] += c1.0 - c0.0;
        stage_allocs[1] += c2.0 - c1.0;
        stage_allocs[2] += c3.0 - c2.0;
        samples.push((
            updates.len(),
            (t1 - t0).as_nanos(),
            (t2 - t1).as_nanos(),
            (t3 - t2).as_nanos(),
            (t3 - t0).as_nanos(),
        ));
        black_box(&book);
        // Temporary Vec deallocations occur after t3 for both formats.
    }
    let f1 = faults();
    let after = proc_status();
    let summary = book.summary();
    assert_eq!(summary, expected["book"], "refuse mismatched final state");
    let mut out = fs::File::create(&a.output).unwrap();
    writeln!(
        out,
        "updates,parse_ns,normalize_ns,apply_ns,tick_to_book_ns"
    )
    .unwrap();
    for (n, p, z, b, t) in samples {
        writeln!(out, "{n},{p},{z},{b},{t}").unwrap();
    }
    let report = json!({"format":a.format,"book_kind":a.book,"messages":frames.len(),"correctness":summary,"allocator_instrumented":cfg!(feature="study-alloc"),"stage_allocations":stage_allocs,"book_requested_heap_bytes":book_bytes,"minor_faults_loop":f1.0-f0.0,"major_faults_loop":f1.1-f0.1,"process_before_book":before,"process_after_loop":after});
    fs::write(
        a.output.with_extension("json"),
        serde_json::to_string_pretty(&report).unwrap(),
    )
    .unwrap();
    println!("{report}");
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn inclusive_filter_and_delete_have_identical_state() {
        let mut tree = Book::new("btree");
        let mut ladder = Book::new("ladder");
        let updates: Vec<_> = [(MIN - 1, 2), (MIN, 2), (MAX, 3), (MAX + 1, 4), (MIN, 0)]
            .into_iter()
            .flat_map(|(p, s)| {
                [Side::Bid, Side::Ask].map(|side| BookUpdate {
                    side,
                    price: Price(p),
                    size: Size(s),
                })
            })
            .collect();
        tree.apply(&updates);
        ladder.apply(&updates);
        assert_eq!(tree.state(), ladder.state());
        assert_eq!(tree.summary(), ladder.summary());
        assert_eq!((tree.below, tree.above, ladder.out), (2, 2, 0));
        assert_eq!(
            tree.best(),
            (Some((Price(MAX), Size(3))), Some((Price(MAX), Size(3))))
        );
    }
}
