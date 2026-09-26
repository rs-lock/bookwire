use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use clap::Parser;
use clob_binance::model::parse_fixed;
use clob_binance::sbe::{SbeReplay, decode as decode_sbe};
use clob_binance::sources::live::{BinanceRest, BinanceWs};
use clob_binance::sources::replay::{Replay, ReplayClock, ReplaySnap, Speed};
use clob_binance::sources::{Frames, Snaps};
use clob_binance::venue::Binance;
use clob_core::price_ladder::{LadderUpdate, PriceLadder};
use clob_core::{OrderBook, Price, Side, Size};
use clob_venue::{BookUpdate, Venue, VenueError};
use hdrhistogram::Histogram;

type ActiveBinance = Binance<Frames, Snaps>;

#[derive(Clone, Copy, clap::ValueEnum)]
enum SourceKind {
    Live,
    Replay,
}

#[derive(Clone, Copy, clap::ValueEnum, PartialEq, Eq)]
enum ReplayFormat {
    Json,
    Sbe,
}

#[derive(Clone, Copy, clap::ValueEnum, PartialEq, Eq)]
enum BookKind {
    Btree,
    Ladder,
}

#[derive(clap::Parser)]
struct Args {
    /// Where frames come from: the live socket or a recorded replay file
    #[arg(long, value_enum, default_value = "live")]
    source: SourceKind,

    /// Replay wire format. SBE is supported for replay only.
    #[arg(long, value_enum, default_value = "json")]
    format: ReplayFormat,

    /// Replay pacing (ignored for `--source live`): `one`, `max`, or `xN`
    #[arg(long, default_value = "max")]
    speed: Speed,

    /// Override the replay delta file.
    #[arg(long)]
    replay_file: Option<PathBuf>,

    /// Override the JSON snapshot file.
    #[arg(long)]
    snapshot_file: Option<PathBuf>,

    /// Book implementation used by the SBE replay benchmark.
    #[arg(long, value_enum, default_value = "btree")]
    book: BookKind,

    /// Instrument tick size used to map prices to ladder indexes.
    #[arg(long, default_value = "0.1", value_parser = parse_tick_size)]
    tick_size: i64,

    /// Ignore SBE levels below this price (for example `50000`).
    #[arg(long, value_parser = parse_price)]
    min_book_price: Option<i64>,

    /// Ignore SBE levels above this price (for example `120000`).
    #[arg(long, value_parser = parse_price)]
    max_book_price: Option<i64>,
}

fn parse_tick_size(value: &str) -> Result<i64, String> {
    let tick_size = parse_fixed(value).map_err(|error| error.to_string())?;
    if tick_size <= 0 {
        return Err("tick size must be positive".into());
    }
    Ok(tick_size)
}

fn parse_price(value: &str) -> Result<i64, String> {
    parse_fixed(value).map_err(|error| error.to_string())
}

async fn make_binance(args: &Args) -> Result<ActiveBinance, Box<dyn std::error::Error>> {
    Ok(match args.source {
        SourceKind::Live => Binance::new(
            "BTC",
            Frames::Live(BinanceWs::new("BTC")),
            Snaps::Live(BinanceRest::new()),
        ),
        SourceKind::Replay => {
            let clock = ReplayClock::new(0, Instant::now(), args.speed);
            Binance::new(
                "BTC",
                Frames::Replay(
                    Replay::open(
                        args.replay_file
                            .as_deref()
                            .unwrap_or(Path::new("crates/binance/replayz/deltas.jsonl")),
                        clock,
                    )
                    .await?,
                ),
                Snaps::Replay(ReplaySnap::open(
                    args.snapshot_file
                        .clone()
                        .unwrap_or_else(|| "crates/binance/replayz/snapshot.jsonl".into()),
                    clock,
                )),
            )
        }
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("install rustls crypto provider");

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let args = Args::parse();
    if args.format == ReplayFormat::Sbe {
        if !matches!(args.source, SourceKind::Replay) {
            return Err("--format sbe requires --source replay".into());
        }
        let path = args
            .replay_file
            .as_deref()
            .unwrap_or(Path::new("crates/binance/replay/output.sbe"));
        run_sbe_replay(
            path,
            args.book,
            args.tick_size,
            args.min_book_price.map(Price),
            args.max_book_price.map(Price),
        )
        .await?;
        return Ok(());
    }
    if args.book != BookKind::Btree {
        return Err("--book ladder is currently a replay benchmark for --format sbe".into());
    }

    let mut venue = make_binance(&args).await?;
    venue.connect().await?;

    let mut feed_worker = FeedHandler::new(OrderBook::default(), venue);

    feed_worker.run().await?;
    Ok(())
}

struct FeedHandler<V> {
    ob: OrderBook,
    venue: V,
    latency: LatencyRecorder,
}

impl<V: Venue> FeedHandler<V> {
    fn new(ob: OrderBook, venue: V) -> Self {
        Self {
            ob,
            venue,
            latency: LatencyRecorder::new(),
        }
    }

    async fn run(&mut self) -> Result<(), VenueError> {
        loop {
            match self.venue.next_updates().await {
                Ok(update) => {
                    update.venue_update.apply(&mut self.ob);
                    let book_updated = Instant::now();

                    if let Some(timing) = update.timing {
                        self.latency.record(
                            nanos(timing.parsed_at - timing.frame_ready_at),
                            nanos(book_updated - timing.parsed_at),
                            nanos(book_updated - timing.frame_ready_at),
                            timing.frame_bytes,
                            timing.update_count,
                        );
                    }
                }

                // Replay drained: not an error, just the end of the recording.
                Err(VenueError::ReplayEof) => {
                    println!("replay finished");
                    self.latency.print();
                    break;
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }
}

enum ReplayBookStorage {
    Btree(OrderBook),
    Ladder(PriceLadder),
}

struct ReplayBook {
    storage: ReplayBookStorage,
    min_price: Option<Price>,
    max_price: Option<Price>,
    filtered_below: u64,
    filtered_above: u64,
    out_of_range: u64,
}

impl ReplayBook {
    fn apply(&mut self, updates: &[BookUpdate]) -> Result<(), Box<dyn std::error::Error>> {
        for update in updates {
            if self.min_price.is_some_and(|min| update.price < min) {
                self.filtered_below += 1;
                continue;
            }
            if self.max_price.is_some_and(|max| update.price > max) {
                self.filtered_above += 1;
                continue;
            }

            match &mut self.storage {
                ReplayBookStorage::Btree(book) => {
                    book.upsert(update.side, update.price, update.size);
                }
                ReplayBookStorage::Ladder(book) => {
                    if book.upsert(update.side, update.price, update.size)?
                        == LadderUpdate::OutOfRange
                    {
                        self.out_of_range += 1;
                    }
                }
            }
        }
        Ok(())
    }

    fn print_summary(&self) {
        match &self.storage {
            ReplayBookStorage::Btree(_) => println!(
                "book: BTreeMap, min_price={}, max_price={}, filtered_below={}, filtered_above={}",
                display_optional_price(self.min_price),
                display_optional_price(self.max_price),
                self.filtered_below,
                self.filtered_above,
            ),
            ReplayBookStorage::Ladder(book) => println!(
                "book: price ladder, range={:.8}..={:.8}, tick={:.8}, slots={}, memory={:.2} MiB, filtered_below={}, filtered_above={}, out_of_range={}",
                book.base_price().to_f64(),
                book.max_price().to_f64(),
                book.tick_size() as f64 / clob_core::SCALE as f64,
                book.capacity(),
                book.allocated_bytes() as f64 / (1024.0 * 1024.0),
                self.filtered_below,
                self.filtered_above,
                self.out_of_range,
            ),
        }
    }
}

fn display_optional_price(price: Option<Price>) -> String {
    price.map_or_else(|| "none".into(), |price| format!("{:.8}", price.to_f64()))
}

async fn sbe_price_bounds(
    path: &Path,
    min_filter: Option<Price>,
    max_filter: Option<Price>,
) -> Result<(Price, Price), Box<dyn std::error::Error>> {
    let mut replay = SbeReplay::open(path).await?;
    let mut min_price: Option<i64> = None;
    let mut max_price: Option<i64> = None;

    while let Some(raw) = replay.next().await? {
        let depth = decode_sbe(&raw)?;
        for (price, _) in depth.bids.iter().chain(&depth.asks) {
            if min_filter.is_some_and(|min| price.0 < min.0) {
                continue;
            }
            if max_filter.is_some_and(|max| price.0 > max.0) {
                continue;
            }
            min_price = Some(min_price.map_or(price.0, |current| current.min(price.0)));
            max_price = Some(max_price.map_or(price.0, |current| current.max(price.0)));
        }
    }

    match (min_price, max_price) {
        (Some(observed_min), Some(observed_max)) => Ok((
            min_filter.unwrap_or(Price(observed_min)),
            max_filter.unwrap_or(Price(observed_max)),
        )),
        _ => Err("SBE replay contains no price levels".into()),
    }
}

async fn run_sbe_replay(
    path: &Path,
    book_kind: BookKind,
    tick_size: i64,
    min_price: Option<Price>,
    max_price: Option<Price>,
) -> Result<(), Box<dyn std::error::Error>> {
    if min_price.is_some_and(|price| price.0 % tick_size != 0) {
        return Err("--min-book-price must be aligned to --tick-size".into());
    }
    if max_price.is_some_and(|price| price.0 % tick_size != 0) {
        return Err("--max-book-price must be aligned to --tick-size".into());
    }
    if min_price.zip(max_price).is_some_and(|(min, max)| min > max) {
        return Err("--min-book-price must not exceed --max-book-price".into());
    }

    let storage = match book_kind {
        BookKind::Btree => ReplayBookStorage::Btree(OrderBook::default()),
        BookKind::Ladder => {
            // This preparatory pass is intentionally outside all latency timings.
            // It gives the replay benchmark a lossless fixed window, so it applies
            // exactly the same levels as the BTreeMap implementation.
            let (min_price, max_price) = sbe_price_bounds(path, min_price, max_price).await?;
            ReplayBookStorage::Ladder(PriceLadder::covering(min_price, max_price, tick_size)?)
        }
    };
    let mut book = ReplayBook {
        storage,
        min_price,
        max_price,
        filtered_below: 0,
        filtered_above: 0,
        out_of_range: 0,
    };
    let mut replay = SbeReplay::open(path).await?;
    let mut latency = LatencyRecorder::new();

    while let Some(raw) = replay.next().await? {
        // Match the JSON measurement boundary: file/channel I/O and allocation
        // of the raw frame are complete before this timestamp.
        let frame_ready_at = Instant::now();
        let depth = decode_sbe(&raw)?;
        let parsed_at = Instant::now();
        let update_count = depth.bids.len() + depth.asks.len();

        let mut updates = Vec::with_capacity(update_count);
        updates.extend(depth.bids.iter().map(|(price, size)| BookUpdate {
            side: Side::Bid,
            price: Price(price.0),
            size: Size(size.0),
        }));
        updates.extend(depth.asks.iter().map(|(price, size)| BookUpdate {
            side: Side::Ask,
            price: Price(price.0),
            size: Size(size.0),
        }));

        book.apply(&updates)?;
        let book_updated = Instant::now();
        latency.record(
            nanos(parsed_at - frame_ready_at),
            nanos(book_updated - parsed_at),
            nanos(book_updated - frame_ready_at),
            raw.len(),
            update_count,
        );
    }

    println!(
        "SBE replay finished: {} (decode + normalize + apply; no futures pu in spot SBE)",
        path.display()
    );
    book.print_summary();
    latency.print();
    Ok(())
}

#[inline]
fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

struct LatencyRecorder {
    parse: Histogram<u64>,
    apply: Histogram<u64>,
    total: Histogram<u64>,
    per_update: Histogram<u64>,
    buckets: [LatencyBucket; 5],
}
impl LatencyRecorder {
    fn new() -> Self {
        Self {
            parse: hdrhistogram::Histogram::new(3).unwrap(),
            apply: hdrhistogram::Histogram::new(3).unwrap(),
            total: hdrhistogram::Histogram::new(3).unwrap(),
            per_update: hdrhistogram::Histogram::new(3).unwrap(),
            buckets: [
                LatencyBucket::new("1-50"),
                LatencyBucket::new("51-200"),
                LatencyBucket::new("201-500"),
                LatencyBucket::new("501-1000"),
                LatencyBucket::new("1001+"),
            ],
        }
    }

    #[inline]
    fn record(
        &mut self,
        parse_ns: u64,
        apply_ns: u64,
        ttb_ns: u64,
        frame_bytes: usize,
        update_count: usize,
    ) {
        self.parse.record(parse_ns).expect("record parse latency");
        self.apply.record(apply_ns).expect("record apply latency");
        self.total
            .record(ttb_ns)
            .expect("record tick to book latency");
        if update_count != 0 {
            self.per_update
                .record(ttb_ns / update_count as u64)
                .expect("record latency per update");
        }

        let bucket = match update_count {
            0..=50 => 0,
            51..=200 => 1,
            201..=500 => 2,
            501..=1000 => 3,
            _ => 4,
        };
        self.buckets[bucket].record(parse_ns, apply_ns, ttb_ns, frame_bytes as u64);
    }

    pub fn print(&self) {
        for (name, histogram) in [
            ("parse", &self.parse),
            ("apply", &self.apply),
            ("tick_to_book", &self.total),
        ] {
            println!(
                "{name:>12}: p50={} ns, p99={} ns, p99.9={} ns, max={} ns, n={}",
                histogram.value_at_quantile(0.50),
                histogram.value_at_quantile(0.99),
                histogram.value_at_quantile(0.999),
                histogram.max(),
                histogram.len(),
            );
        }

        println!(
            "  ns/update: p50={} ns, p99={} ns, p99.9={} ns",
            self.per_update.value_at_quantile(0.50),
            self.per_update.value_at_quantile(0.99),
            self.per_update.value_at_quantile(0.999),
        );
        println!("\nby updates/message:");
        println!(
            "       bucket        bytes p50/p99       parse p50/p99       apply p50/p99         ttb p50/p99       n"
        );
        for bucket in &self.buckets {
            bucket.print();
        }
    }
}

struct LatencyBucket {
    name: &'static str,
    bytes: Histogram<u64>,
    parse: Histogram<u64>,
    apply: Histogram<u64>,
    total: Histogram<u64>,
}

impl LatencyBucket {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            bytes: Histogram::new(3).unwrap(),
            parse: Histogram::new(3).unwrap(),
            apply: Histogram::new(3).unwrap(),
            total: Histogram::new(3).unwrap(),
        }
    }

    #[inline]
    fn record(&mut self, parse_ns: u64, apply_ns: u64, total_ns: u64, bytes: u64) {
        self.bytes.record(bytes).expect("record frame bytes");
        self.parse.record(parse_ns).expect("record bucket parse");
        self.apply.record(apply_ns).expect("record bucket apply");
        self.total.record(total_ns).expect("record bucket total");
    }

    fn print(&self) {
        if self.total.is_empty() {
            return;
        }
        println!(
            "{:>13}  {:>7}/{:<7}  {:>7}/{:<7} ns  {:>7}/{:<7} ns  {:>7}/{:<7} ns  {}",
            self.name,
            self.bytes.value_at_quantile(0.50),
            self.bytes.value_at_quantile(0.99),
            self.parse.value_at_quantile(0.50),
            self.parse.value_at_quantile(0.99),
            self.apply.value_at_quantile(0.50),
            self.apply.value_at_quantile(0.99),
            self.total.value_at_quantile(0.50),
            self.total.value_at_quantile(0.99),
            self.total.len(),
        );
    }
}
