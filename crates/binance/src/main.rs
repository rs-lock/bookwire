use std::time::{Duration, Instant};

use clap::Parser;
use clob_binance::sources::live::{BinanceRest, BinanceWs};
use clob_binance::sources::replay::{Replay, ReplayClock, ReplaySnap, Speed};
use clob_binance::sources::{Frames, Snaps};
use clob_binance::venue::Binance;
use clob_core::OrderBook;
use clob_venue::{Venue, VenueError};
use hdrhistogram::Histogram;

type ActiveBinance = Binance<Frames, Snaps>;

#[derive(Clone, Copy, clap::ValueEnum)]
enum SourceKind {
    Live,
    Replay,
}

#[derive(clap::Parser)]
struct Args {
    /// Where frames come from: the live socket or a recorded replay file
    #[arg(long, value_enum, default_value = "live")]
    source: SourceKind,

    /// Replay pacing (ignored for `--source live`): `one`, `max`, or `xN`
    #[arg(long, default_value = "max")]
    speed: Speed,
}

async fn make_binance(args: &Args) -> ActiveBinance {
    match args.source {
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
                    Replay::open("crates/binance/replayz/deltas.jsonl", clock)
                        .await
                        .unwrap(),
                ),
                Snaps::Replay(ReplaySnap::open(
                    "crates/binance/replayz/snapshot.jsonl",
                    clock,
                )),
            )
        }
    }
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
    let mut venue = make_binance(&args).await;
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

#[inline]
fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

struct LatencyRecorder {
    parse: Histogram<u64>,
    apply: Histogram<u64>,
    total: Histogram<u64>,
}
impl LatencyRecorder {
    fn new() -> Self {
        Self {
            parse: hdrhistogram::Histogram::new(3).unwrap(),
            apply: hdrhistogram::Histogram::new(3).unwrap(),
            total: hdrhistogram::Histogram::new(3).unwrap(),
        }
    }

    #[inline]
    fn record(&mut self, parse_ns: u64, apply_ns: u64, ttb_ns: u64) {
        self.parse.record(parse_ns).expect("record parse latency");
        self.apply.record(apply_ns).expect("record apply latency");
        self.total
            .record(ttb_ns)
            .expect("record tick to book latency");
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
    }
}
