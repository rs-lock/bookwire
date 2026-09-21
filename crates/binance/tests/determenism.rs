use clob_binance::{
    model::BinanceSnapshot,
    sources::replay::{Replay, ReplayClock, ReplaySnap, Speed},
    venue::Binance,
};
use clob_core::{OrderBook, Price, Size};
use clob_venue::{Venue, VenueBookUpdate, VenueError};
use std::time::Instant;

// Paths are resolved from the crate manifest so the tests don't depend on the
// process working directory (integration tests run with cwd = crate root).
const DELTAS: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/deltas.jsonl");
const SNAP_T0: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/snap_t0.jsonl");
const SNAP_T1: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/snap_t1.json");

/// A replay venue over the fixtures, at `--speed=max` (no pacing — we want the
/// compute path, not wall-clock timing).
async fn replay_venue() -> Binance<Replay, ReplaySnap> {
    let clock = ReplayClock::new(0, Instant::now(), Speed::Max);
    Binance::new(
        "BTC",
        Replay::open(DELTAS, clock)
            .await
            .expect("open deltas fixture"),
        ReplaySnap::open(SNAP_T0, clock),
    )
}

/// Drive the venue to end-of-replay, applying every update into a fresh book.
async fn build_book_to_eof() -> OrderBook {
    let mut venue = replay_venue().await;
    venue.connect().await.expect("connect");

    let mut book = OrderBook::default();
    loop {
        match venue.next_updates().await {
            Ok(update) => update.venue_update.apply(&mut book),
            Err(VenueError::ReplayEof) => break,
            Err(e) => panic!("venue error: {e}"),
        }
    }
    book
}

/// Compare price/size on raw integers so the two distinct `Price`/`Size`
/// newtypes (core vs binance::model) don't get in the way — both share the same
/// fixed-point scale (8 decimals).
fn raw(levels: &[(Price, Size)]) -> Vec<(i64, i64)> {
    levels.iter().map(|(p, s)| (p.0, s.0 as i64)).collect()
}

/// Turn a REST snapshot into a book-replacing update (model levels -> core).
fn snapshot_update(snap: &BinanceSnapshot) -> VenueBookUpdate {
    let bids = snap
        .bids
        .iter()
        .map(|(p, s)| (Price(p.0), Size(s.0)))
        .collect();
    let asks = snap
        .asks
        .iter()
        .map(|(p, s)| (Price(p.0), Size(s.0)))
        .collect();
    VenueBookUpdate::Snapshot { bids, asks }
}

/// Correctness: the book built from `snap_t0 + deltas` matches an independent
/// REST snapshot pulled mid-stream (`snap_t1`), i.e. our apply logic never
/// diverges from the exchange.
///
/// Binance's `lastUpdateId` sits *inside* a delta event, so once we cross it the
/// book is at the bridge event's `u`, slightly past `l`. We bring the reference
/// to the same point by applying that very same bridge event on top of `snap_t1`
/// — then both books are at `B.u` and the comparison is exact, not flaky.
///
/// Fixture requirement: `snap_t0` must precede `snap_t1` (so the bridge is a
/// live delta after the splice buffer drains), and the deltas must cover `l`.
#[tokio::test]
async fn delta_built_book_matches_exchange_snapshot() {
    let raw_snap = tokio::fs::read_to_string(SNAP_T1)
        .await
        .expect("read snap_t1");
    let snap_t1 = serde_json::from_str::<BinanceSnapshot>(&raw_snap).expect("parse snap_t1");
    let l = snap_t1.last_u;

    let mut venue = replay_venue().await;
    venue.connect().await.expect("connect");

    let mut book = OrderBook::default();
    loop {
        match venue.next_updates().await {
            Ok(update) => {
                update.venue_update.apply(&mut book);

                // Only a delta can be the bridge; never mistake the splice
                // snapshot for it. The first delta whose `u` reaches `l` is B.
                if matches!(update.venue_update, VenueBookUpdate::Delta(_))
                    && venue.last_u().is_some_and(|u| u >= l)
                {
                    let mut refbook = OrderBook::default();
                    snapshot_update(&snap_t1).apply(&mut refbook); // snap_t1 @ l
                    update.venue_update.apply(&mut refbook); // + same bridge B  -> @ B.u

                    let (bids, asks) = book.top_n(100);
                    let (rbids, rasks) = refbook.top_n(100);
                    assert_eq!(raw(&bids), raw(&rbids), "bids diverged from exchange");
                    assert_eq!(raw(&asks), raw(&rasks), "asks diverged from exchange");
                    return;
                }
            }
            Err(VenueError::ReplayEof) => {
                panic!("replay drained before reaching snap_t1 lastUpdateId {l}")
            }
            Err(e) => panic!("venue error: {e}"),
        }
    }
}

/// Determinism: replaying the same recording twice yields the identical final
/// book (full-state hash). No reference snapshot involved — this is purely about
/// reproducibility of the pipeline.
#[tokio::test]
async fn replay_is_deterministic() {
    let a = build_book_to_eof().await;
    let b = build_book_to_eof().await;
    assert_eq!(
        a.state_hash(),
        b.state_hash(),
        "replay is not deterministic"
    );
}
