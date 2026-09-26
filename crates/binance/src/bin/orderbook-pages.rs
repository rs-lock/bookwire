//! Untimed OS-fault diagnostics for ladder first touches, separate from the study.
//! Instrumented timings are not part of the published comparison.
use clob_binance::sbe;
use clob_core::{
    Price, SCALE, Side, Size,
    price_ladder::{LadderUpdate, PriceLadder},
};
use clob_venue::BookUpdate;
use serde_json::json;
use std::{fs, time::Instant};

fn faults() -> u64 {
    let s = fs::read_to_string("/proc/self/stat").unwrap();
    s.rsplit_once(')')
        .unwrap()
        .1
        .split_whitespace()
        .nth(7)
        .unwrap()
        .parse()
        .unwrap()
}
fn apply(book: &mut PriceLadder, updates: &[BookUpdate]) {
    for u in updates {
        if u.price.0 < 50000 * SCALE || u.price.0 > 120000 * SCALE {
            continue;
        }
        assert_eq!(
            book.upsert(u.side, u.price, u.size).unwrap(),
            LadderUpdate::Applied
        );
    }
}
fn main() {
    assert!(!cfg!(debug_assertions), "use --release");
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "crates/binance/replay/output.sbe".into());
    let bytes = fs::read(path).unwrap();
    let mut rest = bytes.as_slice();
    let mut updates = Vec::new();
    while !rest.is_empty() {
        let n = u32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
        let d = sbe::decode(&rest[4..4 + n]).unwrap();
        rest = &rest[4 + n..];
        let mut u = Vec::with_capacity(d.bids.len() + d.asks.len());
        for (side, levels) in [(Side::Bid, d.bids), (Side::Ask, d.asks)] {
            u.extend(levels.into_iter().map(|(p, s)| BookUpdate {
                side,
                price: Price(p.0),
                size: Size(s.0),
            }));
        }
        updates.push(u);
    }
    let mut reference = None;
    let mut results = Vec::new();
    // Same fresh empty book in each case. Prefaulting writes zeros over every
    // quantity/bitmap page through the existing clear_side API before replay.
    for prefault in [false, true] {
        let mut book =
            PriceLadder::covering(Price(50000 * SCALE), Price(120000 * SCALE), SCALE / 10).unwrap();
        if prefault {
            book.clear_side(Side::Bid);
            book.clear_side(Side::Ask);
        }
        let mut selected = Vec::new();
        let mut total_faults = 0;
        for (i, u) in updates.iter().enumerate() {
            let before = faults();
            let start = Instant::now();
            apply(&mut book, u);
            let ns = start.elapsed().as_nanos();
            let delta = faults() - before;
            total_faults += delta;
            if delta > 0 || [0, 22, 102, 16570, 25987, 40098].contains(&i) {
                selected.push(json!({"message":i+1,"updates":u.len(),"minor_faults":delta,"instrumented_apply_ns":ns}));
            }
        }
        let state = (
            book.view().levels(Side::Bid).collect::<Vec<_>>(),
            book.view().levels(Side::Ask).collect::<Vec<_>>(),
        );
        if let Some(r) = &reference {
            assert_eq!(r, &state);
        } else {
            reference = Some(state);
        }
        results.push(json!({"prefault":prefault,"total_minor_faults_around_apply":total_faults,"messages_with_faults_or_selected":selected}));
    }
    println!("{}", serde_json::to_string_pretty(&results).unwrap());
}
