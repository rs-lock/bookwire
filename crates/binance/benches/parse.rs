//! Produce a sample first by running the feed with capture enabled, e.g.:
//!
//! ```text
//! BINANCE_CAPTURE=misc/capture.jsonl BINANCE_CAPTURE_N=2000 cargo run -p clob-binance
//! ```
//!
//! then benchmark it (path overridable via `BINANCE_CAPTURE`):
//!
//! ```text
//! BINANCE_CAPTURE=misc/capture.jsonl cargo bench -p clob-binance
//! ```
//!

use std::hint::black_box;

use clob_binance::venue::parse;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};

use crate::sbe::{SbeDecoder, decode_header, decode_message};

mod sbe;

const DEFAULT_SAMPLE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/replay/deltas.jsonl");
const DEFAULT_SBE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/replay/output.sbe");

fn load_frames() -> Vec<(Option<u64>, String)> {
    let path = std::env::var("BINANCE_CAPTURE").unwrap_or_else(|_| DEFAULT_SAMPLE.into());
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "cannot read capture file '{path}': {e}\n\
             capture one first: BINANCE_CAPTURE={path} cargo run -p clob-binance"
        )
    });
    let mut frames = Vec::new();
    let mut skipped_acks = 0;
    let line_count = text.lines().count();
    for (index, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if index + 1 == line_count && line.parse::<u64>().is_ok() {
            eprintln!(
                "{path}:{}: skipping incomplete final timestamp-only record",
                index + 1
            );
            continue;
        }
        let (recv_ts_ns, json) = if line.starts_with('{') {
            (None, line)
        } else {
            let (timestamp, json) = line
                .split_once(char::is_whitespace)
                .unwrap_or_else(|| panic!("{path}:{}: expected timestamp + JSON", index + 1));
            let timestamp = timestamp
                .parse::<u64>()
                .unwrap_or_else(|e| panic!("{path}:{}: invalid timestamp: {e}", index + 1));
            (Some(timestamp), json.trim_start())
        };
        if let Err(error) = parse(json) {
            let value: serde_json::Value = serde_json::from_str(json)
                .unwrap_or_else(|e| panic!("{path}:{}: invalid JSON: {e}", index + 1));
            if value.get("data").is_none()
                && value.get("result").is_some_and(serde_json::Value::is_null)
                && value.get("id").is_some()
            {
                skipped_acks += 1;
                continue;
            }
            panic!("{path}:{}: invalid depth frame: {error}", index + 1);
        }

        frames.push((recv_ts_ns, json.to_owned()));
    }
    eprintln!(
        "Validated {} depth frames; skipped {skipped_acks} subscription acknowledgements",
        frames.len()
    );
    assert!(!frames.is_empty(), "capture file '{path}' is empty");
    frames
}

fn bench_parse(c: &mut Criterion) {
    let frames = load_frames();
    let bytes_frames = load_bytes();

    let mut group = c.benchmark_group("parse");
    group.throughput(Throughput::Elements(frames.len() as u64));

    // Full parse: header + both level vectors decoded to fixed-point (the path
    // the live feed actually uses).
    group.bench_function("full", |b| {
        b.iter(|| {
            for (_recv_ts_ns, frame) in &frames {
                black_box(parse(black_box(frame)).unwrap());
            }
        })
    });

    let messages = split_messages(&bytes_frames).unwrap();
    group.throughput(Throughput::Elements(messages.len() as u64));

    group.bench_function("sbe", |b| {
        b.iter(|| {
            for message in &messages {
                let mut d = SbeDecoder::new(black_box(*message));
                let header = decode_header(&mut d).unwrap();
                let decoded = decode_message(&mut d, &header).unwrap();
                black_box(decoded);
            }
        });
    });

    group.finish();
}

fn split_messages(mut bytes: &[u8]) -> Result<Vec<&[u8]>, &'static str> {
    let mut messages = Vec::new();

    while !bytes.is_empty() {
        if bytes.len() < 4 {
            return Err("truncated message length");
        }

        let len = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
        bytes = &bytes[4..];

        if len == 0 || len > bytes.len() {
            return Err("invalid message length");
        }

        let (message, rest) = bytes.split_at(len);
        messages.push(message);
        bytes = rest;
    }

    Ok(messages)
}

fn load_bytes() -> Vec<u8> {
    let path = std::env::var("BINANCE_SBE").unwrap_or_else(|_| DEFAULT_SBE.into());

    std::fs::read(&path).unwrap_or_else(|e| {
        panic!(
            "cannot read capture file '{path}': {e}\n\
             capture one first: BINANCE_CAPTURE={path} cargo run -p clob-binance"
        )
    })
}

criterion_group!(benches, bench_parse);
criterion_main!(benches);
