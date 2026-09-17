//! Raw-frame capture: append WS text frames verbatim to a file, one JSON object
//! per line (JSONL), so they can later be replayed through [`crate::feed::parse`]
//! in a criterion benchmark. Off by default; enabled by setting `BINANCE_CAPTURE`
//! to an output path (see [`Capture::from_env`]).

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::Path;

use tokio::time::Instant;

const DEFAULT_LIMIT: usize = 50000;

pub struct Capture {
    writer: BufWriter<File>,
    snap_writer: BufWriter<File>,
    remaining: usize,
    started: Instant,
}

impl Capture {
    pub fn create(
        path: impl AsRef<Path>,
        snap_path: impl AsRef<Path>,
        limit: usize,
    ) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        let snap_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(snap_path)?;

        Ok(Self {
            writer: BufWriter::new(file),
            snap_writer: BufWriter::new(snap_file),
            remaining: limit,
            started: Instant::now(),
        })
    }

    pub fn from_env() -> Option<Self> {
        let path = std::env::var("BINANCE_CAPTURE").ok()?;
        let snap_path = std::env::var("BINANCE_SNAP").ok()?;
        let limit = std::env::var("BINANCE_CAPTURE_N")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_LIMIT);
        match Self::create(&path, &snap_path, limit) {
            Ok(cap) => {
                tracing::info!(path = %path, limit, "capturing raw frames");
                Some(cap)
            }
            Err(e) => {
                tracing::warn!(path = %path, error = %e, "capture disabled: cannot open file");
                None
            }
        }
    }

    /// Record one raw frame. `raw` must be newline-free (Binance frames are a
    /// single line of JSON) so that one line maps to exactly one frame on replay.
    /// Returns `true` while still capturing, `false` once the limit is reached.
    pub fn record(&mut self, raw: &str) -> io::Result<bool> {
        if self.remaining == 0 {
            return Ok(false);
        }
        let timestamp = self.started.elapsed().as_nanos() as u64;

        write!(self.writer, "{} ", timestamp)?;
        self.writer.write_all(raw.as_bytes())?;
        self.writer.write_all(b"\n")?;
        self.remaining -= 1;
        if self.remaining == 0 {
            // Sample complete — flush so the file is usable immediately.
            self.writer.flush()?;
            tracing::info!("capture complete");
        }
        Ok(true)
    }

    pub fn record_snap(&mut self, raw: &str) -> io::Result<()> {
        let timestamp = self.started.elapsed().as_nanos() as u64;
        write!(self.snap_writer, "{} ", timestamp)?;
        self.snap_writer.write_all(raw.as_bytes())?;
        self.snap_writer.write_all(b"\n")?;
        self.snap_writer.flush()?;
        Ok(())
    }

    /// True once the configured number of frames has been written.
    pub fn is_done(&self) -> bool {
        self.remaining == 0
    }
}
