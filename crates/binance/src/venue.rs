use std::collections::VecDeque;
use std::time::Instant;

use async_trait::async_trait;
use clob_core::{Price, Side, Size};
use clob_venue::{BookUpdate, UpdateTiming, Venue, VenueBookUpdate, VenueError, VenueEvent};
use tokio::task::JoinHandle;

use crate::capture::Capture;
use crate::model::{BinanceSnapshot, Depth, Frame};
use crate::sequencer::{Outcome, Sequencer, Splice, State};
use crate::{FrameSource, SnapshotSource};

pub fn parse(json: &str) -> Result<Frame, serde_json::Error> {
    serde_json::from_str(json)
}

pub struct Binance<F, S> {
    frame_source: F,
    snapshot_source: S,
    symbol: String,
    sequencer: Sequencer,
    pending: VecDeque<VenueBookUpdate>,
    /// Optional raw-frame recorder for building a criterion replay sample. `None`
    /// unless `BINANCE_CAPTURE` is set; see [`crate::capture`].
    capture: Option<Capture>,
}

impl<F, S> Binance<F, S> {
    pub fn new(symbol: impl Into<String>, frame_source: F, snapshot_source: S) -> Self {
        Self {
            symbol: symbol.into(),
            sequencer: Sequencer::default(),
            pending: VecDeque::new(),
            capture: Capture::from_env(),
            frame_source,
            snapshot_source,
        }
    }

    /// Final update id (`u`) of the last applied delta; `None` until Live.
    /// Lets a reconciliation test stop at the event bridging a reference
    /// snapshot's `lastUpdateId`.
    pub fn last_u(&self) -> Option<u64> {
        self.sequencer.last_u()
    }
}

#[async_trait]
impl<F, S> Venue for Binance<F, S>
where
    S: SnapshotSource + Send + Sync + Clone,
    F: FrameSource + Send + Sync,
{
    async fn connect(&mut self) -> Result<(), VenueError> {
        self.frame_source.connect().await?;
        // Enter the buffering phase; the snapshot is fetched lazily in the loop
        self.sequencer.begin();
        Ok(())
    }

    async fn next_updates(&mut self) -> Result<VenueEvent, VenueError> {
        loop {
            // 1. Flush anything a prior splice queued up (snapshot + deltas)
            if let Some(out) = self.pending.pop_front() {
                return Ok(VenueEvent {
                    venue_update: out,
                    timing: None,
                });
            }

            match self.sequencer.state() {
                // 2. Steady state: read one delta and judge it by sequence.
                State::Live => {
                    let raw = self.frame_source.next().await?;

                    // Record the raw frame (outside the parse timing) for replay.
                    if let Some(cap) = self.capture.as_mut()
                        && let Err(e) = cap.record(&raw)
                    {
                        tracing::warn!(error = %e, "capture write failed");
                    }

                    let frame_ready_at = Instant::now();
                    let Ok(frame) = parse(&raw) else { continue };
                    let parsed_at = Instant::now();

                    let d = frame.data;
                    match self
                        .sequencer
                        .on_delta(d.first_u, d.final_u, d.prev_u, &raw)
                    {
                        Outcome::Apply => {
                            return Ok(VenueEvent {
                                venue_update: delta_update(&d),
                                timing: Some(UpdateTiming {
                                    frame_ready_at,
                                    parsed_at,
                                }),
                            });
                        }
                        Outcome::Invalidate => {
                            metrics::counter!("resync_events").increment(1);
                            return Ok(VenueEvent {
                                venue_update: VenueBookUpdate::Invalidate,
                                timing: None,
                            });
                        }
                        Outcome::Buffer | Outcome::Ignore => continue,
                    }
                }

                // 3. Buffering / recovery: splice a fresh snapshot onto the
                //    buffered deltas, then loop to flush the queued output.
                State::Buffering => self.buffer_until_live().await?,

                // Shouldn't happen after `connect`, but recover gracefully.
                State::Connecting => self.sequencer.begin(),
            }
        }
    }

    fn name(&self) -> &str {
        "binance"
    }
}

impl<F, S> Binance<F, S>
where
    S: SnapshotSource + Send + Sync + Clone,
    F: FrameSource + Send + Sync,
{
    /// Drive the buffering phase to completion: fetch the REST snapshot while
    /// concurrently reading WS frames into the buffer (so no delta is missed),
    /// then splice and enqueue the resulting book updates. Returns once we are
    /// `Live` with `self.pending` populated.
    async fn buffer_until_live(&mut self) -> Result<(), VenueError> {
        // Snapshot in flight; owns its input so it needn't borrow `self`.
        let symbol = format!("{}USDT", self.symbol);

        let mut snap_task = spawn_snapshot(&self.snapshot_source, &symbol);
        // A snapshot that arrived but is still ahead of the buffer (`NeedMore`):
        // held here and re-spliced as fresh deltas land.
        let mut pending_snap: Option<BinanceSnapshot> = None;
        // A replay frame source is a finite file; reaching its end is not a fatal
        // disconnect
        let mut frames_eof = false;

        loop {
            if frames_eof && pending_snap.is_some() {
                return Err(VenueError::ReplayEof);
            }

            tokio::select! {

                joined = &mut snap_task, if pending_snap.is_none() => {
                    let snap = joined
                        .map_err(|e| VenueError::Snapshot(e.to_string()))??;

                    if let Some(cap) = self.capture.as_mut()
                        && let Err(e) = cap.record_snap(&snap.1) {
                        tracing::warn!(error = %e, "capture write failed");
                    }


                    match self.sequencer.on_snapshot(snap.0.last_u) {
                        Splice::Live(frames) => {
                            self.enqueue_splice(&snap.0, frames);
                            return Ok(());
                        }

                        Splice::Retry if frames_eof => return Err(VenueError::ReplayEof),
                        Splice::Retry => snap_task = spawn_snapshot(&self.snapshot_source, &self.symbol),
                        // Snapshot ahead of the buffer: keep it, wait for deltas
                        Splice::NeedMore => pending_snap = Some(snap.0),
                    }
                }

                raw = self.frame_source.next(), if !frames_eof => {
                    let raw = match raw {
                        Ok(raw) => raw,
                        Err(VenueError::ReplayEof) => {
                            frames_eof = true;
                            continue;
                        }
                        Err(e) => return Err(e),
                    };


                    if let Some(cap) = self.capture.as_mut()
                        && let Err(e) = cap.record(&raw)
                    {
                        tracing::warn!(error = %e, "capture write failed");
                    }

                    let Ok(frame) = parse(&raw) else { continue };

                    let d = frame.data;
                    self.sequencer.on_delta(d.first_u, d.final_u, d.prev_u, &raw);

            
                    if let Some(snap) = pending_snap.take() {
                        match self.sequencer.on_snapshot(snap.last_u) {
                            Splice::Live(frames) => {
                                self.enqueue_splice(&snap, frames);
                                return Ok(());
                            }
                            Splice::Retry => snap_task =  spawn_snapshot(&self.snapshot_source, &self.symbol),
                            Splice::NeedMore => pending_snap = Some(snap),
                        }
                    }
                }
            }
        }
    }

    fn enqueue_splice(&mut self, snap: &BinanceSnapshot, frames: Vec<String>) {
        self.pending.push_back(snapshot_update(snap));
        for raw in frames {
            if let Ok(frame) = parse(&raw) {
                self.pending.push_back(delta_update(&frame.data));
            }
        }
    }
}

fn spawn_snapshot<S: SnapshotSource + Clone>(
    source: &S,
    symbol: &str,
) -> JoinHandle<Result<(BinanceSnapshot, String), VenueError>> {
    let sym = symbol.to_owned();

    let source = source.clone();
    tokio::spawn(async move { source.pull(&sym).await })
}

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

fn delta_update(d: &Depth) -> VenueBookUpdate {
    let mut updates: Vec<BookUpdate> = Vec::with_capacity(d.bids.len() + d.asks.len());

    updates.extend(d.bids.iter().map(|level| BookUpdate {
        side: Side::Bid,
        price: Price(level.0.0),
        size: Size(level.1.0),
    }));

    updates.extend(d.asks.iter().map(|level| BookUpdate {
        side: Side::Ask,
        price: Price(level.0.0),
        size: Size(level.1.0),
    }));

    VenueBookUpdate::Delta(updates)
}
