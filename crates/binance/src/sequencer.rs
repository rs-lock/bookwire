use std::collections::VecDeque;

#[derive(Default)]
pub struct Sequencer {
    state: State,
    /// Final update id of the last *applied* delta. Meaningful only in `Live`;
    /// established by the snapshot splice, then advanced on each applied delta.
    last_u: Option<u64>,
    /// Deltas held verbatim during buffering, spliced against the snapshot once
    /// it arrives. Their `U`/`u` ids ride alongside the raw bytes so the splice
    /// never has to re-parse JSON.
    buffer: VecDeque<Buffered>,
    resync_count: u64,
}

/// A buffered delta: its sequence window plus the raw frame to replay at splice
/// time. `first_u`/`final_u` are the event's `U`/`u`.
struct Buffered {
    first_u: u64,
    final_u: u64,
    raw: String,
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// Just constructed / not collecting yet.
    #[default]
    Connecting,
    /// Collecting deltas while waiting for (or re-fetch) the REST snapshot.
    /// This is also the recovery phase after a gap — there is no separate
    /// `Resync` state because it would behave identically.
    Buffering,
    /// Book is consistent; deltas are applied in order.
    Live,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Not collecting yet — drop it.
    Ignore,
    /// Stashed in the buffer; do nothing until the snapshot arrives.
    Buffer,
    /// In sequence — the venue should decode this delta's levels into the book.
    Apply,
    /// Sequence gap — the book is no longer trustworthy. The venue must drop the
    /// book and (re)fetch a snapshot; the sequencer has already reset itself to
    /// buffering.
    Invalidate,
}

/// Verdict of splicing the buffered deltas against a REST snapshot.
#[derive(Debug, PartialEq, Eq)]
pub enum Splice {
    /// The snapshot bridged onto the buffer. Build the book from the snapshot,
    /// then decode and apply these frames in order — we are now `Live`.
    Live(Vec<String>),
    /// The snapshot predates the buffer (a gap sits between them): its
    /// `lastUpdateId` never reaches the first buffered delta. Fetch a newer
    /// snapshot; we stay `Buffering`.
    Retry,
    /// The snapshot is newer than everything buffered so far — no delta yet spans
    /// its `lastUpdateId`. Keep the same snapshot, wait for more deltas, and
    /// splice again. (Refetching here would only move the target further away.)
    NeedMore,
}

impl Sequencer {
    /// Enter (or reenter) the buffering phase. Called once after subscribing and
    /// again on recovery. Clears any prior progress; the caller must fetch a
    /// snapshot next.
    pub fn begin(&mut self) {
        self.state = State::Buffering;
        self.last_u = None;
        self.buffer.clear();
    }

    /// Judge one delta by its ids. `first_u`/`final_u` are the event's `U`/`u`;
    /// `prev_u` is its `pu` (previous final update id, futures-only). `raw` is
    /// stored verbatim only when we buffer — otherwise it is untouched.
    pub fn on_delta(&mut self, first_u: u64, final_u: u64, prev_u: u64, raw: &str) -> Outcome {
        match self.state {
            State::Connecting => Outcome::Ignore,

            // Pre-snapshot: buffer everything
            State::Buffering => {
                self.buffer.push_back(Buffered {
                    first_u,
                    final_u,
                    raw: raw.to_owned(),
                });
                Outcome::Buffer
            }

            // Steady state: each delta's `pu` must chain onto the last applied `u`.
            State::Live => {
                if self.last_u == Some(prev_u) {
                    self.last_u = Some(final_u);
                    Outcome::Apply
                } else {
                    // Gap
                    self.resync_count += 1;
                    tracing::warn!(last = self.last_u, prev = prev_u, "book gap -> resyncing ");
                    self.begin();
                    Outcome::Invalidate
                }
            }
        }
    }

    /// Splice the buffered deltas against a REST snapshot identified by its
    /// `last_update_id`
    pub fn on_snapshot(&mut self, last_update_id: u64) -> Splice {
        // Only meaningful while buffering; a snapshot arriving in any other state
        // is spurious (e.g. a late retry after we already went live).
        if self.state != State::Buffering {
            return Splice::Retry;
        }

        // 1. Discard deltas the snapshot already accounts for.
        while let Some(front) = self.buffer.front() {
            if front.final_u < last_update_id {
                self.buffer.pop_front();
            } else {
                break;
            }
        }

        // 2. Judge the first survivor against the snapshot's edge.
        let Some(front) = self.buffer.front() else {
            // Everything we had is older than the snapshot; wait for the delta
            // that will span `lastUpdateId`, keeping this same snapshot.
            return Splice::NeedMore;
        };

        // Bridge window: U <= lastId+1 <= u.
        if front.first_u <= last_update_id + 1 && last_update_id + 1 <= front.final_u {
            // The buffer is a contiguous run from a single uninterrupted stream,
            // so the last survivor's `u` is where the applied sequence ends.
            self.last_u = self.buffer.back().map(|b| b.final_u);
            self.state = State::Live;
            let frames = self.buffer.drain(..).map(|b| b.raw).collect();
            Splice::Live(frames)
        } else {
            // front.first_u > last_update_id + 1: the buffer starts past the
            // snapshot's edge — deltas were missed in between. A newer snapshot
            // (higher lastUpdateId) will reach into the buffer.
            Splice::Retry
        }
    }

    /// Read-only view of the phase. The venue exposes the book to readers only
    /// while this is `Live`.
    pub fn state(&self) -> State {
        self.state
    }

    /// Number of frames currently buffered (for a bounded-buffer guard / metric).
    pub fn buffered(&self) -> usize {
        self.buffer.len()
    }

    /// Final update id (`u`) of the last applied delta. `None` until the first
    /// splice anchors the sequence. Used by reconciliation tests to stop at the
    /// event that bridges a reference snapshot's `lastUpdateId`.
    pub fn last_u(&self) -> Option<u64> {
        self.last_u
    }

    pub fn resync_count(&self) -> u64 {
        self.resync_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buffers_until_snapshot() {
        let mut s = Sequencer::default();
        s.begin();
        assert_eq!(s.on_delta(1, 10, 0, "{}"), Outcome::Buffer);
        assert_eq!(s.buffered(), 1);
        assert_eq!(s.state(), State::Buffering);
    }

    #[test]
    fn applies_when_chained_in_live() {
        let mut s = Sequencer::default();
        s.begin();
        // simulate going live at u=100 (normally done by the splice)
        s.state = State::Live;
        s.last_u = Some(100);
        assert_eq!(s.on_delta(101, 105, 100, "{}"), Outcome::Apply);
        assert_eq!(s.on_delta(106, 110, 105, "{}"), Outcome::Apply);
    }

    #[test]
    fn gap_in_live_invalidates_and_rebuffers() {
        let mut s = Sequencer::default();
        s.state = State::Live;
        s.last_u = Some(100);
        // pu=104 does not chain onto last_u=100 - gap
        assert_eq!(s.on_delta(105, 108, 104, "{}"), Outcome::Invalidate);
        assert_eq!(s.state(), State::Buffering);
        assert_eq!(s.last_u, None);
        // the gap is counted exactly once (feed-quality metric)
        assert_eq!(s.resync_count(), 1);
    }

    #[test]
    fn buffering_before_snapshot() {
        let mut s = Sequencer::default();
        s.begin();

        let state = s.on_delta(100, 105, 99, "");

        assert_eq!(state, Outcome::Buffer);
    }
    #[test]
    fn snapshot_splice_live() {
        let mut s = Sequencer::default();
        s.begin();

        let state = s.on_delta(100, 105, 99, "");

        assert_eq!(state, Outcome::Buffer);
        let splice = s.on_snapshot(104);

        assert_eq!(splice, Splice::Live(vec!["".into()]));
        assert_eq!(s.state(), State::Live);
    }

    #[test]
    fn stale_snapshot_splice_retry() {
        let mut s = Sequencer::default();
        s.begin();

        let state = s.on_delta(100, 105, 99, "");

        assert_eq!(state, Outcome::Buffer);
        let splice = s.on_snapshot(98);

        assert_eq!(splice, Splice::Retry);
        assert_eq!(s.state(), State::Buffering);
        assert_eq!(s.buffered(), 1);
    }

    #[test]
    fn snapshot_splice_needmore() {
        let mut s = Sequencer::default();
        s.begin();

        let state = s.on_delta(100, 105, 99, "");

        assert_eq!(state, Outcome::Buffer);
        let splice = s.on_snapshot(106);

        assert_eq!(splice, Splice::NeedMore);
        assert_eq!(s.state(), State::Buffering);
        assert_eq!(s.buffered(), 0);
    }

    #[test]
    fn diverged_delta_invalidate() {
        let mut s = Sequencer::default();
        s.begin();

        let state = s.on_delta(100, 105, 99, "");

        assert_eq!(state, Outcome::Buffer);
        let splice = s.on_snapshot(104);
        assert_eq!(splice, Splice::Live(vec!["".into()]));

        let inv_state = s.on_delta(1, 10, 9, "");

        assert_eq!(inv_state, Outcome::Invalidate);
        assert_eq!(s.state(), State::Buffering);
        assert_eq!(s.buffered(), 0);
    }

    #[test]
    fn recovery_live_after_stale_snapshot() {
        let mut s = Sequencer::default();
        s.begin();

        let state = s.on_delta(100, 105, 99, "");

        assert_eq!(state, Outcome::Buffer);
        let splice = s.on_snapshot(98);
        assert_eq!(splice, Splice::Retry);

        let splice = s.on_snapshot(103);

        assert_eq!(splice, Splice::Live(vec!["".into()]));
        assert_eq!(s.state(), State::Live);
        assert_eq!(s.buffered(), 0);
    }
}
