pub mod live;
pub mod replay;

use async_trait::async_trait;
use clob_venue::VenueError;

use crate::model::BinanceSnapshot;
use crate::{FrameSource, SnapshotSource};
use live::{BinanceRest, BinanceWs};
use replay::{Replay, ReplaySnap};

pub enum Frames {
    Live(BinanceWs),
    Replay(Replay),
}

#[async_trait]
impl FrameSource for Frames {
    async fn connect(&mut self) -> Result<(), VenueError> {
        match self {
            Frames::Live(s) => s.connect().await,
            Frames::Replay(s) => s.connect().await,
        }
    }

    async fn next(&mut self) -> Result<String, VenueError> {
        match self {
            Frames::Live(s) => s.next().await,
            Frames::Replay(s) => s.next().await,
        }
    }
}

#[derive(Clone)]
pub enum Snaps {
    Live(BinanceRest),
    Replay(ReplaySnap),
}

#[async_trait]
impl SnapshotSource for Snaps {
    async fn pull(&self, symbol: &str) -> Result<(BinanceSnapshot, String), VenueError> {
        match self {
            Snaps::Live(s) => s.pull(symbol).await,
            Snaps::Replay(s) => s.pull(symbol).await,
        }
    }
}
