use async_trait::async_trait;
use clob_venue::VenueError;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, Instant};
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::model::BinanceSnapshot;
use crate::{FrameSource, SnapshotSource};

#[derive(Default, Clone, Copy)]
pub enum Speed {
    #[default]
    One,
    Nx(u64),
    Max,
}

impl FromStr for Speed {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "one" | "1" => Ok(Speed::One),
            "max" => Ok(Speed::Max),
            s if s.starts_with('x') => {
                let n = s[1..]
                    .parse::<u64>()
                    .map_err(|_| format!("invalid speed: {s}"))?;

                Ok(Speed::Nx(n))
            }
            s => Err(format!("invalid speed: {s}")),
        }
    }
}

#[derive(Clone, Copy)]
pub struct ReplayClock {
    pub started_at: Instant,
    pub replay_offset_nanos: u64,
    speed: Speed,
}

impl ReplayClock {
    pub fn new(offset_ns: u64, started_at: Instant, speed: Speed) -> Self {
        Self {
            started_at,
            replay_offset_nanos: offset_ns,
            speed,
        }
    }

    pub async fn wait_until(&self, replay_ns: u64) {
        let elapsed = self.started_at.elapsed();

        let target = match &self.speed {
            Speed::One => Duration::from_nanos(replay_ns),
            Speed::Nx(n) => Duration::from_nanos(replay_ns / *n),
            Speed::Max => return,
        };

        if elapsed < target {
            tokio::time::sleep(target - elapsed).await;
        }
    }
}

pub struct Replay {
    rx: mpsc::Receiver<Result<String, VenueError>>,
    _task: JoinHandle<()>,
}

impl Replay {
    pub async fn open(path: impl AsRef<Path>, clock: ReplayClock) -> std::io::Result<Self> {
        let file = File::open(path.as_ref()).await?;

        let (tx, rx) = mpsc::channel::<Result<String, VenueError>>(1024);

        let task = tokio::spawn(async move {
            let mut reader = BufReader::new(file);
            let mut line = String::new();

            loop {
                line.clear();
                match reader.read_line(&mut line).await {
                    Ok(0) => break, // EOF: dropping `tx` closes the channel.
                    Ok(_) => {}
                    Err(e) => {
                        let _ = tx.send(Err(VenueError::Connection(e.to_string()))).await;
                        break;
                    }
                }

                let trimmed = line.trim_end_matches(['\r', '\n']);
                let Some((ts, json)) = trimmed.split_once(' ') else {
                    let _ = tx
                        .send(Err(VenueError::Connection(
                            "invalid replay line frame".into(),
                        )))
                        .await;
                    break;
                };
                let ts: u64 = match ts.parse() {
                    Ok(ts) => ts,
                    Err(e) => {
                        let _ = tx
                            .send(Err(VenueError::Connection(format!(
                                "invalid replay timestamp: {e}"
                            ))))
                            .await;
                        break;
                    }
                };

                clock.wait_until(ts).await;

                if tx.send(Ok(json.to_owned())).await.is_err() {
                    break;
                }
            }
        });

        Ok(Self { rx, _task: task })
    }
}
#[async_trait]
impl FrameSource for Replay {
    async fn connect(&mut self) -> Result<(), VenueError> {
        Ok(())
    }

    async fn next(&mut self) -> Result<String, VenueError> {
        match self.rx.recv().await {
            Some(res) => res,
            None => Err(VenueError::ReplayEof),
        }
    }
}

#[derive(Clone)]
pub struct ReplaySnap {
    path: PathBuf,
    clock: ReplayClock,
}

impl ReplaySnap {
    pub fn open(path: impl Into<PathBuf>, clock: ReplayClock) -> Self {
        Self {
            path: path.into(),
            clock,
        }
    }
}

#[async_trait]
impl SnapshotSource for ReplaySnap {
    async fn pull(&self, symbol: &str) -> Result<(BinanceSnapshot, String), VenueError> {
        let raw = tokio::fs::read_to_string(&self.path)
            .await
            .map_err(|e| VenueError::Snapshot(e.to_string()))?;

        let (ts, snap) = raw
            .split_once(' ')
            .ok_or_else(|| VenueError::Connection("invalid replay line snap".into()))?;

        let ts: u64 = ts
            .parse()
            .map_err(|e| VenueError::Connection(format!("invalid replay timestamp: {e}")))?;

        self.clock.wait_until(ts).await;
        let snapshot = serde_json::from_str::<BinanceSnapshot>(snap)
            .map_err(|e| VenueError::Snapshot(e.to_string()))?;

        Ok((snapshot, raw))
    }
}
