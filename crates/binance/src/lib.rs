use async_trait::async_trait;
use clob_venue::VenueError;

use crate::model::BinanceSnapshot;

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

pub mod capture;
pub mod model;
pub mod sbe;
mod sequencer;
pub mod sources;
pub mod venue;

#[async_trait]
pub trait FrameSource {
    async fn connect(&mut self) -> Result<(), VenueError>;
    async fn next(&mut self) -> Result<String, VenueError>;
}

#[async_trait]
pub trait SnapshotSource: Send + Sync + 'static {
    async fn pull(&self, symbol: &str) -> Result<(BinanceSnapshot, String), VenueError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);

        assert_eq!(result, 4);
    }
}
