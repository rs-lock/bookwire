use std::time::Instant;

use async_trait::async_trait;
use clob_core::{OrderBook, Price, Side, Size};

#[async_trait]
pub trait Venue: Send + Sync {
    async fn connect(&mut self) -> Result<(), VenueError>;
    async fn next_updates(&mut self) -> Result<VenueEvent, VenueError>;
    fn name(&self) -> &str;

    /// Read the next update and apply it straight to book
    async fn next_into(&mut self, book: &mut OrderBook) -> Result<VenueEvent, VenueError> {
        let update = self.next_updates().await?;
        update.venue_update.apply(book);
        Ok(update)
    }
}

pub struct VenueEvent {
    pub venue_update: VenueBookUpdate,
    pub timing: Option<UpdateTiming>,
    pub exchange_timing: Option<ExchangeTiming>,
}

pub struct UpdateTiming {
    pub frame_ready_at: Instant,
    pub parsed_at: Instant,
    pub frame_bytes: usize,
    pub update_count: usize,
}

pub struct ExchangeTiming {
    pub event_time: u64,
    pub tx_time: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BookUpdate {
    pub side: Side,
    pub price: Price,
    pub size: Size,
}

#[derive(Debug)]
pub enum VenueBookUpdate {
    Snapshot {
        bids: Vec<(Price, Size)>,
        asks: Vec<(Price, Size)>,
    },

    Delta(Vec<BookUpdate>),

    /// The venue lost sequence continuity and can no longer vouch for the book
    Invalidate,
}

impl VenueBookUpdate {
    pub fn apply(&self, book: &mut OrderBook) {
        match self {
            VenueBookUpdate::Snapshot { bids, asks } => {
                book.replace_asks(asks);
                book.replace_bids(bids);
            }
            VenueBookUpdate::Delta(book_update) => {
                book_update
                    .iter()
                    .for_each(|b_u| book.upsert(b_u.side, b_u.price, b_u.size));
            }
            VenueBookUpdate::Invalidate => {
                book.clear_side(Side::Bid);
                book.clear_side(Side::Ask);
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum VenueError {
    #[error("connection failed: {0}")]
    Connection(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("snapshot error: {0}")]
    Snapshot(String),
    #[error("ReplayEof")]
    ReplayEof,
}

#[cfg(test)]
mod tests {

    use crate::{BookUpdate, VenueBookUpdate};
    use clob_core::{OrderBook, Price, Side, Size};

    #[test]
    fn snapshot_fills_book() {
        let mut book = OrderBook::default();
        let upd = VenueBookUpdate::Snapshot {
            bids: vec![(Price(100), Size(5)), (Price(99), Size(3))],
            asks: vec![(Price(101), Size(2))],
        };
        upd.apply(&mut book);
        assert_eq!(book.best_bid().unwrap().0, &Price(100));
        assert_eq!(book.best_ask().unwrap().0, &Price(101));
    }

    #[test]
    fn snapshot_evicts_stale_levels() {
        let mut book = OrderBook::default();
        VenueBookUpdate::Snapshot {
            bids: vec![(Price(100), Size(5)), (Price(99), Size(3))],
            asks: vec![],
        }
        .apply(&mut book);

        VenueBookUpdate::Snapshot {
            bids: vec![(Price(100), Size(5))],
            asks: vec![],
        }
        .apply(&mut book);

        assert_eq!(book.bids().count(), 1);
    }

    #[test]
    fn delta_fills_book() {
        let mut book = OrderBook::default();
        VenueBookUpdate::Delta(vec![BookUpdate {
            side: Side::Bid,
            price: Price(100),
            size: Size(5),
        }])
        .apply(&mut book);
        assert_eq!(book.best_bid().unwrap().1, &Size(5));
    }
}
