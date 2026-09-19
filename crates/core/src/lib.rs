use std::collections::BTreeMap;
use std::ops::Sub;

use sha2::{Digest, Sha256};

pub mod traits;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Bid,
    Ask,
}

pub const SCALE_DECIMALS: u32 = 8;
pub const SCALE: i64 = 10_i64.pow(SCALE_DECIMALS);

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Price(pub i64);

impl Price {
    pub fn to_f64(self) -> f64 {
        self.0 as f64 / SCALE as f64
    }
}

impl Sub for Price {
    type Output = Price;

    fn sub(self, rhs: Price) -> Price {
        Price(self.0 - rhs.0)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Size(pub u64);

impl Size {
    /// Decode to a human-readable value (display only).
    pub fn to_f64(self) -> f64 {
        self.0 as f64 / SCALE as f64
    }
}

#[derive(Debug, Default)]
pub struct OrderBook {
    bids: BTreeMap<Price, Size>,
    asks: BTreeMap<Price, Size>,
}

impl OrderBook {
    pub fn state_hash(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();

        hasher.update(b"bids");

        for (price, size) in &self.bids {
            hasher.update(price.0.to_le_bytes());
            hasher.update(b":");
            hasher.update(size.0.to_le_bytes());
            hasher.update(b";");
        }

        hasher.update(b"asks");

        for (price, size) in &self.asks {
            hasher.update(price.0.to_le_bytes());
            hasher.update(b":");
            hasher.update(size.0.to_le_bytes());
            hasher.update(b";");
        }

        hasher.finalize().into()
    }

    pub fn top_n(&self, n: usize) -> (Vec<(Price, Size)>, Vec<(Price, Size)>) {
        let bids = self
            .bids
            .iter()
            .rev()
            .take(n)
            .map(|(p, s)| (*p, *s))
            .collect();

        let asks = self.asks.iter().take(n).map(|(p, s)| (*p, *s)).collect();

        (bids, asks)
    }

    pub fn bids(&self) -> impl DoubleEndedIterator<Item = (&Price, &Size)> {
        self.bids.iter()
    }

    pub fn asks(&self) -> impl DoubleEndedIterator<Item = (&Price, &Size)> {
        self.asks.iter()
    }

    pub fn upsert(&mut self, side: Side, price: Price, size: Size) {
        let levels = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };

        if size.0 == 0 {
            levels.remove(&price);
        } else {
            levels.insert(price, size);
        }
    }

    pub fn best_bid(&self) -> Option<(&Price, &Size)> {
        self.bids.last_key_value()
    }

    pub fn best_ask(&self) -> Option<(&Price, &Size)> {
        self.asks.first_key_value()
    }

    pub fn spread(&self) -> Option<Price> {
        let ba = self.best_ask()?;
        let bb = self.best_bid()?;

        Some(*ba.0 - *bb.0)
    }

    pub fn clear_side(&mut self, side: Side) {
        match side {
            Side::Bid => self.bids.clear(),
            Side::Ask => self.asks.clear(),
        };
    }

    pub fn replace_bids(&mut self, bids: &[(Price, Size)]) {
        replace_side(&mut self.bids, bids);
    }

    pub fn replace_asks(&mut self, asks: &[(Price, Size)]) {
        replace_side(&mut self.asks, asks);
    }
}

fn replace_side(side: &mut BTreeMap<Price, Size>, levels: &[(Price, Size)]) {
    side.clear();
    side.extend(levels.iter().copied().filter(|(_, s)| s.0 != 0));
}

#[cfg(test)]
mod tests {
    use std::assert_eq;

    use crate::{OrderBook, Price, Side, Size};

    #[test]
    fn new_level_created() {
        let mut ob = OrderBook::default();
        ob.upsert(Side::Bid, Price(1), Size(2));
        assert_eq!(*ob.best_bid().unwrap().0, Price(1));
    }

    #[test]
    fn update_level() {
        let mut ob = OrderBook::default();
        ob.upsert(Side::Bid, Price(1), Size(2));
        ob.upsert(Side::Bid, Price(1), Size(10));
        assert_eq!(*ob.best_bid().unwrap().1, Size(10));
    }

    #[test]
    fn rm_level_with_zero_size() {
        let mut ob = OrderBook::default();
        ob.upsert(Side::Bid, Price(1), Size(2));
        ob.upsert(Side::Bid, Price(1), Size(0));
        assert!(ob.best_bid().is_none())
    }

    #[test]
    fn empty_book() {
        let ob = OrderBook::default();
        assert!(ob.best_bid().is_none());
        assert!(ob.best_ask().is_none());
    }

    #[test]
    fn best_bid() {
        let mut ob = OrderBook::default();
        ob.upsert(Side::Bid, Price(1), Size(2));
        ob.upsert(Side::Bid, Price(2), Size(1));
        ob.upsert(Side::Bid, Price(10), Size(5));
        assert_eq!(*ob.best_bid().unwrap().0, Price(10));
    }

    #[test]
    fn best_ask() {
        let mut ob = OrderBook::default();
        ob.upsert(Side::Ask, Price(1), Size(2));
        ob.upsert(Side::Ask, Price(2), Size(1));
        ob.upsert(Side::Ask, Price(10), Size(5));
        assert_eq!(*ob.best_ask().unwrap().0, Price(1));
    }

    #[test]
    fn spread() {
        let mut ob = OrderBook::default();
        ob.upsert(Side::Ask, Price(4), Size(2));
        ob.upsert(Side::Bid, Price(2), Size(2));
        assert_eq!(ob.spread().unwrap(), Price(2));
    }

    use proptest::{prop_assert_eq, proptest};
    proptest! {
        #[test]
        fn best_bid_is_max(prices in proptest::collection::vec(0i64..1000, 1..50)) {
            let mut book = OrderBook::default();
            for p in &prices {
                book.upsert(Side::Bid, Price(*p), Size(1));
            }

            let expected = prices.iter().max().unwrap();
            prop_assert_eq!(book.best_bid().unwrap().0.0, *expected);
        }
    }

    proptest! {
        #[test]
        fn best_ask_is_min(prices in proptest::collection::vec(0i64..1000, 1..50)) {
            let mut book = OrderBook::default();
            for p in &prices {
                book.upsert(Side::Ask, Price(*p), Size(1));
            }

            let expected = prices.iter().min().unwrap();
            prop_assert_eq!(book.best_ask().unwrap().0.0, *expected);
        }
    }
}
