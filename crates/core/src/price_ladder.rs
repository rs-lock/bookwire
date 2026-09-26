use std::mem::size_of;

use crate::{Price, Side, Size};

/// A dense, allocation-free-on-update L2 order book over a fixed price range.
///
/// Prices are converted to array indexes with
/// `(price - base_price) / tick_size`. A bitmap per side makes finding the next
/// occupied level independent of the number of empty price slots.
#[derive(Debug)]
pub struct PriceLadder {
    base_price: Price,
    max_price: Price,
    tick_size: i64,
    capacity: usize,
    bids: LadderSide,
    asks: LadderSide,
}

#[derive(Debug)]
struct LadderSide {
    quantities: Box<[u64]>,
    occupied: Box<[u64]>,
    best: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LadderUpdate {
    Applied,
    OutOfRange,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PriceLadderError {
    #[error("tick size must be positive")]
    InvalidTickSize,
    #[error("price ladder capacity must be positive")]
    InvalidCapacity,
    #[error("invalid price range {min:?}..={max:?}")]
    InvalidRange { min: Price, max: Price },
    #[error("price range does not fit in memory")]
    CapacityOverflow,
    #[error("price {price:?} is not aligned to tick size {tick_size}")]
    MisalignedPrice { price: Price, tick_size: i64 },
}

impl PriceLadder {
    pub fn new(
        base_price: Price,
        tick_size: i64,
        capacity: usize,
    ) -> Result<Self, PriceLadderError> {
        if tick_size <= 0 {
            return Err(PriceLadderError::InvalidTickSize);
        }
        if capacity == 0 {
            return Err(PriceLadderError::InvalidCapacity);
        }

        let steps = i64::try_from(capacity - 1).map_err(|_| PriceLadderError::CapacityOverflow)?;
        let max_price = steps
            .checked_mul(tick_size)
            .and_then(|span| base_price.0.checked_add(span))
            .map(Price)
            .ok_or(PriceLadderError::CapacityOverflow)?;

        let words = capacity
            .checked_add(63)
            .ok_or(PriceLadderError::CapacityOverflow)?
            / 64;
        let make_side = || LadderSide {
            quantities: vec![0; capacity].into_boxed_slice(),
            occupied: vec![0; words].into_boxed_slice(),
            best: None,
        };

        Ok(Self {
            base_price,
            max_price,
            tick_size,
            capacity,
            bids: make_side(),
            asks: make_side(),
        })
    }

    /// Allocate the smallest ladder that covers both endpoint prices.
    pub fn covering(
        min_price: Price,
        max_price: Price,
        tick_size: i64,
    ) -> Result<Self, PriceLadderError> {
        if tick_size <= 0 {
            return Err(PriceLadderError::InvalidTickSize);
        }
        if max_price < min_price {
            return Err(PriceLadderError::InvalidRange {
                min: min_price,
                max: max_price,
            });
        }

        let span = max_price
            .0
            .checked_sub(min_price.0)
            .ok_or(PriceLadderError::CapacityOverflow)?;
        if span % tick_size != 0 {
            return Err(PriceLadderError::MisalignedPrice {
                price: max_price,
                tick_size,
            });
        }
        let capacity = usize::try_from(span / tick_size)
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or(PriceLadderError::CapacityOverflow)?;

        Self::new(min_price, tick_size, capacity)
    }

    #[inline]
    pub fn upsert(
        &mut self,
        side: Side,
        price: Price,
        size: Size,
    ) -> Result<LadderUpdate, PriceLadderError> {
        let Some(index) = self.index_of(price)? else {
            return Ok(LadderUpdate::OutOfRange);
        };

        match side {
            Side::Bid => self.bids.upsert_bid(index, size),
            Side::Ask => self.asks.upsert_ask(index, size),
        }
        Ok(LadderUpdate::Applied)
    }

    pub fn clear_side(&mut self, side: Side) {
        let side = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        side.quantities.fill(0);
        side.occupied.fill(0);
        side.best = None;
    }

    #[inline]
    pub fn best_bid(&self) -> Option<(Price, Size)> {
        self.bids.best.map(|index| self.level(Side::Bid, index))
    }

    #[inline]
    pub fn best_ask(&self) -> Option<(Price, Size)> {
        self.asks.best.map(|index| self.level(Side::Ask, index))
    }

    /// A zero-copy, read-only view suitable for a strategy.
    pub fn view(&self) -> PriceLadderView<'_> {
        PriceLadderView { ladder: self }
    }

    pub fn base_price(&self) -> Price {
        self.base_price
    }

    pub fn max_price(&self) -> Price {
        self.max_price
    }

    pub fn tick_size(&self) -> i64 {
        self.tick_size
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Heap memory used by both quantity arrays and both bitmaps.
    pub fn allocated_bytes(&self) -> usize {
        2 * self.capacity * size_of::<u64>() + 2 * self.bids.occupied.len() * size_of::<u64>()
    }

    #[inline]
    fn index_of(&self, price: Price) -> Result<Option<usize>, PriceLadderError> {
        let Some(delta) = price.0.checked_sub(self.base_price.0) else {
            return Ok(None);
        };
        if delta < 0 {
            return Ok(None);
        }
        if delta % self.tick_size != 0 {
            return Err(PriceLadderError::MisalignedPrice {
                price,
                tick_size: self.tick_size,
            });
        }
        let Ok(index) = usize::try_from(delta / self.tick_size) else {
            return Ok(None);
        };
        Ok((index < self.capacity).then_some(index))
    }

    #[inline]
    fn price_at(&self, index: usize) -> Price {
        Price(self.base_price.0 + index as i64 * self.tick_size)
    }

    #[inline]
    fn level(&self, side: Side, index: usize) -> (Price, Size) {
        let quantities = match side {
            Side::Bid => &self.bids.quantities,
            Side::Ask => &self.asks.quantities,
        };
        (self.price_at(index), Size(quantities[index]))
    }
}

impl LadderSide {
    #[inline]
    fn upsert_bid(&mut self, index: usize, size: Size) {
        self.set(index, size);
        if size.0 == 0 {
            if self.best == Some(index) {
                self.best = index.checked_sub(1).and_then(|from| self.find_prev(from));
            }
        } else if self.best.is_none_or(|best| index > best) {
            self.best = Some(index);
        }
    }

    #[inline]
    fn upsert_ask(&mut self, index: usize, size: Size) {
        self.set(index, size);
        if size.0 == 0 {
            if self.best == Some(index) {
                self.best = index.checked_add(1).and_then(|from| self.find_next(from));
            }
        } else if self.best.is_none_or(|best| index < best) {
            self.best = Some(index);
        }
    }

    #[inline]
    fn set(&mut self, index: usize, size: Size) {
        self.quantities[index] = size.0;
        let word = &mut self.occupied[index / 64];
        let mask = 1_u64 << (index % 64);
        if size.0 == 0 {
            *word &= !mask;
        } else {
            *word |= mask;
        }
    }

    fn find_next(&self, from: usize) -> Option<usize> {
        if from >= self.quantities.len() {
            return None;
        }
        let mut word_index = from / 64;
        let mut word = self.occupied[word_index] & (u64::MAX << (from % 64));
        loop {
            if word != 0 {
                let index = word_index * 64 + word.trailing_zeros() as usize;
                return (index < self.quantities.len()).then_some(index);
            }
            word_index += 1;
            word = *self.occupied.get(word_index)?;
        }
    }

    fn find_prev(&self, from: usize) -> Option<usize> {
        if self.quantities.is_empty() {
            return None;
        }
        let from = from.min(self.quantities.len() - 1);
        let mut word_index = from / 64;
        let bit = from % 64;
        let mask = if bit == 63 {
            u64::MAX
        } else {
            (1_u64 << (bit + 1)) - 1
        };
        let mut word = self.occupied[word_index] & mask;
        loop {
            if word != 0 {
                return Some(word_index * 64 + (63 - word.leading_zeros() as usize));
            }
            word_index = word_index.checked_sub(1)?;
            word = self.occupied[word_index];
        }
    }
}

#[derive(Clone, Copy)]
pub struct PriceLadderView<'a> {
    ladder: &'a PriceLadder,
}

impl<'a> PriceLadderView<'a> {
    pub fn best_bid(self) -> Option<(Price, Size)> {
        self.ladder.best_bid()
    }

    pub fn best_ask(self) -> Option<(Price, Size)> {
        self.ladder.best_ask()
    }

    /// Iterate from best price outwards without allocating or copying the book.
    pub fn levels(self, side: Side) -> PriceLevels<'a> {
        let next = match side {
            Side::Bid => self.ladder.bids.best,
            Side::Ask => self.ladder.asks.best,
        };
        PriceLevels {
            ladder: self.ladder,
            side,
            next,
        }
    }
}

pub struct PriceLevels<'a> {
    ladder: &'a PriceLadder,
    side: Side,
    next: Option<usize>,
}

impl Iterator for PriceLevels<'_> {
    type Item = (Price, Size);

    fn next(&mut self) -> Option<Self::Item> {
        let index = self.next?;
        let level = self.ladder.level(self.side, index);
        self.next = match self.side {
            Side::Bid => index
                .checked_sub(1)
                .and_then(|from| self.ladder.bids.find_prev(from)),
            Side::Ask => index
                .checked_add(1)
                .and_then(|from| self.ladder.asks.find_next(from)),
        };
        Some(level)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OrderBook;

    #[test]
    fn best_moves_after_delete() {
        let mut book = PriceLadder::new(Price(100), 10, 16).unwrap();
        book.upsert(Side::Bid, Price(120), Size(2)).unwrap();
        book.upsert(Side::Bid, Price(140), Size(4)).unwrap();
        book.upsert(Side::Ask, Price(180), Size(8)).unwrap();
        book.upsert(Side::Ask, Price(160), Size(6)).unwrap();

        assert_eq!(book.best_bid(), Some((Price(140), Size(4))));
        assert_eq!(book.best_ask(), Some((Price(160), Size(6))));

        book.upsert(Side::Bid, Price(140), Size(0)).unwrap();
        book.upsert(Side::Ask, Price(160), Size(0)).unwrap();
        assert_eq!(book.best_bid(), Some((Price(120), Size(2))));
        assert_eq!(book.best_ask(), Some((Price(180), Size(8))));
    }

    #[test]
    fn view_iterates_from_best_outward() {
        let mut book = PriceLadder::new(Price(100), 10, 16).unwrap();
        for price in [110, 150, 130] {
            book.upsert(Side::Bid, Price(price), Size(price as u64))
                .unwrap();
            book.upsert(Side::Ask, Price(price), Size(price as u64))
                .unwrap();
        }

        let bids: Vec<_> = book.view().levels(Side::Bid).collect();
        let asks: Vec<_> = book.view().levels(Side::Ask).collect();
        assert_eq!(
            bids.iter().map(|x| x.0.0).collect::<Vec<_>>(),
            [150, 130, 110]
        );
        assert_eq!(
            asks.iter().map(|x| x.0.0).collect::<Vec<_>>(),
            [110, 130, 150]
        );
    }

    #[test]
    fn reports_out_of_range_and_misaligned_prices() {
        let mut book = PriceLadder::new(Price(100), 10, 4).unwrap();
        assert_eq!(
            book.upsert(Side::Bid, Price(90), Size(1)).unwrap(),
            LadderUpdate::OutOfRange
        );
        assert_eq!(
            book.upsert(Side::Ask, Price(140), Size(1)).unwrap(),
            LadderUpdate::OutOfRange
        );
        assert!(matches!(
            book.upsert(Side::Ask, Price(115), Size(1)),
            Err(PriceLadderError::MisalignedPrice { .. })
        ));
    }

    use proptest::{prop_assert_eq, proptest};
    proptest! {
        #[test]
        fn agrees_with_btree_for_in_range_updates(
            updates in proptest::collection::vec((proptest::bool::ANY, 0usize..128, 0u64..1000), 1..1000)
        ) {
            let mut ladder = PriceLadder::new(Price(0), 1, 128).unwrap();
            let mut tree = OrderBook::default();
            for (is_bid, index, size) in updates {
                let side = if is_bid { Side::Bid } else { Side::Ask };
                ladder.upsert(side, Price(index as i64), Size(size)).unwrap();
                tree.upsert(side, Price(index as i64), Size(size));
            }

            let ladder_bids: Vec<_> = ladder.view().levels(Side::Bid).collect();
            let ladder_asks: Vec<_> = ladder.view().levels(Side::Ask).collect();
            let (tree_bids, tree_asks) = tree.top_n(usize::MAX);
            prop_assert_eq!(ladder_bids, tree_bids);
            prop_assert_eq!(ladder_asks, tree_asks);
        }
    }
}
