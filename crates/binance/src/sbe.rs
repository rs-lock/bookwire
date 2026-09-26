//! Decoder and length-delimited replay reader for Binance spot SBE depth diffs.
//!
//! The fixture uses Binance `DepthDiffStreamEvent` (template 10003), framed as
//! `[u32 little-endian message length][SBE message]...`.

use std::io::ErrorKind;
use std::path::Path;

use tokio::fs::File;
use tokio::io::{AsyncReadExt, BufReader};

use crate::model::{Price, Size};

const HEADER_LEN: usize = 8;
const ROOT_BLOCK_LEN: u16 = 26;
const LEVEL_BLOCK_LEN: u16 = 16;
const DEPTH_DIFF_TEMPLATE_ID: u16 = 10003;
const FIXED_EXPONENT: i8 = -8;

#[derive(Debug)]
pub struct SbeDepth {
    pub event_time_us: u64,
    pub first_u: u64,
    pub final_u: u64,
    pub bids: Vec<(Price, Size)>,
    pub asks: Vec<(Price, Size)>,
}

#[derive(Debug, thiserror::Error)]
pub enum SbeError {
    #[error("truncated SBE message at byte {0}")]
    Truncated(usize),
    #[error("unexpected template id {0}")]
    Template(u16),
    #[error("unsupported root block length {0}")]
    RootBlockLength(u16),
    #[error("unsupported level block length {0}")]
    LevelBlockLength(u16),
    #[error("unsupported decimal exponents price={price}, quantity={quantity}")]
    Exponent { price: i8, quantity: i8 },
    #[error("invalid negative value for {0}")]
    Negative(&'static str),
}

struct Decoder<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn take<const N: usize>(&mut self) -> Result<[u8; N], SbeError> {
        let end = self
            .pos
            .checked_add(N)
            .ok_or(SbeError::Truncated(self.pos))?;
        let slice = self
            .bytes
            .get(self.pos..end)
            .ok_or(SbeError::Truncated(self.pos))?;
        self.pos = end;
        Ok(slice.try_into().expect("slice length checked"))
    }

    fn skip(&mut self, count: usize) -> Result<(), SbeError> {
        let end = self
            .pos
            .checked_add(count)
            .ok_or(SbeError::Truncated(self.pos))?;
        self.bytes
            .get(self.pos..end)
            .ok_or(SbeError::Truncated(self.pos))?;
        self.pos = end;
        Ok(())
    }

    fn u8(&mut self) -> Result<u8, SbeError> {
        Ok(self.take::<1>()?[0])
    }

    fn i8(&mut self) -> Result<i8, SbeError> {
        Ok(self.u8()? as i8)
    }

    fn u16(&mut self) -> Result<u16, SbeError> {
        Ok(u16::from_le_bytes(self.take()?))
    }

    fn i64(&mut self) -> Result<i64, SbeError> {
        Ok(i64::from_le_bytes(self.take()?))
    }
}

pub fn decode(bytes: &[u8]) -> Result<SbeDepth, SbeError> {
    let mut decoder = Decoder::new(bytes);

    let root_block_len = decoder.u16()?;
    let template_id = decoder.u16()?;
    let _schema_id = decoder.u16()?;
    let _version = decoder.u16()?;

    if template_id != DEPTH_DIFF_TEMPLATE_ID {
        return Err(SbeError::Template(template_id));
    }
    if root_block_len < ROOT_BLOCK_LEN {
        return Err(SbeError::RootBlockLength(root_block_len));
    }

    let event_time = decoder.i64()?;
    let first_u = decoder.i64()?;
    let final_u = decoder.i64()?;
    let price_exponent = decoder.i8()?;
    let quantity_exponent = decoder.i8()?;
    decoder.skip(usize::from(root_block_len - ROOT_BLOCK_LEN))?;

    if price_exponent != FIXED_EXPONENT || quantity_exponent != FIXED_EXPONENT {
        return Err(SbeError::Exponent {
            price: price_exponent,
            quantity: quantity_exponent,
        });
    }
    if event_time < 0 || first_u < 0 || final_u < 0 {
        return Err(SbeError::Negative("timestamp or update id"));
    }

    let bids = decode_levels(&mut decoder)?;
    let asks = decode_levels(&mut decoder)?;

    // Consume and validate the trailing varString8 symbol.
    let symbol_len = usize::from(decoder.u8()?);
    decoder.skip(symbol_len)?;

    Ok(SbeDepth {
        event_time_us: event_time as u64,
        first_u: first_u as u64,
        final_u: final_u as u64,
        bids,
        asks,
    })
}

fn decode_levels(decoder: &mut Decoder<'_>) -> Result<Vec<(Price, Size)>, SbeError> {
    let block_len = decoder.u16()?;
    let count = usize::from(decoder.u16()?);
    if block_len < LEVEL_BLOCK_LEN {
        return Err(SbeError::LevelBlockLength(block_len));
    }

    let extension_len = usize::from(block_len - LEVEL_BLOCK_LEN);
    let mut levels = Vec::with_capacity(count);
    for _ in 0..count {
        let price = decoder.i64()?;
        let quantity = decoder.i64()?;
        if quantity < 0 {
            return Err(SbeError::Negative("quantity"));
        }
        decoder.skip(extension_len)?;
        levels.push((Price(price), Size(quantity as u64)));
    }
    Ok(levels)
}

pub struct SbeReplay {
    reader: BufReader<File>,
}

impl SbeReplay {
    pub async fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        Ok(Self {
            reader: BufReader::new(File::open(path).await?),
        })
    }

    pub async fn next(&mut self) -> std::io::Result<Option<Vec<u8>>> {
        let mut length_bytes = [0_u8; 4];
        match self.reader.read_exact(&mut length_bytes[..1]).await {
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => return Ok(None),
            Err(error) => return Err(error),
        }
        self.reader.read_exact(&mut length_bytes[1..]).await?;

        let length = u32::from_le_bytes(length_bytes) as usize;
        if length < HEADER_LEN {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                format!("invalid SBE message length {length}"),
            ));
        }

        let mut message = vec![0_u8; length];
        self.reader.read_exact(&mut message).await?;
        Ok(Some(message))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message() -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&ROOT_BLOCK_LEN.to_le_bytes());
        bytes.extend_from_slice(&DEPTH_DIFF_TEMPLATE_ID.to_le_bytes());
        bytes.extend_from_slice(&1_u16.to_le_bytes());
        bytes.extend_from_slice(&0_u16.to_le_bytes());
        bytes.extend_from_slice(&123_i64.to_le_bytes());
        bytes.extend_from_slice(&10_i64.to_le_bytes());
        bytes.extend_from_slice(&20_i64.to_le_bytes());
        bytes.push(FIXED_EXPONENT as u8);
        bytes.push(FIXED_EXPONENT as u8);
        bytes.extend_from_slice(&LEVEL_BLOCK_LEN.to_le_bytes());
        bytes.extend_from_slice(&1_u16.to_le_bytes());
        bytes.extend_from_slice(&100_i64.to_le_bytes());
        bytes.extend_from_slice(&2_i64.to_le_bytes());
        bytes.extend_from_slice(&LEVEL_BLOCK_LEN.to_le_bytes());
        bytes.extend_from_slice(&0_u16.to_le_bytes());
        bytes.push(3);
        bytes.extend_from_slice(b"BTC");
        bytes
    }

    #[test]
    fn decodes_depth_diff() {
        let depth = decode(&message()).expect("decode");
        assert_eq!(depth.event_time_us, 123);
        assert_eq!((depth.first_u, depth.final_u), (10, 20));
        assert_eq!(depth.bids[0].0.0, 100);
        assert_eq!(depth.bids[0].1.0, 2);
        assert!(depth.asks.is_empty());
    }

    #[test]
    fn rejects_truncated_message() {
        let mut bytes = message();
        bytes.pop();
        assert!(matches!(decode(&bytes), Err(SbeError::Truncated(_))));
    }
}
