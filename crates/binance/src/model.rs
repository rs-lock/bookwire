use serde::{Deserialize, de};

#[derive(Debug, Deserialize)]
pub struct Frame {
    pub data: Depth,
}

/// A USD-M futures `@depth` diff
#[derive(Debug, Deserialize)]
pub struct Depth {
    /// `U` — first update id in this event.
    #[serde(rename = "U")]
    pub first_u: u64,
    /// `u` — final update id in this event.
    #[serde(rename = "u")]
    pub final_u: u64,
    /// `pu` — final update id of the *previous* event (continuity anchor).
    #[serde(rename = "pu")]
    pub prev_u: u64,

    #[serde(rename = "b")]
    pub bids: Vec<(Price, Size)>,
    #[serde(rename = "a")]
    pub asks: Vec<(Price, Size)>,
}

#[derive(Debug)]
pub struct Price(pub i64);

impl<'de> serde::Deserialize<'de> for Price {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(PriceVisitor)
    }
}

#[derive(Debug, Deserialize)]
pub struct BinanceSnapshot {
    #[serde(rename = "lastUpdateId")]
    pub last_u: u64,
    #[serde(rename = "E")]
    pub event_t: u64,
    #[serde(rename = "T")]
    pub tx_t: u64,

    pub bids: Vec<(Price, Size)>,

    pub asks: Vec<(Price, Size)>,
}

#[derive(Debug)]
pub struct Size(pub u64);

impl<'de> serde::Deserialize<'de> for Size {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(SizeVisitor)
    }
}

struct PriceVisitor;

impl<'de> de::Visitor<'de> for PriceVisitor {
    type Value = Price;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a decimal price")
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let price = parse_fixed(v).map_err(E::custom)?;
        Ok(Price(price))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let price = parse_fixed(v).map_err(E::custom)?;
        Ok(Price(price))
    }
}

struct SizeVisitor;

impl<'de> de::Visitor<'de> for SizeVisitor {
    type Value = Size;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a non-negative decimal size")
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let size = parse_fixed(v)
            .and_then(|value| u64::try_from(value).map_err(|_| ParseError::Overflow))
            .map_err(E::custom)?;

        Ok(Size(size))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let size = parse_fixed(v)
            .and_then(|value| u64::try_from(value).map_err(|_| ParseError::Overflow))
            .map_err(E::custom)?;

        Ok(Size(size))
    }
}

const FIXED_SCALE: u32 = 8;

const POW10: [i64; FIXED_SCALE as usize + 1] = [
    1,
    10,
    100,
    1_000,
    10_000,
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
];

pub fn parse_fixed(s: &str) -> Result<i64, ParseError> {
    let bytes = s.as_bytes();
    let mut i = 0;
    let is_neg = bytes.first() == Some(&b'-');
    if is_neg {
        i = 1;
    }

    let mut mantissa: i64 = 0;
    let mut frac: u32 = 0;
    let mut seen_dot = false;

    while i < bytes.len() {
        let b = bytes[i];
        match b {
            b'0'..=b'9' => {
                mantissa = mantissa * 10 + (b - b'0') as i64;
                if seen_dot {
                    frac += 1;
                }
            }
            b'.' if !seen_dot => seen_dot = true,
            _ => return Err(ParseError::InvalidFormat),
        }
        i += 1;
    }

    if frac > FIXED_SCALE {
        return Err(ParseError::PrecisionLoss);
    }
    mantissa *= POW10[(FIXED_SCALE - frac) as usize];

    Ok(if is_neg { -mantissa } else { mantissa })
}

pub enum ParseError {
    InvalidFormat,
    Overflow,
    PrecisionLoss,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::InvalidFormat => write!(f, "invalid price"),
            ParseError::Overflow => write!(f, "price overflow"),
            ParseError::PrecisionLoss => write!(f, "price precision loss"),
        }
    }
}

