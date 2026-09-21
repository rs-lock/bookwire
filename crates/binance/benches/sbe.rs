/*Layout follows Binance's real spot SBE schema `stream_1_0.xml`, message
`DepthDiffStreamEvent` (templateId 10003):

    messageHeader (8B, LE): blockLength:u16 templateId:u16 schemaId:u16 version:u16
    root block  (26B, LE):  eventTime:i64(us)  firstBookUpdateId:i64  lastBookUpdateId:i64
                            priceExponent:i8  qtyExponent:i8

    group bids  (groupSize16: blockLength:u16=16 numInGroup:u16) then per level:
                            price:i64(mantissa)  qty:i64(mantissa)
    group asks  (same)
    data symbol (varString8: len:u8 + ascii bytes)
*/

pub struct SbeDecoder<'a> {
    buf: &'a [u8],
    pub pos: usize,
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct Header {
    block_len: u16,
    template_id: u16,
    schema_id: u16,
    version: u16,
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct Level {
    price: i64,
    qty: i64,
}
#[allow(dead_code)]
#[derive(Debug)]
pub struct DepthDiff<'a> {
    event_time: i64,
    first_book_update_id: i64,
    last_book_update_id: i64,

    price_exponent: i8,
    qty_exponent: i8,

    bids: Vec<Level>,
    asks: Vec<Level>,

    symbol: &'a str,
}

impl<'a> SbeDecoder<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn take<const N: usize>(&mut self) -> Result<[u8; N], &'static str> {
        let end = self.pos.checked_add(N).ok_or("offset overflow")?;

        if end > self.buf.len() {
            return Err("()");
        }

        let ptr = unsafe { self.buf.as_ptr().add(self.pos) };
        let bytes = unsafe { std::ptr::read_unaligned(ptr as *const [u8; N]) };

        self.pos = end;
        Ok(bytes)
    }

    pub fn u8(&mut self) -> Result<u8, &'static str> {
        Ok(self.take::<1>()?[0])
    }

    pub fn i8(&mut self) -> Result<i8, &'static str> {
        Ok(self.u8()? as i8)
    }

    pub fn u16_le(&mut self) -> Result<u16, &'static str> {
        Ok(u16::from_le_bytes(self.take()?))
    }

    pub fn i64_le(&mut self) -> Result<i64, &'static str> {
        Ok(i64::from_le_bytes(self.take()?))
    }

    fn bytes(&mut self, len: usize) -> Result<&'a [u8], &'static str> {
        let end = self.pos.checked_add(len).ok_or("offset overflow")?;
        let bytes = self.buf.get(self.pos..end).ok_or("unexpected EOF")?;

        self.pos = end;

        Ok(bytes)
    }
}

pub fn decode_levels(d: &mut SbeDecoder<'_>) -> Result<Vec<Level>, &'static str> {
    let block_length = d.u16_le()?;
    let count = d.u16_le()? as usize;

    if block_length < 16 {
        return Err("zaluoa");
    }

    let mut levels = Vec::with_capacity(count);
    for _ in 0..count {
        //let start = d.pos;
        let price = d.i64_le()?;
        let qty = d.i64_le()?;
        let level = Level { price, qty };

        levels.push(level);
    }

    Ok(levels)
}

pub fn decode_message<'a>(
    d: &mut SbeDecoder<'a>,
    header: &Header,
) -> Result<DepthDiff<'a>, &'static str> {
    let event_time = d.i64_le()?;
    let first_book_update_id = d.i64_le()?;
    let last_book_update_id = d.i64_le()?;
    let price_exponent = d.i8()?;
    let qty_exponent = d.i8()?;

    let bids = decode_levels(d)?;
    let asks = decode_levels(d)?;

    let symbol_len = d.u8()? as usize;
    let symbol_bytes = d.bytes(symbol_len)?;

    let symbol = std::str::from_utf8(symbol_bytes).map_err(|_| "invalid symbol")?;
    Ok(DepthDiff {
        event_time,
        first_book_update_id,
        last_book_update_id,
        price_exponent,
        qty_exponent,
        bids,
        asks,
        symbol,
    })
}

pub fn decode_header(d: &mut SbeDecoder<'_>) -> Result<Header, &'static str> {
    Ok(Header {
        block_len: d.u16_le()?,
        template_id: d.u16_le()?,
        schema_id: d.u16_le()?,
        version: d.u16_le()?,
    })
}
