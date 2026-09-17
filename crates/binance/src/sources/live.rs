use async_trait::async_trait;
use clob_venue::VenueError;
use futures_util::SinkExt;
use futures_util::StreamExt;
use futures_util::stream::SplitSink;
use futures_util::stream::SplitStream;
use serde::Serialize;
use tokio::net::TcpStream;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::FrameSource;
use crate::SnapshotSource;
use crate::model::BinanceSnapshot;

/* curl --request GET \
--url 'https://fapi.binance.com/fapi/v1/depth?symbol=%3Cstring%3E' */

const SNAPEP: &str = "https://fapi.binance.com/fapi/v1/depth";
const ENDPOINT: &str = "wss://fstream.binance.com/public/stream";

pub async fn pull_snapshot(symbol: &str) -> Result<(BinanceSnapshot, String), VenueError> {
    let raw = reqwest::Client::new()
        .get(SNAPEP)
        .query(&[("symbol", symbol), ("limit", "100")])
        .send()
        .await
        .map_err(|e| VenueError::Snapshot(e.to_string()))?
        .text()
        .await
        .map_err(|e| VenueError::Snapshot(e.to_string()))?;

    let snap = serde_json::from_str::<BinanceSnapshot>(&raw)
        .map_err(|e| VenueError::Snapshot(e.to_string()))?;

    Ok((snap, raw))
}

type WsRead = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;
type WsWrite = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;

pub struct BinanceWs {
    symbol: String,
    wsread: Option<WsRead>,
    wswrite: Option<WsWrite>,
}

impl BinanceWs {
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            symbol: symbol.into(),
            wsread: None,
            wswrite: None,
        }
    }
}

#[async_trait]
impl FrameSource for BinanceWs {
    async fn connect(&mut self) -> Result<(), VenueError> {
        let (ws, _resp) = connect_async(ENDPOINT)
            .await
            .map_err(|e| VenueError::Connection(e.to_string()))?;

        let (mut write, read) = ws.split();
        let sub = serde_json::to_string(&SubMessage::new(&self.symbol))
            .map_err(|e| VenueError::Protocol(e.to_string()))?;

        write
            .send(Message::Text(sub))
            .await
            .map_err(|e| VenueError::Connection(e.to_string()))?;

        self.wsread = Some(read);
        self.wswrite = Some(write);
        Ok(())
    }

    async fn next(&mut self) -> Result<String, VenueError> {
        tracing::debug!("frame");
        read_text_frame(self.wsread.as_mut(), self.wswrite.as_mut()).await
    }
}

#[derive(Default, Clone)]
pub struct BinanceRest;

impl BinanceRest {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl SnapshotSource for BinanceRest {
    async fn pull(&self, symbol: &str) -> Result<(BinanceSnapshot, String), VenueError> {
        pull_snapshot(symbol).await
    }
}

async fn read_text_frame(
    read: Option<&mut WsRead>,
    mut write: Option<&mut WsWrite>,
) -> Result<String, VenueError> {
    let read = read.ok_or_else(|| VenueError::Connection("wsread".into()))?;
    loop {
        match read.next().await {
            Some(Ok(Message::Text(t))) => return Ok(t),

            Some(Ok(Message::Ping(payload))) => {
                let write = write
                    .as_deref_mut()
                    .ok_or_else(|| VenueError::Connection("wswrite".into()))?;
                write
                    .send(Message::Pong(payload))
                    .await
                    .map_err(|e| VenueError::Protocol(e.to_string()))?;
            }

            Some(Ok(Message::Close(_))) => {
                return Err(VenueError::Connection("websocket closed".into()));
            }
            Some(Ok(_)) => {} // Pong / Binary / Frame — ignore
            Some(Err(e)) => return Err(VenueError::Connection(e.to_string())),
            None => return Err(VenueError::Connection("stream ended".into())),
        }
    }
}

#[derive(Debug, Serialize)]
struct SubMessage {
    method: &'static str,
    params: [String; 1],
    id: String,
}

impl SubMessage {
    fn new(symbol: &str) -> Self {
        Self {
            method: "SUBSCRIBE",
            params: [format!("{}usdt@depth@100ms", symbol.to_lowercase())],
            id: uuid::Uuid::new_v4().simple().to_string(),
        }
    }
}
