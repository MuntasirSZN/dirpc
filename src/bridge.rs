use std::sync::Arc;

use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use yawc::{WebSocket, Options};
use yawc::frame::Frame;

use crate::types::ActivityEvent;

/// socket_id -> serialized JSON
type LastMsgs = Arc<DashMap<u64, Arc<str>>>;

pub struct BridgeState {
    pub last_msgs: LastMsgs,
    pub tx: broadcast::Sender<Arc<str>>,
}

impl BridgeState {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(256);
        Self {
            last_msgs: Arc::new(DashMap::new()),
            tx,
        }
    }
}

impl Default for BridgeState {
    fn default() -> Self {
        Self::new()
    }
}

pub async fn start_bridge(
    mut activity_rx: broadcast::Receiver<ActivityEvent>,
    port: u16,
) -> std::io::Result<()> {
    let state = Arc::new(BridgeState::new());

    // 🔥 Feed activity → state + broadcast
    let state_feed = state.clone();
    tokio::spawn(async move {
        loop {
            match activity_rx.recv().await {
                Ok(event) => {
                    let key = event.socket_id;

                    match &event.activity {
                        None => {
                            state_feed.last_msgs.remove(&key);
                        }
                        Some(v) => {
                            let json = match serde_json::to_string(v) {
                                Ok(s) => Arc::<str>::from(s),
                                Err(_) => continue,
                            };

                            state_feed.last_msgs.insert(key, json.clone());
                            let _ = state_feed.tx.send(json);
                        }
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!("Bridge lagged {} messages", n);
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    let addr = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(&addr).await?;
    info!("Bridge listening on ws://{}", addr);

    loop {
        let (stream, peer) = listener.accept().await?;
        debug!("Client connected: {}", peer);

        let state_conn = state.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_client(stream, state_conn).await {
                warn!("Client error: {}", e);
            }
        });
    }
}

async fn handle_client(
    stream: TcpStream,
    state: Arc<BridgeState>,
) -> anyhow::Result<()> {
    // ⚠️ Required by yawc handshake API
    let url = "ws://localhost/".parse().unwrap();

    // Perform handshake over raw TCP stream
    let ws = WebSocket::handshake(
        url,
        stream,
        Options::default(),
    ).await?;

    // yawc implements Sink + Stream → split works
    let (mut sink, mut source) = ws.split();

    let mut rx = state.tx.subscribe();

    // 🔹 Catch-up snapshot
    for entry in state.last_msgs.iter() {
        let payload = entry.value().clone();
        sink.send(Frame::text(&*payload)).await?;
    }

    loop {
        tokio::select! {
            // 🔥 Broadcast → client
            msg = rx.recv() => {
                match msg {
                    Ok(payload) => {
                        if sink.send(Frame::text(&*payload)).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("Client lagged {} messages", n);
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }

            // 🔄 Incoming frames (correct type)
            incoming = source.next() => {
                match incoming {
                    Some(frame) => {
                        // frame is already a Frame (NOT Result)
                        // yawc auto-handles ping/pong/close internally
                        let _ = frame;
                    }
                    None => break,
                }
            }
        }
    }

    debug!("Client disconnected");
    Ok(())
}
