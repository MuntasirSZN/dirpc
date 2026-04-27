use std::collections::HashMap;
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Mutex};
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};

use crate::types::ActivityEvent;

/// Per-socket-id last known activity (null = cleared).
type LastMsgs = Arc<Mutex<HashMap<u64, Value>>>;

/// Shared bridge state: last messages + a broadcast channel for live updates.
pub struct BridgeState {
    pub last_msgs: LastMsgs,
    pub tx: broadcast::Sender<String>,
}

impl BridgeState {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(128);
        Self {
            last_msgs: Arc::new(Mutex::new(HashMap::new())),
            tx,
        }
    }
}

impl Default for BridgeState {
    fn default() -> Self {
        Self::new()
    }
}

/// Start the bridge WebSocket server.
///
/// * Listens on `port` (default 1337).
/// * Subscribes to `activity_rx` and broadcasts updates to all web clients.
/// * On new client connection, replays the current activity table for catch-up.
pub async fn start_bridge(
    mut activity_rx: broadcast::Receiver<ActivityEvent>,
    port: u16,
) -> std::io::Result<()> {
    let state = Arc::new(BridgeState::new());

    // Drive the activity feed into the bridge state + broadcast channel.
    let state_feed = state.clone();
    tokio::spawn(async move {
        loop {
            match activity_rx.recv().await {
                Ok(event) => {
                    let key = event.socket_id;
                    let msg_json = serde_json::to_string(&event.activity).unwrap_or_default();

                    {
                        let mut map = state_feed.last_msgs.lock().await;
                        match &event.activity {
                            None => {
                                map.remove(&key);
                            }
                            Some(v) => {
                                map.insert(key, v.clone());
                            }
                        }
                    }

                    let _ = state_feed.tx.send(msg_json);
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!("Bridge activity channel lagged by {} messages", n);
                }
                Err(broadcast::error::RecvError::Closed) => {
                    debug!("Bridge activity channel closed");
                    break;
                }
            }
        }
    });

    let addr = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(&addr).await?;
    info!("Bridge WebSocket server listening on ws://{}", addr);

    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                debug!("Bridge client connected from {}", peer);
                let state_conn = state.clone();
                tokio::spawn(async move {
                    handle_bridge_client(stream, state_conn).await;
                });
            }
            Err(e) => {
                error!("Bridge accept error: {}", e);
            }
        }
    }
}

async fn handle_bridge_client(
    stream: tokio::net::TcpStream,
    state: Arc<BridgeState>,
) {
    let ws_stream = match accept_async(stream).await {
        Ok(ws) => ws,
        Err(e) => {
            warn!("Bridge WS handshake failed: {}", e);
            return;
        }
    };

    let (mut sink, mut source) = ws_stream.split();

    // Subscribe before sending catch-up to avoid missing concurrent updates.
    let mut rx = state.tx.subscribe();

    // Send catch-up: all currently-tracked activities.
    {
        let map = state.last_msgs.lock().await;
        for v in map.values() {
            let payload = serde_json::to_string(v).unwrap_or_default();
            if sink.send(Message::Text(payload)).await.is_err() {
                return;
            }
        }
    }

    loop {
        tokio::select! {
            // Forward live activity updates to the web client.
            result = rx.recv() => {
                match result {
                    Ok(payload) => {
                        if sink.send(Message::Text(payload)).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("Bridge client missed {} activity messages", n);
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            // Drain incoming frames (we don't use them, but must drive the reader).
            msg = source.next() => {
                match msg {
                    Some(Ok(_)) => {}
                    _ => break,
                }
            }
        }
    }

    debug!("Bridge client disconnected");
}
