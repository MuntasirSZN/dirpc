use std::sync::Arc;

use crate::HashMap;
use bytes::BytesMut;
use sockudo_ws::handshake::{build_response, generate_accept_key, parse_request};
use sockudo_ws::protocol::Message;
use sockudo_ws::{Config, WebSocketStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::types::ActivityEvent;

/// socket_id -> serialized JSON
type LastMsgs = Arc<HashMap<u64, Arc<str>>>;

pub struct BridgeState {
    pub last_msgs: LastMsgs,
    pub tx: broadcast::Sender<Arc<str>>,
}

impl BridgeState {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(256);
        Self {
            last_msgs: Arc::new(HashMap::default()),
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

    // Feed activity → state + broadcast
    let state_feed = state.clone();
    tokio::spawn(async move {
        loop {
            match activity_rx.recv().await {
                Ok(event) => {
                    let key = event.socket_id;

                    match &event.activity {
                        None => {
                            state_feed.last_msgs.pin().remove(&key);
                        }
                        Some(v) => {
                            let json = match serde_json::to_string(v) {
                                Ok(s) => Arc::<str>::from(s),
                                Err(_) => continue,
                            };

                            state_feed.last_msgs.pin().insert(key, json.clone());
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

async fn do_handshake(mut stream: TcpStream) -> anyhow::Result<TcpStream> {
    let mut buf = BytesMut::with_capacity(4096);

    loop {
        let n = stream.read_buf(&mut buf).await?;
        if n == 0 {
            return Err(anyhow::anyhow!("Connection closed during handshake"));
        }

        if let Some((req, _)) = parse_request(&buf)? {
            let accept_key = generate_accept_key(req.key);
            let response = build_response(&accept_key, None, None);
            stream.write_all(&response).await?;
            stream.flush().await?;
            break;
        }
    }

    Ok(stream)
}

async fn handle_client(stream: TcpStream, state: Arc<BridgeState>) -> anyhow::Result<()> {
    let stream = do_handshake(stream).await?;
    let ws = WebSocketStream::server(stream, Config::uws_defaults());
    let (mut reader, mut writer) = ws.split();

    let mut rx = state.tx.subscribe();

    // Catch-up snapshot: send all last known activity payloads to the new client.
    let snapshot: Vec<Arc<str>> = state
        .last_msgs
        .pin()
        .iter()
        .map(|(_, v)| v.clone())
        .collect();
    for payload in snapshot {
        writer.send(Message::text(&*payload)).await?;
    }

    loop {
        tokio::select! {
            // Broadcast → client
            msg = rx.recv() => {
                match msg {
                    Ok(payload) => {
                        if writer.send(Message::text(&*payload)).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("Client lagged {} messages", n);
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }

            // Incoming frames from client (ignore content, just drain to detect close)
            incoming = reader.next() => {
                match incoming {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(e)) => {
                        debug!("Bridge recv error: {}", e);
                        break;
                    }
                }
            }
        }
    }

    debug!("Client disconnected");
    Ok(())
}
