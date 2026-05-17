use std::sync::Arc;

use bytes::BytesMut;
use sockudo_ws::WebSocketStream;
use sockudo_ws::handshake::{build_response, generate_accept_key, parse_request};
use sockudo_ws::protocol::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info, warn};

use crate::server::{READY_PAYLOAD, ServerState};
use crate::types::RpcMessage;

/// Return `true` when the `Origin` header is acceptable for WebSocket upgrades.
///
/// An empty origin (direct / non-browser connections) is permitted, as are the
/// official Discord web domains.
pub fn validate_origin(origin: &str) -> bool {
    matches!(
        origin,
        "" | "https://discord.com" | "https://ptb.discord.com" | "https://canary.discord.com"
    )
}

/// Parse query parameters from the raw HTTP request line.
///
/// Returns `(v, encoding, client_id)` extracted from the URI query string.
fn parse_query(buf: &[u8]) -> (Option<u32>, Option<String>, String) {
    let text = std::str::from_utf8(buf).unwrap_or("");
    let request_line = text.lines().next().unwrap_or("");
    let path = request_line.split_whitespace().nth(1).unwrap_or("/");
    let query = path.split_once('?').map(|(_, q)| q).unwrap_or("");

    let mut v: Option<u32> = None;
    let mut encoding: Option<String> = None;
    let mut client_id = String::new();

    for part in query.split('&') {
        if let Some((key, value)) = part.split_once('=') {
            match key {
                "v" => v = value.parse().ok(),
                "encoding" => encoding = Some(value.to_string()),
                "client_id" => client_id = value.to_string(),
                _ => {}
            }
        }
    }

    (v, encoding, client_id)
}

/// Extract the `Origin` header value from a raw HTTP request.
fn extract_origin(buf: &[u8]) -> &str {
    let text = match std::str::from_utf8(buf) {
        Ok(s) => s,
        Err(_) => return "",
    };
    text.lines()
        .find(|l| l.len() > 7 && l[..7].eq_ignore_ascii_case("origin:"))
        .and_then(|l| l.split_once(':').map(|(_, v)| v.trim()))
        .unwrap_or("")
}

/// Try to bind an HTTP/WebSocket server on the first available port in 6463–6472.
pub async fn start_ws_server(state: Arc<ServerState>) -> std::io::Result<()> {
    for port in 6463u16..=6472 {
        let addr = format!("127.0.0.1:{port}");
        match TcpListener::bind(&addr).await {
            Ok(listener) => {
                info!("WebSocket server listening on ws://{addr}");
                loop {
                    match listener.accept().await {
                        Ok((stream, _)) => {
                            let state = Arc::clone(&state);
                            tokio::spawn(async move {
                                if let Err(e) = handle_connection(stream, state).await {
                                    debug!("WS connection error: {e}");
                                }
                            });
                        }
                        Err(e) => debug!("Accept error: {e}"),
                    }
                }
            }
            Err(e) => debug!("Port {port} unavailable: {e}"),
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AddrInUse,
        "No available port in 6463-6472",
    ))
}

/// Read the HTTP upgrade request, validate query params / Origin, complete the
/// WebSocket handshake, and return the upgraded stream plus the `client_id`.
async fn do_handshake(mut stream: TcpStream) -> anyhow::Result<(TcpStream, String)> {
    let mut buf = BytesMut::with_capacity(4096);

    loop {
        let n = stream.read_buf(&mut buf).await?;
        if n == 0 {
            return Err(anyhow::anyhow!("Connection closed during handshake"));
        }

        let Some((req, _)) = parse_request(&buf)? else {
            continue;
        };

        // ── Validate query params ─────────────────────────────────────────────
        let (v, encoding, client_id) = parse_query(&buf);

        if v != Some(1) {
            stream
                .write_all(
                    b"HTTP/1.1 400 Bad Request\r\nContent-Length: 12\r\nConnection: close\r\n\r\nv must be 1",
                )
                .await?;
            return Err(anyhow::anyhow!("WS: v must be 1"));
        }

        if encoding.as_deref() != Some("json") {
            stream
                .write_all(
                    b"HTTP/1.1 400 Bad Request\r\nContent-Length: 21\r\nConnection: close\r\n\r\nencoding must be json",
                )
                .await?;
            return Err(anyhow::anyhow!("WS: encoding must be json"));
        }

        // ── Validate Origin ───────────────────────────────────────────────────
        let origin = extract_origin(&buf);
        if !validate_origin(origin) {
            stream
                .write_all(
                    b"HTTP/1.1 403 Forbidden\r\nContent-Length: 18\r\nConnection: close\r\n\r\norigin not allowed",
                )
                .await?;
            return Err(anyhow::anyhow!("WS: origin not allowed"));
        }

        // ── Complete WebSocket upgrade ─────────────────────────────────────────
        let accept_key = generate_accept_key(req.key);
        let response = build_response(&accept_key, None, None);
        stream.write_all(&response).await?;
        stream.flush().await?;

        return Ok((stream, client_id));
    }
}

async fn handle_connection(stream: TcpStream, state: Arc<ServerState>) -> anyhow::Result<()> {
    let (stream, client_id) = do_handshake(stream).await?;

    let ws = WebSocketStream::server(stream, crate::get_ws_config());
    let (mut reader, mut writer) = ws.split();

    let socket_id = state.next_id();
    debug!(
        "WS connected: client_id={} socket_id={}",
        client_id, socket_id
    );

    // Send DISPATCH/READY.
    if writer.send(Message::text(READY_PAYLOAD)).await.is_err() {
        return Ok(());
    }

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    state.register_socket(socket_id, tx).await;

    loop {
        tokio::select! {
            // Server → client
            outbound = rx.recv() => {
                match outbound {
                    Some(json) => {
                        if writer.send(Message::text(&json)).await.is_err() {
                            break;
                        }
                    }
                    None => break,
                }
            }
            // Client → server
            inbound = reader.next() => {
                match inbound {
                    Some(Ok(Message::Text(text))) => {
                        match serde_json::from_slice::<RpcMessage>(&text) {
                            Ok(rpc_msg) => {
                                if let Some(resp) =
                                    state.handle_message(socket_id, &client_id, &rpc_msg).await
                                {
                                    state.send_to_socket(socket_id, resp).await;
                                }
                            }
                            Err(e) => warn!("Bad WS JSON: {e}"),
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(e)) => {
                        debug!("WS recv error: {e}");
                        break;
                    }
                }
            }
        }
    }

    state.unregister_socket(socket_id).await;
    debug!("WS disconnected: socket_id={}", socket_id);
    Ok(())
}
