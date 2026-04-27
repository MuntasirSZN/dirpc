use std::sync::Arc;

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use serde::Deserialize;
use tokio::net::TcpListener;
use tracing::{debug, info, warn};

use crate::server::{ServerState, READY_PAYLOAD};
use crate::types::RpcMessage;

// ── Origin validation ─────────────────────────────────────────────────────────

/// Return `true` when the `Origin` header is acceptable for WebSocket upgrades.
///
/// An empty origin (direct / non-browser connections) is permitted, as are the
/// official Discord web domains.
pub fn validate_origin(origin: &str) -> bool {
    matches!(
        origin,
        "" | "https://discord.com"
            | "https://ptb.discord.com"
            | "https://canary.discord.com"
    )
}

// ── Query parameters ──────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct WsQueryParams {
    pub v: Option<u32>,
    pub encoding: Option<String>,
    pub client_id: Option<String>,
}

// ── Server ────────────────────────────────────────────────────────────────────

/// Try to bind an HTTP/WebSocket server on the first available port in 6463-6472.
pub async fn start_ws_server(state: Arc<ServerState>) -> std::io::Result<()> {
    for port in 6463u16..=6472 {
        let addr = format!("127.0.0.1:{}", port);
        match TcpListener::bind(&addr).await {
            Ok(listener) => {
                info!("WebSocket HTTP server listening on http://{}", addr);
                let app = make_router(state);
                axum::serve(listener, app).await?;
                return Ok(());
            }
            Err(e) => {
                debug!("Port {} unavailable: {}", port, e);
            }
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AddrInUse,
        "No available port in 6463-6472",
    ))
}

/// Build the axum `Router` (useful for tests that want to control the listener).
pub fn make_router(state: Arc<ServerState>) -> Router {
    Router::new()
        .route("/", get(ws_handler))
        .with_state(state)
}

// ── Handler ───────────────────────────────────────────────────────────────────

async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(params): Query<WsQueryParams>,
    headers: HeaderMap,
    State(state): State<Arc<ServerState>>,
) -> Response {
    // Validate protocol version.
    if params.v != Some(1) {
        return (StatusCode::BAD_REQUEST, "v must be 1").into_response();
    }

    // Validate encoding.
    if params.encoding.as_deref() != Some("json") {
        return (StatusCode::BAD_REQUEST, "encoding must be json").into_response();
    }

    // Validate Origin.
    let origin = headers
        .get("origin")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !validate_origin(origin) {
        return (StatusCode::FORBIDDEN, "origin not allowed").into_response();
    }

    let client_id = params.client_id.unwrap_or_default();

    ws.on_upgrade(move |socket| handle_ws_socket(socket, state, client_id))
}

async fn handle_ws_socket(mut socket: WebSocket, state: Arc<ServerState>, client_id: String) {
    let socket_id = state.next_id();
    debug!("WS connected: client_id={} socket_id={}", client_id, socket_id);

    // Send DISPATCH/READY.
    if socket
        .send(Message::Text(READY_PAYLOAD.to_string()))
        .await
        .is_err()
    {
        return;
    }

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    state.register_socket(socket_id, tx).await;

    loop {
        tokio::select! {
            // Server → client
            outbound = rx.recv() => {
                match outbound {
                    Some(json) => {
                        if socket.send(Message::Text(json)).await.is_err() {
                            break;
                        }
                    }
                    None => break,
                }
            }
            // Client → server
            inbound = socket.recv() => {
                match inbound {
                    Some(Ok(Message::Text(text))) => {
                        match serde_json::from_str::<RpcMessage>(&text) {
                            Ok(msg) => {
                                if let Some(resp) =
                                    state.handle_message(socket_id, &client_id, &msg).await
                                {
                                    state.send_to_socket(socket_id, resp).await;
                                }
                            }
                            Err(e) => warn!("Bad WS JSON: {}", e),
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(e)) => {
                        debug!("WS recv error: {}", e);
                        break;
                    }
                }
            }
        }
    }

    state.unregister_socket(socket_id).await;
    debug!("WS disconnected: socket_id={}", socket_id);
}
