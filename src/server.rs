use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use parking_lot::RwLock;
use serde_json::{Value, json};
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, warn};

use crate::types::{ActivityEvent, RpcMessage};

/// Mock user/config data sent to every new client upon connection.
pub const READY_PAYLOAD: &str = r#"{"cmd":"DISPATCH","data":{"v":1,"config":{"cdn_host":"cdn.discordapp.com","api_endpoint":"//discord.com/api","environment":"production"},"user":{"id":"1045800378228281345","username":"arrpc","discriminator":"0","global_name":"arRPC","avatar":"cfefa4d9839fb4bdf030f91c2a13e95c","avatar_decoration_data":null,"bot":false,"flags":0,"premium_type":0}},"evt":"READY","nonce":null}"#;

/// Shared server state threaded through all transport handlers.
pub struct ServerState {
    pub next_socket_id: AtomicU64,
    pub activity_tx: broadcast::Sender<ActivityEvent>,
    sockets: RwLock<HashMap<u64, mpsc::UnboundedSender<String>>>,
}

impl ServerState {
    /// Create a new `ServerState` and return the initial activity broadcast receiver.
    pub fn new() -> (Arc<Self>, broadcast::Receiver<ActivityEvent>) {
        let (activity_tx, activity_rx) = broadcast::channel(64);
        let state = Arc::new(Self {
            next_socket_id: AtomicU64::new(1),
            activity_tx,
            sockets: RwLock::new(HashMap::new()),
        });
        (state, activity_rx)
    }

    /// Allocate the next unique socket identifier.
    pub fn next_id(&self) -> u64 {
        self.next_socket_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Register a per-socket response sender.
    pub async fn register_socket(&self, socket_id: u64, tx: mpsc::UnboundedSender<String>) {
        self.sockets.write().insert(socket_id, tx);
    }

    /// Remove a socket and emit a null-activity cleanup event.
    pub async fn unregister_socket(&self, socket_id: u64) {
        self.sockets.write().remove(&socket_id);
        let _ = self.activity_tx.send(ActivityEvent {
            activity: None,
            pid: None,
            socket_id,
        });
    }

    /// Forward a text frame to a specific socket.
    pub async fn send_to_socket(&self, socket_id: u64, msg: String) {
        let sockets = self.sockets.read();
        if let Some(tx) = sockets.get(&socket_id) {
            let _ = tx.send(msg);
        }
    }

    /// Process an RPC command and return an optional JSON response string.
    ///
    /// Activity broadcast side-effects happen here as well.
    pub async fn handle_message(
        &self,
        socket_id: u64,
        client_id: &str,
        msg: &RpcMessage,
    ) -> Option<String> {
        match msg.cmd.as_str() {
            "SET_ACTIVITY" => {
                let args = msg.args.as_ref().unwrap_or(&Value::Null);
                let pid = args.get("pid").and_then(|v| v.as_u64()).map(|v| v as u32);
                let activity = args.get("activity");

                match activity {
                    None | Some(Value::Null) => {
                        let _ = self.activity_tx.send(ActivityEvent {
                            activity: None,
                            pid,
                            socket_id,
                        });
                        Some(
                            serde_json::to_string(&json!({
                                "cmd": msg.cmd,
                                "data": null,
                                "evt": null,
                                "nonce": msg.nonce,
                            }))
                            .unwrap(),
                        )
                    }
                    Some(raw_activity) => {
                        let mut activity = raw_activity.clone();
                        let mut metadata = serde_json::Map::new();
                        let mut extra = serde_json::Map::new();

                        // Map buttons: extract labels for the frame, urls for metadata.
                        if let Some(buttons) = activity
                            .as_object()
                            .and_then(|o| o.get("buttons"))
                            .and_then(|b| b.as_array())
                            .cloned()
                        {
                            let labels: Vec<Value> = buttons
                                .iter()
                                .filter_map(|b| b.get("label").cloned())
                                .collect();
                            let urls: Vec<Value> = buttons
                                .iter()
                                .filter_map(|b| b.get("url").cloned())
                                .collect();
                            if !labels.is_empty() {
                                extra.insert("buttons".to_string(), json!(labels));
                                metadata.insert("button_urls".to_string(), json!(urls));
                            }
                        }

                        // Translate timestamps from seconds to milliseconds when needed.
                        if let Some(ts_obj) = activity.get_mut("timestamps") {
                            if let Some(start) = ts_obj.get("start").and_then(|v| v.as_i64()) {
                                ts_obj["start"] = json!(maybe_to_ms(start));
                            }
                            if let Some(end) = ts_obj.get("end").and_then(|v| v.as_i64()) {
                                ts_obj["end"] = json!(maybe_to_ms(end));
                            }
                        }

                        let instance = activity
                            .get("instance")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false);
                        let flags: u64 = if instance { 1 } else { 0 };

                        // Merge base fields, activity fields, then extra (buttons).
                        let mut full = serde_json::Map::new();
                        full.insert("application_id".to_string(), json!(client_id));
                        full.insert("type".to_string(), json!(0u32));
                        full.insert("metadata".to_string(), Value::Object(metadata));
                        full.insert("flags".to_string(), json!(flags));
                        if let Some(obj) = activity.as_object() {
                            for (k, v) in obj {
                                full.insert(k.clone(), v.clone());
                            }
                        }
                        for (k, v) in &extra {
                            full.insert(k.clone(), v.clone());
                        }

                        let _ = self.activity_tx.send(ActivityEvent {
                            activity: Some(Value::Object(full)),
                            pid,
                            socket_id,
                        });

                        // Build response data: activity with name/application_id/type overrides.
                        let mut resp_data = activity.clone();
                        if let Some(obj) = resp_data.as_object_mut() {
                            obj.insert("name".to_string(), json!(""));
                            obj.insert("application_id".to_string(), json!(client_id));
                            obj.insert("type".to_string(), json!(0u32));
                        }

                        Some(
                            serde_json::to_string(&json!({
                                "cmd": msg.cmd,
                                "data": resp_data,
                                "evt": null,
                                "nonce": msg.nonce,
                            }))
                            .unwrap(),
                        )
                    }
                }
            }

            "CONNECTIONS_CALLBACK" => Some(
                serde_json::to_string(&json!({
                    "cmd": msg.cmd,
                    "data": {"code": 1000},
                    "evt": "ERROR",
                    "nonce": msg.nonce,
                }))
                .unwrap(),
            ),

            "INVITE_BROWSER" => {
                debug!("INVITE_BROWSER: {:?}", msg.args);
                None
            }

            "GUILD_TEMPLATE_BROWSER" => {
                debug!("GUILD_TEMPLATE_BROWSER: {:?}", msg.args);
                None
            }

            "DEEP_LINK" => {
                debug!("DEEP_LINK: {:?}", msg.args);
                None
            }

            other => {
                warn!("Unknown RPC command: {}", other);
                None
            }
        }
    }
}

impl Default for ServerState {
    fn default() -> Self {
        let (activity_tx, _) = broadcast::channel(64);
        Self {
            next_socket_id: AtomicU64::new(1),
            activity_tx,
            sockets: RwLock::new(HashMap::new()),
        }
    }
}

/// Convert a timestamp to milliseconds if it appears to be in seconds.
///
/// Heuristic: if the current time in milliseconds has more than 2 more digits
/// than `ts`, treat `ts` as a seconds value and multiply by 1000.
/// This matches the original arRPC behaviour (`Date.now().length - ts.length > 2`).
pub fn maybe_to_ms(ts: i64) -> i64 {
    // SystemTime should always be after UNIX_EPOCH on any supported system.
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is before UNIX_EPOCH")
        .as_millis() as u64;

    let now_len = now_ms.to_string().len();
    let ts_len = (ts.unsigned_abs()).to_string().len();

    if now_len as i64 - ts_len as i64 > 2 {
        ts * 1000
    } else {
        ts
    }
}
