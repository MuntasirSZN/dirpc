use serde::{Deserialize, Serialize};

/// Incoming/outgoing RPC message envelope.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RpcMessage {
    pub cmd: String,
    #[serde(default)]
    pub data: serde_json::Value,
    #[serde(default)]
    pub evt: Option<String>,
    #[serde(default)]
    pub nonce: Option<String>,
    #[serde(default)]
    pub args: Option<serde_json::Value>,
}

impl Default for RpcMessage {
    fn default() -> Self {
        Self {
            cmd: String::new(),
            data: serde_json::Value::Null,
            evt: None,
            nonce: None,
            args: None,
        }
    }
}

/// Activity update emitted by the server towards the bridge.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ActivityEvent {
    pub activity: Option<serde_json::Value>,
    pub pid: Option<u32>,
    pub socket_id: u64,
}

/// IPC handshake payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Handshake {
    pub v: u32,
    pub client_id: String,
}

/// IPC wire opcodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum IpcOpcode {
    Handshake = 0,
    Frame = 1,
    Close = 2,
    Ping = 3,
    Pong = 4,
}

impl IpcOpcode {
    pub fn from_i32(v: i32) -> Option<Self> {
        match v {
            0 => Some(Self::Handshake),
            1 => Some(Self::Frame),
            2 => Some(Self::Close),
            3 => Some(Self::Ping),
            4 => Some(Self::Pong),
            _ => None,
        }
    }
}
