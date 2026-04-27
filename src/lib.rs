pub mod bridge;
pub mod process;
pub mod server;
pub mod transports;
pub mod types;

pub use bridge::{start_bridge, BridgeState};
pub use process::detectable::{load_detectable, match_process, path_filename, path_variants, strip_64_suffix};
pub use server::{maybe_to_ms, ServerState, READY_PAYLOAD};
pub use transports::ipc::{decode, encode, ipc_path};
pub use transports::websocket::validate_origin;
pub use types::{ActivityEvent, Handshake, IpcOpcode, RpcMessage};
