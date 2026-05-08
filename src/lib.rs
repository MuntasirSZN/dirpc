pub mod bridge;
pub mod json;
pub mod process;
pub mod server;
pub mod transports;
pub mod types;

pub use bridge::{BridgeState, start_bridge};
pub use process::detectable::{
    DetectableEntry, Executable, load_detectable, match_process, path_filename, path_variants,
    strip_64_suffix,
};
pub use server::{READY_PAYLOAD, ServerState, maybe_to_ms};
pub use transports::ipc::{decode, encode, ipc_path};
pub use transports::websocket::validate_origin;
pub use types::{ActivityEvent, Handshake, IpcOpcode, RpcMessage};

pub fn sample_entries() -> Vec<DetectableEntry> {
    vec![
        DetectableEntry {
            id: "359550717720469504".to_string(),
            name: "Counter-Strike: Global Offensive".to_string(),
            executables: vec![Executable {
                name: "csgo".to_string(),
                is_launcher: false,
                arguments: None,
                os: None,
            }],
        },
        DetectableEntry {
            id: "356869127241924608".to_string(),
            name: "Overwatch".to_string(),
            executables: vec![Executable {
                name: "overwatch.exe".to_string(),
                is_launcher: false,
                arguments: None,
                os: None,
            }],
        },
        DetectableEntry {
            id: "1073232715901124688".to_string(),
            name: "Counter-Strike 2".to_string(),
            executables: vec![Executable {
                name: "cs2".to_string(),
                is_launcher: false,
                arguments: None,
                os: None,
            }],
        },
    ]
}
