pub mod bridge;
pub mod process;
pub mod server;
pub mod transports;
pub mod types;

use smallvec::smallvec;

pub use bridge::{BridgeState, start_bridge};
pub use process::detectable::{
    DetectableEntry, Executable, match_process, path_filename, path_variants, strip_64_suffix,
};
pub use server::{READY_PAYLOAD, ServerState, maybe_to_ms};
#[cfg(not(target_pointer_width = "64"))]
pub use std::sync::atomic::AtomicU32 as Atomic;
#[cfg(target_pointer_width = "64")]
pub use std::sync::atomic::AtomicU64 as Atomic;
pub use transports::ipc::{decode, encode, ipc_path};
pub use transports::websocket::validate_origin;
pub use types::{ActivityEvent, Handshake, IpcOpcode, RpcMessage};

pub type HashMap<K, V> = papaya::HashMap<K, V, ahash::RandomState>;
pub type HashSet<K> = papaya::HashSet<K, ahash::RandomState>;

#[doc(hidden)]
pub fn sample_entries() -> Vec<DetectableEntry> {
    vec![
        DetectableEntry {
            id: "359550717720469504".into(),
            name: "Counter-Strike: Global Offensive".into(),
            executables: smallvec![Executable {
                name: "csgo".into(),
                is_launcher: false,
                arguments: None,
                os: None,
            }],
        },
        DetectableEntry {
            id: "356869127241924608".into(),
            name: "Overwatch".into(),
            executables: smallvec![Executable {
                name: "overwatch.exe".into(),
                is_launcher: false,
                arguments: None,
                os: None,
            }],
        },
        DetectableEntry {
            id: "1073232715901124688".into(),
            name: "Counter-Strike 2".into(),
            executables: smallvec![Executable {
                name: "cs2".into(),
                is_launcher: false,
                arguments: None,
                os: None,
            }],
        },
    ]
}
