pub mod detectable;

use std::collections::HashMap;
use std::sync::Arc;

use crate::json::json;
use tracing::{debug, info};

use crate::server::ServerState;
use detectable::{DetectableEntry, load_detectable, match_process};

/// Information about a single running process.
#[derive(Debug, Clone)]
pub struct ProcessInfo {
    pub pid: u32,
    /// Path to the executable (argv[0]).
    pub path: String,
    /// Additional command-line arguments (argv[1..]).
    pub args: Vec<String>,
}

/// Read the current process list using [`sysinfo`] (cross-platform).
pub async fn get_process_list() -> Vec<ProcessInfo> {
    tokio::task::spawn_blocking(|| {
        use sysinfo::{ProcessesToUpdate, System};

        let mut sys = System::new();
        sys.refresh_processes(ProcessesToUpdate::All, false);

        sys.processes()
            .values()
            .filter_map(|proc| {
                let path = proc.exe()?.to_string_lossy().into_owned();
                if path.is_empty() {
                    return None;
                }
                let args: Vec<String> = proc
                    .cmd()
                    .iter()
                    .skip(1) // skip argv[0] (the executable itself)
                    .map(|s| s.to_string_lossy().into_owned())
                    .collect();
                Some(ProcessInfo {
                    pid: proc.pid().as_u32(),
                    path,
                    args,
                })
            })
            .collect()
    })
    .await
    .unwrap_or_default()
}

/// Run the process scanner loop: every 5 seconds scan running processes and
/// emit `SET_ACTIVITY` events for newly detected or lost games.
///
/// The detectable-apps list is loaded once at startup (from cache or network)
/// and refreshed weekly in the background.
pub async fn start_process_scanner(state: Arc<ServerState>) {
    let mut entries = load_detectable().await;
    let mut active: HashMap<u32, String> = HashMap::new();
    let mut last_refresh = std::time::Instant::now();

    info!(
        "Process scanner started ({} detectable entries)",
        entries.len()
    );

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        // Refresh the detectable list once per week.
        if last_refresh.elapsed().as_secs() > 7 * 24 * 3600 {
            entries = load_detectable().await;
            last_refresh = std::time::Instant::now();
            info!("Refreshed detectable list ({} entries)", entries.len());
        }

        scan_once(&state, &entries, &mut active).await;
    }
}

/// Single scan iteration (exposed for testing).
pub async fn scan_once(
    state: &Arc<ServerState>,
    entries: &[DetectableEntry],
    active: &mut HashMap<u32, String>,
) {
    let processes = get_process_list().await;

    let mut still_present: HashMap<u32, String> = HashMap::new();

    for proc in &processes {
        let arg_refs: Vec<&str> = proc.args.iter().map(String::as_str).collect();
        if let Some(entry) = match_process(&proc.path, &arg_refs, entries) {
            still_present.insert(proc.pid, entry.id.clone());

            // Newly detected game.
            if !active.contains_key(&proc.pid) {
                debug!(
                    "Detected game '{}' (id={}) pid={}",
                    entry.name, entry.id, proc.pid
                );
                let activity = json!({
                    "application_id": entry.id,
                    "name": entry.name,
                    "timestamps": {"start": now_ms()},
                });
                let msg = crate::types::RpcMessage {
                    cmd: "SET_ACTIVITY".to_string(),
                    args: Some(json!({
                        "pid": proc.pid,
                        "activity": activity,
                    })),
                    ..Default::default()
                };
                state.handle_message(0, &entry.id, &msg).await;
            }
        }
    }

    // Games that disappeared since last scan.
    let lost: Vec<u32> = active
        .keys()
        .filter(|pid| !still_present.contains_key(*pid))
        .copied()
        .collect();

    for pid in lost {
        debug!("Lost game pid={}", pid);
        let msg = crate::types::RpcMessage {
            cmd: "SET_ACTIVITY".to_string(),
            args: Some(json!({ "pid": pid, "activity": null })),
            ..Default::default()
        };
        let game_id = active.get(&pid).cloned().unwrap_or_default();
        state.handle_message(0, &game_id, &msg).await;
    }

    *active = still_present;
}

/// Current Unix time in milliseconds, using [`jiff`] (panic-free).
fn now_ms() -> i64 {
    jiff::Timestamp::now().as_millisecond()
}
