pub mod detectable;

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::json;
use tracing::{debug, info};

use crate::server::ServerState;
use detectable::{load_detectable, match_process, DetectableEntry};

// ── Process list ─────────────────────────────────────────────────────────────

/// Information about a single running process.
#[derive(Debug, Clone)]
pub struct ProcessInfo {
    pub pid: u32,
    /// Path to the executable (argv[0]).
    pub path: String,
    /// Additional command-line arguments.
    pub args: Vec<String>,
}

/// Read the current process list.
///
/// On Linux this reads `/proc`; on other platforms returns an empty list.
pub async fn get_process_list() -> Vec<ProcessInfo> {
    #[cfg(target_os = "linux")]
    {
        read_proc_linux().await
    }
    #[cfg(not(target_os = "linux"))]
    {
        Vec::new()
    }
}

#[cfg(target_os = "linux")]
async fn read_proc_linux() -> Vec<ProcessInfo> {
    let mut result = Vec::new();

    let mut dir = match tokio::fs::read_dir("/proc").await {
        Ok(d) => d,
        Err(_) => return result,
    };

    while let Ok(Some(entry)) = dir.next_entry().await {
        let name = entry.file_name();
        let Ok(pid) = name.to_string_lossy().parse::<u32>() else {
            continue;
        };

        let cmdline_path = format!("/proc/{}/cmdline", pid);
        let Ok(data) = tokio::fs::read(&cmdline_path).await else {
            continue;
        };

        // cmdline entries are null-separated.
        let parts: Vec<String> = data
            .split(|&b| b == 0)
            .filter(|s| !s.is_empty())
            .map(|s| String::from_utf8_lossy(s).into_owned())
            .collect();

        if let Some(path) = parts.first() {
            result.push(ProcessInfo {
                pid,
                path: path.clone(),
                args: parts[1..].to_vec(),
            });
        }
    }

    result
}

// ── Scanner ───────────────────────────────────────────────────────────────────

/// Run the process scanner loop: every 5 seconds scan running processes and
/// emit `SET_ACTIVITY` events for newly detected or lost games.
pub async fn start_process_scanner(state: Arc<ServerState>) {
    let entries = load_detectable();
    let mut active: HashMap<u32, String> = HashMap::new(); // pid → game id

    info!("Process scanner started ({} detectable entries)", entries.len());

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
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
                state
                    .handle_message(0, &entry.id, &msg)
                    .await;
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

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
