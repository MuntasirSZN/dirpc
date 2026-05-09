pub mod detectable;

use std::collections::HashMap;
use std::sync::Arc;

use crate::json::json;
use tracing::{debug, info, warn};

use crate::server::ServerState;
use detectable::{DetectableDb, cache_db_path, load_detectable_entries};

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
/// ## Startup
/// 1. Try to open an existing `redb` file; if it is populated, skip a network
///    round-trip entirely (the mmap-backed database is already on disk).
/// 2. Otherwise fetch the Discord detectable list, write it into redb, and
///    build the in-memory FST.
///
/// ## Refresh
/// Once per week the list is re-fetched.  A 304 Not-Modified response leaves
/// the existing database untouched.
pub async fn start_process_scanner(state: Arc<ServerState>) {
    let db_path = cache_db_path();

    // ── Initial load ─────────────────────────────────────────────────────────
    let mut db = match DetectableDb::open(&db_path) {
        Ok(d) if !d.is_empty() => {
            info!(
                "Process scanner started (redb open, {} exe names in FST)",
                d.fst_len()
            );
            d
        }
        _ => {
            info!("Building detectable database from Discord API…");
            let entries = load_detectable_entries().await;
            match DetectableDb::rebuild(&db_path, &entries).await {
                Ok(d) => {
                    info!(
                        "Process scanner started ({} entries, {} exe names in FST)",
                        entries.len(),
                        d.fst_len()
                    );
                    d
                }
                Err(e) => {
                    warn!("Failed to build detectable database: {e}; game detection disabled");
                    return;
                }
            }
        }
    };

    let mut active: HashMap<u32, String> = HashMap::new();
    let mut last_refresh = std::time::Instant::now();

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        // ── Weekly refresh ───────────────────────────────────────────────────
        if last_refresh.elapsed().as_secs() > 7 * 24 * 3600 {
            let entries = load_detectable_entries().await;
            if entries.is_empty() {
                // 304 Not-Modified or network error: keep using the existing db.
                debug!("Detectable list unchanged (304 or network error); keeping existing db");
            } else {
                match DetectableDb::rebuild(&db_path, &entries).await {
                    Ok(new_db) => {
                        info!(
                            "Refreshed detectable database ({} entries, {} exe names in FST)",
                            entries.len(),
                            new_db.fst_len()
                        );
                        db = new_db;
                    }
                    Err(e) => warn!("Failed to refresh detectable database: {e}"),
                }
            }
            last_refresh = std::time::Instant::now();
        }

        scan_once(&state, &db, &mut active).await;
    }
}

/// Single scan iteration (exposed for testing).
pub async fn scan_once(
    state: &Arc<ServerState>,
    db: &DetectableDb,
    active: &mut HashMap<u32, String>,
) {
    let processes = get_process_list().await;

    let mut still_present: HashMap<u32, String> = HashMap::new();

    for proc in &processes {
        let arg_refs: Vec<&str> = proc.args.iter().map(String::as_str).collect();
        if let Some((id, name)) = db.match_process(&proc.path, &arg_refs) {
            still_present.insert(proc.pid, id.clone());

            // Newly detected game.
            if !active.contains_key(&proc.pid) {
                debug!("Detected game '{}' (id={}) pid={}", name, id, proc.pid);
                let activity = json!({
                    "application_id": id,
                    "name": name,
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
                state.handle_message(0, &id, &msg).await;
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
