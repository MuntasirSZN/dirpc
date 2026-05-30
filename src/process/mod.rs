pub mod detectable;

use ahash::{AHashMap, AHashSet};
use compact_str::CompactString;
use smallvec::SmallVec;
use std::sync::Arc;

use serde_json::json;
use tracing::{debug, info, warn};

use crate::server::ServerState;
use detectable::{DetectableDb, cache_db_path, load_detectable_entries};

/// Information about a single running process.
#[derive(Debug, Clone)]
pub struct ProcessInfo {
    pub pid: u32,
    /// Path to the executable (argv[0]).
    pub path: CompactString,
    /// Additional command-line arguments (argv[1..]).
    pub args: SmallVec<[CompactString; 8]>,
}

/// Read the current process list using [`sysinfo`] (cross-platform).
pub async fn get_process_list() -> Vec<ProcessInfo> {
    use sysinfo::System;
    let (_sys, processes) = get_process_list_with_system(System::new()).await;
    processes
}

async fn get_process_list_with_system(
    mut sys: sysinfo::System,
) -> (sysinfo::System, Vec<ProcessInfo>) {
    tokio::task::spawn_blocking(|| {
        use sysinfo::ProcessesToUpdate;
        sys.refresh_processes(ProcessesToUpdate::All, false);

        let processes = sys
            .processes()
            .values()
            .filter_map(|proc| {
                let path = proc.exe()?.to_string_lossy().into_owned();
                if path.is_empty() {
                    return None;
                }
                let args: SmallVec<[CompactString; 8]> = proc
                    .cmd()
                    .iter()
                    .skip(1) // skip argv[0] (the executable itself)
                    .map(|s| CompactString::from(s.to_string_lossy().as_ref()))
                    .collect();
                Some(ProcessInfo {
                    pid: proc.pid().as_u32(),
                    path: CompactString::from(path.as_str()),
                    args,
                })
            })
            .collect();
        (sys, processes)
    })
    .await
    .unwrap_or_else(|_| (sysinfo::System::new(), Vec::new()))
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
    use sysinfo::System;

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

    // pid → app_id for currently-active games.
    let mut active: AHashMap<u32, CompactString> = AHashMap::default();
    let mut last_refresh = std::time::Instant::now();
    let mut sys = System::new();

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

        let (next_sys, processes) = get_process_list_with_system(sys).await;
        sys = next_sys;
        scan_once(&state, &db, &mut active, &processes).await;
    }
}

/// Single scan iteration (exposed for testing).
pub async fn scan_once(
    state: &Arc<ServerState>,
    db: &DetectableDb,
    active: &mut AHashMap<u32, CompactString>,
    processes: &[ProcessInfo],
) {
    let mut present: AHashSet<u32> = AHashSet::with_capacity(processes.len());
    let mut lost: SmallVec<[(u32, CompactString); 16]> = SmallVec::new();

    for proc in processes {
        if let Some((id, name)) = db.match_process_compact(&proc.path, &proc.args) {
            present.insert(proc.pid);

            // Newly detected game.
            let already_tracked_with_same_id = active
                .get(&proc.pid)
                .is_some_and(|prev| prev.as_str() == id.as_str());
            if !already_tracked_with_same_id {
                debug!("Detected game '{}' (id={}) pid={}", name, id, proc.pid);
                let activity = json!({
                    "application_id": id,
                    "name": name,
                    "timestamps": {"start": now_ms()},
                });
                let msg = crate::types::RpcMessage {
                    cmd: "SET_ACTIVITY".into(),
                    args: Some(json!({
                        "pid": proc.pid,
                        "activity": activity,
                    })),
                    ..Default::default()
                };
                state.handle_message(0, &id, &msg).await;
            }

            active.insert(proc.pid, id);
        }
    }

    active.retain(|pid, game_id| {
        if present.contains(pid) {
            true
        } else {
            lost.push((*pid, game_id.clone()));
            false
        }
    });

    for (pid, game_id) in lost {
        debug!("Lost game pid={}", pid);
        let msg = crate::types::RpcMessage {
            cmd: "SET_ACTIVITY".into(),
            args: Some(json!({ "pid": pid, "activity": null })),
            ..Default::default()
        };
        state.handle_message(0, &game_id, &msg).await;
    }
}

/// Current Unix time in milliseconds, using [`jiff`] (panic-free).
fn now_ms() -> i64 {
    jiff::Timestamp::now().as_millisecond()
}
