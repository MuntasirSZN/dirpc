use std::sync::Arc;

use dirpc::{
    decode, encode,
    process::detectable::{match_process, path_filename, path_variants, strip_64_suffix},
    sample_entries,
    server::{READY_PAYLOAD, ServerState, maybe_to_ms},
    types::{ActivityEvent, IpcOpcode, RpcMessage},
    validate_origin,
};
use serde_json::{Value, json};
use smallvec::smallvec;

#[test]
fn test_ipc_encode_decode_roundtrip() {
    let payload = r#"{"cmd":"DISPATCH","evt":"READY"}"#;
    let encoded = encode(IpcOpcode::Frame as i32, payload);

    // Header is 8 bytes + payload bytes.
    assert_eq!(encoded.len(), 8 + payload.len());

    // Opcode field.
    let opcode = i32::from_le_bytes(encoded[0..4].try_into().unwrap());
    assert_eq!(opcode, IpcOpcode::Frame as i32);

    // Length field.
    let length = i32::from_le_bytes(encoded[4..8].try_into().unwrap()) as usize;
    assert_eq!(length, payload.len());

    // Roundtrip via decode.
    let (op, body) = decode(&encoded).expect("decode failed");
    assert_eq!(op, IpcOpcode::Frame as i32);
    assert_eq!(body, payload);
}

#[test]
fn test_ipc_encode_handshake_opcode() {
    let buf = encode(IpcOpcode::Handshake as i32, "{}");
    let opcode = i32::from_le_bytes(buf[0..4].try_into().unwrap());
    assert_eq!(opcode, 0);
}

#[test]
fn test_ipc_encode_empty_payload() {
    let buf = encode(IpcOpcode::Ping as i32, "");
    assert_eq!(buf.len(), 8);
    let length = i32::from_le_bytes(buf[4..8].try_into().unwrap());
    assert_eq!(length, 0);
}

#[test]
fn test_ipc_decode_insufficient_data() {
    // Less than 8 bytes → None.
    assert!(decode(&[0u8; 4]).is_none());
    // Exactly 8 bytes (header only) with length=0 → Some.
    let buf = encode(IpcOpcode::Pong as i32, "");
    assert!(decode(&buf).is_some());
}

#[test]
fn test_ipc_decode_truncated_body() {
    // Encode a message, then truncate the body.
    let buf = encode(IpcOpcode::Frame as i32, "hello");
    let truncated = &buf[..9]; // header + 1 byte of body
    assert!(decode(truncated).is_none());
}

#[test]
fn test_ipc_decode_invalid_utf8() {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(IpcOpcode::Frame as i32).to_le_bytes());
    buf.extend_from_slice(&(2_i32).to_le_bytes());
    buf.extend_from_slice(&[0xC3, 0x28]); // invalid UTF-8 sequence
    assert!(decode(&buf).is_none());
}

#[test]
fn test_ipc_opcode_roundtrip() {
    for (n, expected) in [
        (0, IpcOpcode::Handshake),
        (1, IpcOpcode::Frame),
        (2, IpcOpcode::Close),
        (3, IpcOpcode::Ping),
        (4, IpcOpcode::Pong),
    ] {
        assert_eq!(IpcOpcode::from_i32(n), Some(expected));
    }
    assert_eq!(IpcOpcode::from_i32(99), None);
}

#[tokio::test]
#[cfg(not(miri))]
async fn test_connections_callback() {
    let (state, _rx) = ServerState::new();
    let state = Arc::new(state);

    let msg = RpcMessage {
        cmd: "CONNECTIONS_CALLBACK".into(),
        nonce: Some("abc".into()),
        ..Default::default()
    };

    let resp = state
        .handle_message(1, "client123", &msg)
        .await
        .expect("expected a response");

    let v: Value = serde_json::from_str(&resp).unwrap();
    assert_eq!(v["cmd"], "CONNECTIONS_CALLBACK");
    assert_eq!(v["evt"], "ERROR");
    assert_eq!(v["data"]["code"], 1000);
    assert_eq!(v["nonce"], "abc");
}

#[tokio::test]
#[cfg(not(miri))]
async fn test_set_activity_null_clears_activity() {
    let (state, mut rx) = ServerState::new();
    let state = Arc::new(state);

    let msg = RpcMessage {
        cmd: "SET_ACTIVITY".into(),
        args: Some(json!({"pid": 42, "activity": null})),
        nonce: Some("n1".into()),
        ..Default::default()
    };

    let resp = state
        .handle_message(1, "cid", &msg)
        .await
        .expect("expected a response");

    let v: Value = serde_json::from_str(&resp).unwrap();
    assert_eq!(v["cmd"], "SET_ACTIVITY");
    assert_eq!(v["data"], Value::Null);
    assert_eq!(v["evt"], Value::Null);

    // An activity event with activity=None must be broadcast.
    let event = rx.try_recv().expect("expected activity event");
    assert!(event.activity.is_none());
    assert_eq!(event.pid, Some(42));
    assert_eq!(event.socket_id, 1);
}

#[tokio::test]
#[cfg(not(miri))]
async fn test_set_activity_with_activity_broadcasts_event() {
    let (state, mut rx) = ServerState::new();
    let state = Arc::new(state);

    let msg = RpcMessage {
        cmd: "SET_ACTIVITY".into(),
        args: Some(json!({
            "pid": 999,
            "activity": {
                "details": "Playing a game",
                "state": "In a match"
            }
        })),
        nonce: Some("n2".into()),
        ..Default::default()
    };

    let resp = state
        .handle_message(2, "game_id", &msg)
        .await
        .expect("expected a response");

    let v: Value = serde_json::from_str(&resp).unwrap();
    assert_eq!(v["cmd"], "SET_ACTIVITY");
    assert_eq!(v["data"]["application_id"], "game_id");
    assert_eq!(v["data"]["type"], 0);
    assert_eq!(v["evt"], Value::Null);

    let event: ActivityEvent = rx.try_recv().expect("expected broadcast event");
    let act = event.activity.expect("expected Some activity");
    assert_eq!(act["application_id"], "game_id");
    assert_eq!(act["type"], 0);
    assert_eq!(act["details"], "Playing a game");
    assert_eq!(event.pid, Some(999));
}

#[tokio::test]
#[cfg(not(miri))]
async fn test_set_activity_buttons_are_processed() {
    let (state, mut rx) = ServerState::new();
    let state = Arc::new(state);

    let msg = RpcMessage {
        cmd: "SET_ACTIVITY".into(),
        args: Some(json!({
            "pid": 1,
            "activity": {
                "buttons": [
                    {"label": "Watch Stream", "url": "https://twitch.tv/test"},
                    {"label": "Join Server", "url": "https://discord.gg/test"}
                ]
            }
        })),
        ..Default::default()
    };

    state.handle_message(3, "c", &msg).await;

    let event = rx.try_recv().unwrap();
    let act = event.activity.unwrap();

    // Buttons in activity frame should be labels only.
    let buttons = act["buttons"].as_array().unwrap();
    assert_eq!(buttons[0], "Watch Stream");
    assert_eq!(buttons[1], "Join Server");

    // URLs go into metadata.
    let urls = act["metadata"]["button_urls"].as_array().unwrap();
    assert_eq!(urls[0], "https://twitch.tv/test");
}

#[tokio::test]
#[cfg(not(miri))]
async fn test_set_activity_instance_flag() {
    let (state, mut rx) = ServerState::new();
    let state = Arc::new(state);

    let msg = RpcMessage {
        cmd: "SET_ACTIVITY".into(),
        args: Some(json!({"pid": 1, "activity": {"instance": true}})),
        ..Default::default()
    };

    state.handle_message(4, "c", &msg).await;
    let event = rx.try_recv().unwrap();
    let act = event.activity.unwrap();
    assert_eq!(act["flags"], 1u64);
}

#[tokio::test]
#[cfg(not(miri))]
async fn test_set_activity_no_instance_flag() {
    let (state, mut rx) = ServerState::new();
    let state = Arc::new(state);

    let msg = RpcMessage {
        cmd: "SET_ACTIVITY".into(),
        args: Some(json!({"pid": 1, "activity": {"instance": false}})),
        ..Default::default()
    };

    state.handle_message(5, "c", &msg).await;
    let event = rx.try_recv().unwrap();
    let act = event.activity.unwrap();
    assert_eq!(act["flags"], 0u64);
}

#[tokio::test]
#[cfg(not(miri))]
async fn test_invite_browser_returns_none() {
    let (state, _rx) = ServerState::new();
    let state = Arc::new(state);

    let msg = RpcMessage {
        cmd: "INVITE_BROWSER".into(),
        args: Some(json!({"code": "abc123"})),
        ..Default::default()
    };

    let resp = state.handle_message(1, "c", &msg).await;
    assert!(resp.is_none());
}

#[tokio::test]
#[cfg(not(miri))]
async fn test_guild_template_browser_returns_none() {
    let (state, _rx) = ServerState::new();
    let state = Arc::new(state);

    let msg = RpcMessage {
        cmd: "GUILD_TEMPLATE_BROWSER".into(),
        ..Default::default()
    };

    assert!(state.handle_message(1, "c", &msg).await.is_none());
}

#[tokio::test]
#[cfg(not(miri))]
async fn test_deep_link_returns_none() {
    let (state, _rx) = ServerState::new();
    let state = Arc::new(state);

    let msg = RpcMessage {
        cmd: "DEEP_LINK".into(),
        ..Default::default()
    };

    assert!(state.handle_message(1, "c", &msg).await.is_none());
}

#[tokio::test]
#[cfg(not(miri))]
async fn test_unknown_command_returns_none() {
    let (state, _rx) = ServerState::new();
    let state = Arc::new(state);

    let msg = RpcMessage {
        cmd: "TOTALLY_UNKNOWN".into(),
        ..Default::default()
    };

    assert!(state.handle_message(1, "c", &msg).await.is_none());
}

#[tokio::test]
#[cfg(not(miri))]
async fn test_socket_registration_and_unregistration() {
    let (state, mut rx) = ServerState::new();
    let state = Arc::new(state);

    let (tx, mut client_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    state.register_socket(99, tx).await;

    state.send_to_socket(99, "hello".to_string()).await;
    assert_eq!(client_rx.recv().await.unwrap(), "hello");

    // Unregister emits null activity event.
    state.unregister_socket(99).await;
    let event = rx.try_recv().unwrap();
    assert!(event.activity.is_none());
    assert_eq!(event.socket_id, 99);

    // No longer receives messages.
    state.send_to_socket(99, "ignored".to_string()).await;
    assert!(client_rx.try_recv().is_err());
}

#[tokio::test]
#[cfg(not(miri))]
async fn test_socket_id_increments() {
    let (state, _rx) = ServerState::new();
    let id1 = state.next_id();
    let id2 = state.next_id();
    let id3 = state.next_id();
    assert!(id1 < id2 && id2 < id3);
}

#[test]
fn test_ready_payload_is_valid_json() {
    let v: Value = serde_json::from_str(READY_PAYLOAD).expect("READY_PAYLOAD must be valid JSON");
    assert_eq!(v["cmd"], "DISPATCH");
    assert_eq!(v["evt"], "READY");
    assert_eq!(v["data"]["v"], 1);
    assert_eq!(v["data"]["user"]["username"], "arrpc");
}

#[test]
fn test_valid_origins() {
    assert!(validate_origin(""));
    assert!(validate_origin("https://discord.com"));
    assert!(validate_origin("https://ptb.discord.com"));
    assert!(validate_origin("https://canary.discord.com"));
}

#[test]
fn test_invalid_origins() {
    assert!(!validate_origin("https://evil.com"));
    assert!(!validate_origin("http://discord.com"));
    assert!(!validate_origin("https://notdiscord.com"));
    assert!(!validate_origin("https://discord.com.evil.com"));
}

#[tokio::test]
#[cfg(not(miri))]
async fn test_bridge_state_new() {
    let bridge = dirpc::bridge::BridgeState::new();
    assert!(bridge.last_msgs.is_empty());
}

#[tokio::test]
#[cfg(not(miri))]
async fn test_bridge_broadcasts_activity() {
    use dirpc::bridge::BridgeState;

    let bridge = Arc::new(BridgeState::new());
    let mut sub = bridge.tx.subscribe();

    // Simulate sending an activity payload.
    bridge
        .tx
        .send(Arc::<str>::from(r#"{"application_id":"123"}"#))
        .unwrap();

    let received = sub.recv().await.unwrap();
    assert_eq!(&*received, r#"{"application_id":"123"}"#);
}

#[tokio::test]
#[cfg(not(miri))]
async fn test_bridge_last_msgs_updated() {
    use dirpc::bridge::BridgeState;

    let bridge = Arc::new(BridgeState::new());
    bridge
        .last_msgs
        .pin()
        .insert(1, Arc::from(r#"{"application_id":"abc"}"#));
    bridge
        .last_msgs
        .pin()
        .insert(2, Arc::from(r#"{"application_id":"def"}"#));

    assert_eq!(bridge.last_msgs.len(), 2);
    let binding = bridge.last_msgs.pin();
    let val1 = binding.get(&1).unwrap();
    assert!(val1.contains("abc"));
}

#[test]
fn test_path_variants_simple() {
    let vs = path_variants("/usr/bin/csgo");
    assert!(vs.iter().any(|v| v == "csgo"));
    assert!(vs.iter().any(|v| v == "bin/csgo"));
    assert!(vs.iter().any(|v| v == "usr/bin/csgo"));
}

#[test]
fn test_path_variants_max_four_components() {
    let vs = path_variants("/a/b/c/d/e/game");
    // Only last 4 components.
    assert!(vs.iter().any(|v| v == "game"));
    assert!(vs.iter().any(|v| v == "e/game"));
    assert!(vs.iter().any(|v| v == "d/e/game"));
    assert!(vs.iter().any(|v| v == "c/d/e/game"));
    // 5-component variant should NOT appear.
    assert!(!vs.iter().any(|v| v == "b/c/d/e/game"));
}

#[test]
fn test_strip_64_suffix() {
    assert_eq!(strip_64_suffix("game64"), "game");
    assert_eq!(strip_64_suffix("gamex64"), "game");
    assert_eq!(strip_64_suffix("game_64"), "game");
    assert_eq!(strip_64_suffix("game.x64"), "game");
    assert_eq!(strip_64_suffix("game"), "game");
    // Must NOT strip "64" from the middle of a name.
    assert_eq!(strip_64_suffix("base64encoder"), "base64encoder");
}

#[test]
fn test_path_filename() {
    assert_eq!(path_filename("/usr/bin/csgo"), "csgo");
    assert_eq!(path_filename(r"C:\games\overwatch.exe"), "overwatch.exe");
    assert_eq!(path_filename("csgo"), "csgo");
    assert_eq!(path_filename("/usr/bin/csgo///"), "csgo");
    assert_eq!(path_filename(r"C:\games\overwatch.exe\\"), "overwatch.exe");
    // All-separator paths return empty string.
    assert_eq!(path_filename("///"), "");
    assert_eq!(path_filename(""), "");
}

#[test]
fn test_path_variants_includes_64_cleaned() {
    let vs = path_variants("/opt/csgo64");
    assert!(vs.iter().any(|v| v == "csgo64"));
    assert!(vs.iter().any(|v| v == "csgo")); // stripped variant
}

#[test]
fn test_match_process_found_by_filename() {
    let entries = sample_entries();
    let entry = match_process("/home/user/.steam/csgo", &[], &entries);
    assert!(entry.is_some());
    assert_eq!(entry.unwrap().id, "359550717720469504");
}

#[test]
fn test_match_process_found_win_exe() {
    let entries = sample_entries();
    let entry = match_process(r"C:\games\overwatch.exe", &[], &entries);
    assert!(entry.is_some());
    assert_eq!(entry.unwrap().name, "Overwatch");
}

#[test]
fn test_match_process_cs2() {
    let entries = sample_entries();
    let entry = match_process("/home/user/.steam/cs2", &[], &entries);
    assert!(entry.is_some());
    assert_eq!(entry.unwrap().id, "1073232715901124688");
}

#[test]
fn test_match_process_no_match() {
    let entries = sample_entries();
    let entry = match_process("/usr/bin/notepad", &[], &entries);
    assert!(entry.is_none());
}

#[test]
fn test_match_process_exact_name_prefix() {
    // Build a synthetic entry that uses '>' prefix to require exact filename match.
    use dirpc::process::detectable::{DetectableEntry, Executable};

    let entries = vec![DetectableEntry {
        id: "test".into(),
        name: "TestGame".into(),
        executables: smallvec![Executable {
            name: ">testgame".into(),
            is_launcher: false,
            arguments: None,
            os: None,
        }],
    }];

    // Any path whose last component is "testgame" should match.
    assert!(match_process("/opt/testgame", &[], &entries).is_some());
    assert!(match_process("/opt/other/testgame", &[], &entries).is_some());
    // A different filename must not match.
    assert!(match_process("/opt/othertestgame", &[], &entries).is_none());
    assert!(match_process("/opt/testgame2", &[], &entries).is_none());
}

#[test]
fn test_match_process_with_required_args() {
    use dirpc::process::detectable::{DetectableEntry, Executable};

    let entries = vec![DetectableEntry {
        id: "argtest".into(),
        name: "ArgGame".into(),
        executables: smallvec![Executable {
            name: "launcher".into(),
            is_launcher: true,
            arguments: Some(smallvec!["--game=mygame".into()]),
            os: None,
        }],
    }];

    // Without required arg → no match.
    assert!(match_process("/usr/bin/launcher", &[], &entries).is_none());
    // With required arg → match.
    assert!(match_process("/usr/bin/launcher", &["--game=mygame"], &entries).is_some());
}

#[test]
fn test_detectable_json_loads() {
    let entries = sample_entries();
    assert!(!entries.is_empty());
}

#[test]
fn test_maybe_to_ms_converts_seconds() {
    // A Unix timestamp in seconds (10 digits in 2024) should be multiplied.
    let ts_s: i64 = 1_700_000_000; // 10 digits
    let result = maybe_to_ms(ts_s);
    assert_eq!(result, ts_s * 1000);
}

#[test]
fn test_maybe_to_ms_keeps_milliseconds() {
    // A Unix timestamp already in ms (13 digits) should NOT be multiplied.
    let ts_ms: i64 = 1_700_000_000_000; // 13 digits
    let result = maybe_to_ms(ts_ms);
    assert_eq!(result, ts_ms);
}

#[tokio::test]
#[cfg(not(miri))]
async fn test_set_activity_timestamp_conversion() {
    let (state, mut rx) = ServerState::new();
    let state = Arc::new(state);

    let ts_s: i64 = 1_700_000_000;
    let msg = RpcMessage {
        cmd: "SET_ACTIVITY".into(),
        args: Some(json!({
            "pid": 1,
            "activity": {
                "timestamps": {"start": ts_s}
            }
        })),
        ..Default::default()
    };

    state.handle_message(1, "c", &msg).await;
    let event = rx.try_recv().unwrap();
    let act = event.activity.unwrap();
    assert_eq!(act["timestamps"]["start"], ts_s * 1000);
}

#[test]
fn test_ipc_path_contains_index() {
    use dirpc::ipc_path;
    let path = ipc_path(3);
    let s = path.to_string_lossy();
    assert!(s.contains("discord-ipc-3"), "unexpected path: {}", s);
}

// ─── DetectableDb integration tests ──────────────────────────────────────────

/// Return a path inside the OS temp dir that is unique to this test binary run
/// and the given label.  Using process ID + label avoids collisions between
/// concurrent test processes while keeping paths readable.
fn temp_redb_path(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "dirpc_test_{}_{}_{}.redb",
        std::process::id(),
        // Add a monotonic counter so that helper calls within the same process
        // with the same label still get distinct paths.
        {
            use dirpc::Atomic;
            use std::sync::atomic::Ordering;
            static COUNTER: Atomic = Atomic::new(0);
            COUNTER.fetch_add(1, Ordering::Relaxed)
        },
        label,
    ))
}

#[tokio::test]
#[cfg(not(miri))]
async fn test_detectable_db_empty_rebuild_is_empty() {
    use dirpc::process::detectable::DetectableDb;
    let path = temp_redb_path("empty");
    let db = DetectableDb::rebuild(&path, &[]).await.unwrap();
    assert!(db.is_empty());
    assert_eq!(db.fst_len(), 0);
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
#[cfg(not(miri))]
async fn test_detectable_db_rebuild_has_correct_fst_len() {
    use dirpc::process::detectable::DetectableDb;
    let path = temp_redb_path("fst_len");
    let entries = sample_entries();
    let db = DetectableDb::rebuild(&path, &entries).await.unwrap();
    assert!(!db.is_empty());
    // sample_entries() has 3 unique executable names: csgo, overwatch.exe, cs2
    assert_eq!(db.fst_len(), 3);
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
#[cfg(not(miri))]
async fn test_detectable_db_match_process_hit() {
    use dirpc::process::detectable::DetectableDb;
    let path = temp_redb_path("hit");
    let db = DetectableDb::rebuild(&path, &sample_entries())
        .await
        .unwrap();
    let result = db.match_process("/home/user/.steam/csgo", &[]);
    assert!(result.is_some());
    let (id, name) = result.unwrap();
    assert_eq!(id, "359550717720469504");
    assert_eq!(name, "Counter-Strike: Global Offensive");
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
#[cfg(not(miri))]
async fn test_detectable_db_match_process_miss() {
    use dirpc::process::detectable::DetectableDb;
    let path = temp_redb_path("miss");
    let db = DetectableDb::rebuild(&path, &sample_entries())
        .await
        .unwrap();
    assert!(db.match_process("/usr/bin/notepad", &[]).is_none());
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
#[cfg(not(miri))]
async fn test_detectable_db_match_windows_path() {
    use dirpc::process::detectable::DetectableDb;
    let path = temp_redb_path("win");
    let db = DetectableDb::rebuild(&path, &sample_entries())
        .await
        .unwrap();
    let result = db.match_process(r"C:\games\overwatch.exe", &[]);
    assert!(result.is_some());
    let (_, name) = result.unwrap();
    assert_eq!(name, "Overwatch");
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
#[cfg(not(miri))]
async fn test_detectable_db_match_cs2() {
    use dirpc::process::detectable::DetectableDb;
    let path = temp_redb_path("cs2");
    let db = DetectableDb::rebuild(&path, &sample_entries())
        .await
        .unwrap();
    let result = db.match_process("/home/user/.steam/cs2", &[]);
    assert!(result.is_some());
    let (id, _) = result.unwrap();
    assert_eq!(id, "1073232715901124688");
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
#[cfg(not(miri))]
async fn test_detectable_db_open_existing() {
    use dirpc::process::detectable::DetectableDb;
    let path = temp_redb_path("open");
    // Build and immediately drop (closes the file handles).
    {
        DetectableDb::rebuild(&path, &sample_entries())
            .await
            .unwrap();
    }
    // Re-open the existing file – FST must be reconstructed and matches must work.
    let db = DetectableDb::open(&path).unwrap();
    assert!(!db.is_empty());
    assert_eq!(db.fst_len(), 3);
    assert!(db.match_process("/home/user/.steam/csgo", &[]).is_some());
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
#[cfg(not(miri))]
async fn test_detectable_db_match_exact_filename_prefix() {
    use dirpc::process::detectable::{DetectableDb, DetectableEntry, Executable};
    let path = temp_redb_path("exact");
    let entries = vec![DetectableEntry {
        id: "exact_id".into(),
        name: "ExactGame".into(),
        executables: smallvec![Executable {
            name: ">exactgame".into(),
            is_launcher: false,
            arguments: None,
            os: None,
        }],
    }];
    let db = DetectableDb::rebuild(&path, &entries).await.unwrap();
    // Any path whose final component is "exactgame" should match.
    assert!(db.match_process("/opt/exactgame", &[]).is_some());
    assert!(db.match_process("/opt/other/exactgame", &[]).is_some());
    // A different filename must not match.
    assert!(db.match_process("/opt/notexactgame", &[]).is_none());
    assert!(db.match_process("/opt/exactgame2", &[]).is_none());
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
#[cfg(not(miri))]
async fn test_detectable_db_match_required_args() {
    use dirpc::process::detectable::{DetectableDb, DetectableEntry, Executable};
    let path = temp_redb_path("args");
    let entries = vec![DetectableEntry {
        id: "arg_id".into(),
        name: "ArgGame".into(),
        executables: smallvec![Executable {
            name: "launcher".into(),
            is_launcher: true,
            arguments: Some(smallvec!["--game=mygame".into()]),
            os: None,
        }],
    }];
    let db = DetectableDb::rebuild(&path, &entries).await.unwrap();
    // Without required arg → no match.
    assert!(db.match_process("/usr/bin/launcher", &[]).is_none());
    // With required arg → match.
    assert!(
        db.match_process("/usr/bin/launcher", &["--game=mygame"])
            .is_some()
    );
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
#[cfg(not(miri))]
async fn test_detectable_db_rebuild_is_idempotent() {
    use dirpc::process::detectable::DetectableDb;
    let path = temp_redb_path("idempotent");
    // First build.
    let db1 = DetectableDb::rebuild(&path, &sample_entries())
        .await
        .unwrap();
    let len1 = db1.fst_len();
    drop(db1);
    // Rebuild with the same entries – must overwrite the old file cleanly.
    let db2 = DetectableDb::rebuild(&path, &sample_entries())
        .await
        .unwrap();
    assert_eq!(db2.fst_len(), len1);
    assert!(db2.match_process("/home/user/.steam/csgo", &[]).is_some());
    let _ = std::fs::remove_file(&path);
}
