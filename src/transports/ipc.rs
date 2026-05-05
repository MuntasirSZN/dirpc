use std::path::PathBuf;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, error, info, warn};

use crate::server::{READY_PAYLOAD, ServerState};
use crate::types::{Handshake, IpcOpcode};

// ── Wire helpers ────────────────────────────────────────────────────────────

/// Encode an IPC frame: 4-byte LE opcode + 4-byte LE length + payload bytes.
pub fn encode(opcode: i32, data: &str) -> Vec<u8> {
    let bytes = data.as_bytes();
    let len = bytes.len() as i32;
    let mut buf = Vec::with_capacity(8 + bytes.len());
    buf.extend_from_slice(&opcode.to_le_bytes());
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(bytes);
    buf
}

/// Decode a complete IPC frame from a byte slice (must include header + body).
pub fn decode(data: &[u8]) -> Option<(i32, String)> {
    if data.len() < 8 {
        return None;
    }
    let opcode = i32::from_le_bytes(data[0..4].try_into().ok()?);
    let length = i32::from_le_bytes(data[4..8].try_into().ok()?) as usize;
    if data.len() < 8 + length {
        return None;
    }
    let body = std::str::from_utf8(&data[8..8 + length]).ok()?.to_string();
    Some((opcode, body))
}

/// Read one IPC frame asynchronously from `reader`.
async fn read_frame<R: AsyncReadExt + Unpin>(reader: &mut R) -> std::io::Result<(i32, Vec<u8>)> {
    let mut header = [0u8; 8];
    reader.read_exact(&mut header).await?;
    let opcode = i32::from_le_bytes(header[0..4].try_into().unwrap());
    let length = usize::try_from(u32::from_le_bytes(header[4..8].try_into().unwrap()))
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let mut body = vec![0u8; length];
    reader.read_exact(&mut body).await?;
    Ok((opcode, body))
}

// ── Socket / pipe paths ───────────────────────────────────────────────────────

/// Build the IPC path for `discord-ipc-{n}`.
///
/// | Platform      | Path format                                                |
/// |---------------|------------------------------------------------------------|
/// | Windows       | `\\.\pipe\discord-ipc-{n}`                                 |
/// | Linux / macOS | `$XDG_RUNTIME_DIR/discord-ipc-{n}` (or TMPDIR/TMP/TEMP/tmp)|
#[cfg(windows)]
pub fn ipc_path(n: u8) -> PathBuf {
    PathBuf::from(format!(r"\\.\pipe\discord-ipc-{}", n))
}

/// Unix variant: resolve the base directory following Discord's documented
/// fallback order: XDG_RUNTIME_DIR → TMPDIR → TMP → TEMP → /tmp.
#[cfg(unix)]
pub fn ipc_path(n: u8) -> PathBuf {
    let base = std::env::var("XDG_RUNTIME_DIR")
        .or_else(|_| std::env::var("TMPDIR"))
        .or_else(|_| std::env::var("TMP"))
        .or_else(|_| std::env::var("TEMP"))
        .unwrap_or_else(|_| "/tmp".to_string());
    PathBuf::from(format!("{}/discord-ipc-{}", base, n))
}

/// Fallback stub for platforms other than Unix and Windows.
#[cfg(not(any(unix, windows)))]
pub fn ipc_path(n: u8) -> PathBuf {
    std::env::temp_dir().join(format!("discord-ipc-{}", n))
}

// ── Shared connection handler ─────────────────────────────────────────────────

/// Drive a fully-connected IPC client: perform the Discord RPC handshake, then
/// run the read/write loop until the connection closes.
///
/// Generic over the reader/writer halves so the same code path handles both
/// Unix domain sockets and Windows named pipes.
async fn handle_ipc_io<R, W>(mut reader: R, mut writer: W, state: Arc<ServerState>)
where
    R: AsyncReadExt + Unpin,
    W: AsyncWriteExt + Unpin,
{
    // ── Step 1: expect HANDSHAKE ───────────────────────────────────────────
    let (opcode, mut body) = match read_frame(&mut reader).await {
        Ok(f) => f,
        Err(e) => {
            debug!("IPC read error during handshake: {}", e);
            return;
        }
    };

    if IpcOpcode::from_i32(opcode) != Some(IpcOpcode::Handshake) {
        warn!("Expected HANDSHAKE opcode, got {}", opcode);
        return;
    }

    let handshake: Handshake = match crate::json::from_slice(&mut body) {
        Ok(h) => h,
        Err(e) => {
            warn!("Invalid handshake JSON: {}", e);
            return;
        }
    };

    if handshake.v != 1 {
        warn!("Unsupported IPC version {}", handshake.v);
        return;
    }

    let client_id = handshake.client_id.clone();
    let socket_id = state.next_id();
    debug!(
        "IPC HANDSHAKE: client_id={} socket_id={}",
        client_id, socket_id
    );

    // ── Step 2: send DISPATCH/READY ────────────────────────────────────────
    let ready = encode(IpcOpcode::Frame as i32, READY_PAYLOAD);
    if let Err(e) = writer.write_all(&ready).await {
        warn!("Failed to send READY: {}", e);
        return;
    }

    // ── Step 3: register + main loop ───────────────────────────────────────
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    state.register_socket(socket_id, tx).await;

    loop {
        tokio::select! {
            // Outbound: flush any pending server → client messages.
            msg = rx.recv() => {
                match msg {
                    Some(json) => {
                        let frame = encode(IpcOpcode::Frame as i32, &json);
                        if let Err(e) = writer.write_all(&frame).await {
                            debug!("IPC write error: {}", e);
                            break;
                        }
                    }
                    None => break,
                }
            }
            // Inbound: read next frame from client.
            result = read_frame(&mut reader) => {
                match result {
                    Ok((op, mut body)) => {
                        match IpcOpcode::from_i32(op) {
                            Some(IpcOpcode::Ping) => {
                                let pong = encode(IpcOpcode::Pong as i32, "");
                                let _ = writer.write_all(&pong).await;
                            }
                            Some(IpcOpcode::Frame) => {
                                match crate::json::from_slice::<crate::types::RpcMessage>(&mut body) {
                                    Ok(msg) => {
                                        if let Some(resp) =
                                            state.handle_message(socket_id, &client_id, &msg).await
                                        {
                                            state.send_to_socket(socket_id, resp).await;
                                        }
                                    }
                                    Err(e) => warn!("Bad IPC JSON: {}", e),
                                }
                            }
                            Some(IpcOpcode::Close) | None => break,
                            _ => {}
                        }
                    }
                    Err(e) => {
                        debug!("IPC read error: {}", e);
                        break;
                    }
                }
            }
        }
    }

    state.unregister_socket(socket_id).await;
    debug!("IPC connection closed for socket_id={}", socket_id);
}

// ── Unix implementation ───────────────────────────────────────────────────────

/// Start the IPC transport using Unix domain sockets.
#[cfg(unix)]
pub async fn start_ipc_server(state: Arc<ServerState>) -> std::io::Result<()> {
    let (listener, path) = find_available_socket().await.ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::AddrInUse,
            "No available discord-ipc socket path found (0-9 all taken)",
        )
    })?;

    info!("IPC server listening on {:?}", path);

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let state_clone = state.clone();
                tokio::spawn(async move {
                    let (reader, writer) = tokio::io::split(stream);
                    handle_ipc_io(reader, writer, state_clone).await;
                });
            }
            Err(e) => {
                error!("IPC accept error: {}", e);
            }
        }
    }
}

/// Find the first socket path (0-9) where no one is already listening.
///
/// Tests a path by attempting a connection; if that fails the path is free.
/// Cleans up a stale socket file before binding.
#[cfg(unix)]
pub async fn find_available_socket() -> Option<(tokio::net::UnixListener, PathBuf)> {
    use tokio::net::UnixStream;

    for n in 0u8..10 {
        let path = ipc_path(n);

        // Probe: if we can connect, someone is already there.
        match tokio::time::timeout(
            std::time::Duration::from_millis(200),
            UnixStream::connect(&path),
        )
        .await
        {
            Ok(Ok(_)) => {
                debug!("IPC path {:?} already taken, skipping", path);
                continue;
            }
            _ => {
                // Not connectable – clean up stale file and try to bind.
                let _ = tokio::fs::remove_file(&path).await;
                match tokio::net::UnixListener::bind(&path) {
                    Ok(listener) => return Some((listener, path)),
                    Err(e) => {
                        warn!("Could not bind {:?}: {}", path, e);
                        continue;
                    }
                }
            }
        }
    }
    None
}

// ── Windows implementation ────────────────────────────────────────────────────

/// Start the IPC transport using Windows named pipes.
#[cfg(windows)]
pub async fn start_ipc_server(state: Arc<ServerState>) -> std::io::Result<()> {
    use tokio::net::windows::named_pipe::ServerOptions;

    let (mut server, pipe_name) = find_available_pipe().await.ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::AddrInUse,
            "No available discord-ipc named pipe found (0-9 all taken)",
        )
    })?;

    info!("IPC server listening on {}", pipe_name);

    loop {
        // Wait for the next client to connect.
        server.connect().await?;

        // Take the connected pipe instance and immediately create a new one
        // for the next connection, so we can keep accepting.
        let connected = server;
        server = ServerOptions::new().create(&pipe_name).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to create next pipe instance: {e}"),
            )
        })?;

        let state_clone = state.clone();
        tokio::spawn(async move {
            let (reader, writer) = tokio::io::split(connected);
            handle_ipc_io(reader, writer, state_clone).await;
        });
    }
}

/// Try to create a `first_pipe_instance` named pipe for each path 0-9, returning
/// the first one that succeeds (i.e. not already claimed by a running Discord client).
#[cfg(windows)]
async fn find_available_pipe() -> Option<(tokio::net::windows::named_pipe::NamedPipeServer, String)>
{
    use tokio::net::windows::named_pipe::ServerOptions;

    for n in 0u8..10 {
        let pipe_name = ipc_path(n).to_string_lossy().into_owned();
        match ServerOptions::new()
            .first_pipe_instance(true)
            .create(&pipe_name)
        {
            Ok(server) => return Some((server, pipe_name)),
            Err(e) => {
                debug!("Pipe {} unavailable: {}", pipe_name, e);
            }
        }
    }
    None
}

// ── Stub for other platforms ──────────────────────────────────────────────────

/// No-op IPC server for platforms other than Unix and Windows.
#[cfg(not(any(unix, windows)))]
pub async fn start_ipc_server(_state: Arc<ServerState>) -> std::io::Result<()> {
    warn!("IPC transport is not supported on this platform");
    std::future::pending::<()>().await;
    Ok(())
}
