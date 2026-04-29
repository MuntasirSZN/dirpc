use clap::Parser;
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt};

use dirpc::{
    bridge::start_bridge,
    process::start_process_scanner,
    server::ServerState,
    transports::{ipc::start_ipc_server, websocket::start_ws_server},
};

/// Discord Rich Presence server – a Rust rewrite of arrpc.
#[derive(Debug, Parser)]
#[command(name = "dirpc", version, about)]
pub struct Cli {
    /// Bridge WebSocket port (env: DIRPC_BRIDGE_PORT).
    #[arg(long, env = "DIRPC_BRIDGE_PORT", default_value = "1337")]
    pub bridge_port: u16,

    /// Disable the IPC transport.
    #[arg(long, default_value = "false")]
    pub no_ipc: bool,

    /// Disable the WebSocket transport.
    #[arg(long, default_value = "false")]
    pub no_ws: bool,

    /// Disable the process scanner.
    #[arg(long, default_value = "false")]
    pub no_scanner: bool,

    /// Log level filter (e.g. debug, info, warn).
    #[arg(long, env = "RUST_LOG", default_value = "info")]
    pub log_level: String,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    fmt().with_env_filter(EnvFilter::new(&cli.log_level)).init();

    info!("Starting dirpc (bridge port {})", cli.bridge_port);

    let (state, activity_rx) = ServerState::new();

    // Bridge.
    let bridge_port = cli.bridge_port;
    tokio::spawn(async move {
        if let Err(e) = start_bridge(activity_rx, bridge_port).await {
            tracing::error!("Bridge error: {}", e);
        }
    });

    // Process scanner.
    if !cli.no_scanner {
        let state_scan = state.clone();
        tokio::spawn(async move {
            start_process_scanner(state_scan).await;
        });
    }

    // IPC transport.
    if !cli.no_ipc {
        let state_ipc = state.clone();
        tokio::spawn(async move {
            if let Err(e) = start_ipc_server(state_ipc).await {
                tracing::error!("IPC server error: {}", e);
            }
        });
    }

    // WebSocket transport (runs on the main task so we block here).
    if !cli.no_ws {
        if let Err(e) = start_ws_server(state).await {
            tracing::error!("WS server error: {}", e);
        }
    } else {
        // Nothing else to keep the process alive.
        std::future::pending::<()>().await;
    }
}
