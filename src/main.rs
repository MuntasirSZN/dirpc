use clap::Parser;
use clap_verbosity_flag::{InfoLevel, Verbosity};
use tracing::info;
use tracing_subscriber::filter::LevelFilter;

use dirpc::{
    bridge::start_bridge,
    process::start_process_scanner,
    server::ServerState,
    transports::{ipc::start_ipc_server, websocket::start_ws_server},
};

/// Discord Rich Presence server – a pure-Rust rewrite of arRPC.
#[derive(Debug, Parser)]
#[command(name = env!("CARGO_PKG_NAME"), version, about)]
pub struct Cli {
    /// Bridge WebSocket port.
    #[arg(short = 'p', long, env = "DIRPC_BRIDGE_PORT", default_value = "1337")]
    pub bridge_port: u16,

    /// Disable the IPC transport.
    #[arg(short = 'I', long, default_value = "false")]
    pub no_ipc: bool,

    /// Disable the WebSocket transport.
    #[arg(short = 'W', long, default_value = "false")]
    pub no_ws: bool,

    /// Disable the process scanner.
    #[arg(short = 'S', long, default_value = "false")]
    pub no_scanner: bool,

    #[command(flatten)]
    pub verbose: Verbosity<InfoLevel>,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Map the log::LevelFilter from clap-verbosity-flag to a tracing LevelFilter.
    let level = match cli.verbose.log_level_filter() {
        log::LevelFilter::Off => LevelFilter::OFF,
        log::LevelFilter::Error => LevelFilter::ERROR,
        log::LevelFilter::Warn => LevelFilter::WARN,
        log::LevelFilter::Info => LevelFilter::INFO,
        log::LevelFilter::Debug => LevelFilter::DEBUG,
        log::LevelFilter::Trace => LevelFilter::TRACE,
    };

    tracing_subscriber::fmt().with_max_level(level).init();

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

