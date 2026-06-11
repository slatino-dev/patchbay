mod auth;
mod budget;
mod config;
mod limits;
mod metrics;
mod router;
mod server;
mod upstream;

use std::net::SocketAddr;

use axum::{Router, routing::get};
use tracing::info;
use tracing_subscriber::EnvFilter;

use config::GatewayConfig;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("patchbay=info".parse()?))
        .init();

    let cfg = GatewayConfig::load()?;
    let bind: SocketAddr = cfg.listen.parse()?;

    let app = Router::new().route("/healthz", get(healthz));

    info!("patchbay listening on {bind}");
    let listener = tokio::net::TcpListener::bind(bind).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn healthz() -> axum::http::StatusCode {
    axum::http::StatusCode::OK
}
