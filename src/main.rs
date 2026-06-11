use axum::{routing::get, Router};
use tracing::info;
use tracing_subscriber::EnvFilter;

use patchbay::config::GatewayConfig;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("patchbay=info".parse()?))
        .init();

    let cfg = GatewayConfig::load()?;
    info!(
        backends = cfg.backends.len(),
        virtual_keys = cfg.virtual_keys.len(),
        policy = ?cfg.policy,
        "configuration loaded"
    );
    for b in &cfg.backends {
        info!(
            name = %b.name,
            privacy = ?b.privacy,
            models = ?b.models,
            tags = ?b.capability_tags,
            "backend registered"
        );
    }

    // Endpoint assembly (chat completions proxy, auth, limits) lands in the
    // next phase; for now the binary validates config and serves liveness.
    let app = Router::new().route("/healthz", get(healthz));

    info!("patchbay listening on {}", cfg.listen);
    let listener = tokio::net::TcpListener::bind(cfg.listen).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn healthz() -> axum::http::StatusCode {
    axum::http::StatusCode::OK
}
