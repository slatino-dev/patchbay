use tracing::info;
use tracing_subscriber::EnvFilter;

use patchbay::config::GatewayConfig;
use patchbay::server::{build_router, AppState};

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

    let state = AppState::from_config(&cfg)?;
    let app = build_router(state);

    info!("patchbay listening on {}", cfg.listen);
    let listener = tokio::net::TcpListener::bind(cfg.listen).await?;

    // Graceful shutdown on SIGINT (Ctrl-C).
    let shutdown = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl-C handler");
        info!("shutdown signal received");
    };

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await?;

    Ok(())
}
