//! Gateway configuration — loaded from `patchbay.toml` (or `PATCHBAY_CONFIG` env var path).
//!
//! TODO(phase-B):
//!   - upstream backend definitions (url, model aliases, weight, tags)
//!   - privacy routing rules (private → local-only enforcement)
//!   - per-key budget limits
//!   - fallback chain configuration
//!   - rate-limit tiers

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct GatewayConfig {
    /// Address the server will bind to, e.g. "0.0.0.0:8080"
    #[serde(default = "default_listen")]
    pub listen: String,
}

fn default_listen() -> String {
    "0.0.0.0:8080".to_string()
}

impl GatewayConfig {
    pub fn load() -> anyhow::Result<Self> {
        let path = std::env::var("PATCHBAY_CONFIG").unwrap_or_else(|_| "patchbay.toml".to_string());

        if std::path::Path::new(&path).exists() {
            let raw = std::fs::read_to_string(&path)?;
            let cfg: Self = toml::from_str(&raw)?;
            Ok(cfg)
        } else {
            // Accept missing config file — use defaults.
            Ok(Self { listen: default_listen() })
        }
    }
}
