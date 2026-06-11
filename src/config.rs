//! Gateway configuration — loaded from `patchbay.toml` (or the path in the
//! `PATCHBAY_CONFIG` environment variable).
//!
//! Secrets never live in the file. Backends reference *environment variable
//! names* (`base_url_env`, `api_key_env`) and virtual keys reference
//! `key_env`; all of them are resolved exactly once at load time. The
//! resolved [`GatewayConfig`] is fully validated — unknown fields, duplicate
//! names, empty model lists, malformed URLs, bad policy parameters, and
//! missing environment variables are all load-time errors — so a gateway
//! that boots has a coherent routing table.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::net::SocketAddr;
use std::path::Path;

use serde::Deserialize;

/// Data-placement class of a backend.
///
/// `Local` backends are allowed to see private traffic; `External` backends
/// never are. The router enforces this in the type system — see
/// [`crate::router`] for the witness-type construction that makes it
/// impossible (not merely "checked") for a private request to select an
/// `External` backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Privacy {
    /// Runs on infrastructure the operator controls; may serve private traffic.
    Local,
    /// Third-party hosted; must never see traffic marked private.
    External,
}

/// A secret resolved from the environment (API key, virtual key).
///
/// Wrapper exists so secrets cannot leak through `Debug`/`Display` in logs;
/// access requires an explicit [`Secret::expose`] call.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Deliberately explicit accessor — grep for `expose()` to audit usage.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

/// A fully resolved upstream backend.
#[derive(Debug, Clone)]
pub struct Backend {
    pub name: String,
    /// Resolved from `base_url_env`, normalized without a trailing slash.
    pub base_url: String,
    /// Resolved from `api_key_env`; `None` for keyless (e.g. LAN) backends.
    pub api_key: Option<Secret>,
    /// Model identifiers this backend serves (exact match).
    pub models: Vec<String>,
    /// Capabilities used for tag-constrained routing (e.g. `code`, `fast`).
    pub capability_tags: Vec<String>,
    pub privacy: Privacy,
}

impl Backend {
    pub fn serves_model(&self, model: &str) -> bool {
        self.models.iter().any(|m| m == model)
    }

    /// True if every required tag is present in this backend's capabilities.
    pub fn has_capability_tags<S: AsRef<str>>(&self, required: &[S]) -> bool {
        required
            .iter()
            .all(|t| self.capability_tags.iter().any(|c| c == t.as_ref()))
    }
}

/// A client-facing key the gateway accepts, resolved from the environment.
#[derive(Debug, Clone)]
pub struct VirtualKey {
    pub name: String,
    pub key: Secret,
    /// When set, every request authenticated with this key is routed as
    /// private (local-only), regardless of per-request markers.
    pub enforce_private: bool,
}

/// Which routing policy the gateway runs with.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PolicySelection {
    /// Pick the first eligible backend in config order.
    StaticPriority,
    /// Pick the eligible backend with the lowest EWMA latency.
    EwmaLatency { alpha: f64 },
}

/// The validated, fully resolved gateway configuration.
#[derive(Debug, Clone)]
pub struct GatewayConfig {
    pub listen: SocketAddr,
    pub backends: Vec<Backend>,
    pub virtual_keys: Vec<VirtualKey>,
    pub policy: PolicySelection,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("config file not found at `{0}` (set PATCHBAY_CONFIG or create patchbay.toml)")]
    NotFound(String),
    #[error("cannot read config file `{path}`: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid TOML: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("invalid listen address `{addr}`: {source}")]
    InvalidListen {
        addr: String,
        #[source]
        source: std::net::AddrParseError,
    },
    #[error("config defines no backends — at least one [[backends]] entry is required")]
    NoBackends,
    #[error("backend name must not be empty")]
    EmptyBackendName,
    #[error("duplicate backend name `{0}`")]
    DuplicateBackend(String),
    #[error("backend `{name}`: `{field}` is not a valid environment variable name: `{value}`")]
    InvalidEnvName {
        name: String,
        field: &'static str,
        value: String,
    },
    #[error("backend `{name}`: environment variable `{var}` (from `{field}`) is not set")]
    MissingEnv {
        name: String,
        field: &'static str,
        var: String,
    },
    #[error("backend `{name}`: base URL `{url}` must start with http:// or https://")]
    InvalidBaseUrl { name: String, url: String },
    #[error("backend `{0}`: `models` must list at least one model")]
    NoModels(String),
    #[error("backend `{name}`: `{field}` contains an empty string")]
    EmptyEntry { name: String, field: &'static str },
    #[error("virtual key name must not be empty")]
    EmptyVirtualKeyName,
    #[error("duplicate virtual key name `{0}`")]
    DuplicateVirtualKey(String),
    #[error("virtual key `{name}`: `key_env` is not a valid environment variable name: `{value}`")]
    InvalidKeyEnvName { name: String, value: String },
    #[error("virtual key `{name}`: environment variable `{var}` is not set")]
    MissingKeyEnv { name: String, var: String },
    #[error("virtual key `{0}`: resolved key is empty")]
    EmptyVirtualKey(String),
    #[error("virtual keys `{0}` and `{1}` resolve to the same secret")]
    DuplicateVirtualKeySecret(String, String),
    #[error("policy `ewma_latency`: alpha must be in (0, 1], got {0}")]
    InvalidAlpha(f64),
}

// ---------------------------------------------------------------------------
// Raw (on-disk) representation — deserialized, then resolved + validated.
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    #[serde(default = "default_listen")]
    listen: String,
    #[serde(default)]
    backends: Vec<RawBackend>,
    #[serde(default)]
    virtual_keys: Vec<RawVirtualKey>,
    #[serde(default)]
    policy: RawPolicy,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawBackend {
    name: String,
    base_url_env: String,
    #[serde(default)]
    api_key_env: Option<String>,
    models: Vec<String>,
    #[serde(default)]
    capability_tags: Vec<String>,
    privacy: Privacy,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawVirtualKey {
    name: String,
    key_env: String,
    #[serde(default)]
    enforce_private: bool,
}

#[derive(Debug, Default, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum RawPolicy {
    #[default]
    StaticPriority,
    EwmaLatency {
        #[serde(default = "default_alpha")]
        alpha: f64,
    },
}

fn default_listen() -> String {
    "0.0.0.0:8080".to_string()
}

fn default_alpha() -> f64 {
    0.3
}

/// POSIX-ish env var name: `[A-Za-z_][A-Za-z0-9_]*`.
fn is_valid_env_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

impl GatewayConfig {
    /// Load from the path in `PATCHBAY_CONFIG` (default `patchbay.toml`),
    /// resolving secrets from the real process environment.
    pub fn load() -> Result<Self, ConfigError> {
        let path = std::env::var("PATCHBAY_CONFIG").unwrap_or_else(|_| "patchbay.toml".to_string());
        if !Path::new(&path).exists() {
            return Err(ConfigError::NotFound(path));
        }
        let raw = std::fs::read_to_string(&path).map_err(|source| ConfigError::Io {
            path: path.clone(),
            source,
        })?;
        Self::from_toml_str(&raw, |var| std::env::var(var).ok())
    }

    /// Parse + resolve + validate from a TOML string, with an injectable
    /// environment lookup (so tests never mutate the process environment).
    pub fn from_toml_str(
        raw: &str,
        env: impl Fn(&str) -> Option<String>,
    ) -> Result<Self, ConfigError> {
        let raw: RawConfig = toml::from_str(raw)?;

        let listen: SocketAddr =
            raw.listen
                .parse()
                .map_err(|source| ConfigError::InvalidListen {
                    addr: raw.listen.clone(),
                    source,
                })?;

        if raw.backends.is_empty() {
            return Err(ConfigError::NoBackends);
        }

        let mut backend_names: HashSet<&str> = HashSet::new();
        let mut backends = Vec::with_capacity(raw.backends.len());
        for b in &raw.backends {
            if b.name.is_empty() {
                return Err(ConfigError::EmptyBackendName);
            }
            if !backend_names.insert(b.name.as_str()) {
                return Err(ConfigError::DuplicateBackend(b.name.clone()));
            }

            if !is_valid_env_name(&b.base_url_env) {
                return Err(ConfigError::InvalidEnvName {
                    name: b.name.clone(),
                    field: "base_url_env",
                    value: b.base_url_env.clone(),
                });
            }
            let base_url = env(&b.base_url_env).ok_or_else(|| ConfigError::MissingEnv {
                name: b.name.clone(),
                field: "base_url_env",
                var: b.base_url_env.clone(),
            })?;
            if !(base_url.starts_with("http://") || base_url.starts_with("https://")) {
                return Err(ConfigError::InvalidBaseUrl {
                    name: b.name.clone(),
                    url: base_url,
                });
            }

            let api_key = match &b.api_key_env {
                None => None,
                Some(var) => {
                    if !is_valid_env_name(var) {
                        return Err(ConfigError::InvalidEnvName {
                            name: b.name.clone(),
                            field: "api_key_env",
                            value: var.clone(),
                        });
                    }
                    let key = env(var).ok_or_else(|| ConfigError::MissingEnv {
                        name: b.name.clone(),
                        field: "api_key_env",
                        var: var.clone(),
                    })?;
                    Some(Secret::new(key))
                }
            };

            if b.models.is_empty() {
                return Err(ConfigError::NoModels(b.name.clone()));
            }
            if b.models.iter().any(String::is_empty) {
                return Err(ConfigError::EmptyEntry {
                    name: b.name.clone(),
                    field: "models",
                });
            }
            if b.capability_tags.iter().any(String::is_empty) {
                return Err(ConfigError::EmptyEntry {
                    name: b.name.clone(),
                    field: "capability_tags",
                });
            }

            backends.push(Backend {
                name: b.name.clone(),
                base_url: base_url.trim_end_matches('/').to_string(),
                api_key,
                models: b.models.clone(),
                capability_tags: b.capability_tags.clone(),
                privacy: b.privacy,
            });
        }

        let mut vk_names: HashSet<&str> = HashSet::new();
        // resolved secret -> name of the key that introduced it
        let mut vk_secrets: HashMap<String, String> = HashMap::new();
        let mut virtual_keys = Vec::with_capacity(raw.virtual_keys.len());
        for vk in &raw.virtual_keys {
            if vk.name.is_empty() {
                return Err(ConfigError::EmptyVirtualKeyName);
            }
            if !vk_names.insert(vk.name.as_str()) {
                return Err(ConfigError::DuplicateVirtualKey(vk.name.clone()));
            }
            if !is_valid_env_name(&vk.key_env) {
                return Err(ConfigError::InvalidKeyEnvName {
                    name: vk.name.clone(),
                    value: vk.key_env.clone(),
                });
            }
            let key = env(&vk.key_env).ok_or_else(|| ConfigError::MissingKeyEnv {
                name: vk.name.clone(),
                var: vk.key_env.clone(),
            })?;
            if key.is_empty() {
                return Err(ConfigError::EmptyVirtualKey(vk.name.clone()));
            }
            if let Some(prev) = vk_secrets.insert(key.clone(), vk.name.clone()) {
                return Err(ConfigError::DuplicateVirtualKeySecret(
                    prev,
                    vk.name.clone(),
                ));
            }
            virtual_keys.push(VirtualKey {
                name: vk.name.clone(),
                key: Secret::new(key),
                enforce_private: vk.enforce_private,
            });
        }

        let policy = match raw.policy {
            RawPolicy::StaticPriority => PolicySelection::StaticPriority,
            RawPolicy::EwmaLatency { alpha } => {
                if !(alpha > 0.0 && alpha <= 1.0) {
                    return Err(ConfigError::InvalidAlpha(alpha));
                }
                PolicySelection::EwmaLatency { alpha }
            }
        };

        Ok(Self {
            listen,
            backends,
            virtual_keys,
            policy,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build an env lookup over a static table.
    fn env<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |k| {
            pairs
                .iter()
                .find(|(name, _)| *name == k)
                .map(|(_, v)| (*v).to_string())
        }
    }

    const FULL: &str = r#"
        listen = "127.0.0.1:9000"

        [policy]
        kind = "ewma_latency"
        alpha = 0.5

        [[backends]]
        name = "local-llm"
        base_url_env = "LLM_BASE_URL"
        models = ["qwen-coder", "qwen-chat"]
        capability_tags = ["code", "fast"]
        privacy = "local"

        [[backends]]
        name = "openai"
        base_url_env = "OPENAI_BASE_URL"
        api_key_env = "OPENAI_API_KEY"
        models = ["gpt-4o"]
        capability_tags = ["general"]
        privacy = "external"

        [[virtual_keys]]
        name = "dev"
        key_env = "PATCHBAY_KEY_DEV"
        enforce_private = true
    "#;

    const FULL_ENV: &[(&str, &str)] = &[
        ("LLM_BASE_URL", "http://127.0.0.1:8000/"),
        ("OPENAI_BASE_URL", "https://api.example.com"),
        ("OPENAI_API_KEY", "test-key-abc"),
        ("PATCHBAY_KEY_DEV", "vk-dev-123"),
    ];

    #[test]
    fn full_config_parses_and_resolves() {
        let cfg = GatewayConfig::from_toml_str(FULL, env(FULL_ENV)).unwrap();
        assert_eq!(cfg.listen, "127.0.0.1:9000".parse().unwrap());
        assert_eq!(cfg.backends.len(), 2);

        let local = &cfg.backends[0];
        assert_eq!(local.name, "local-llm");
        // trailing slash normalized away
        assert_eq!(local.base_url, "http://127.0.0.1:8000");
        assert!(local.api_key.is_none());
        assert_eq!(local.privacy, Privacy::Local);
        assert!(local.serves_model("qwen-coder"));
        assert!(!local.serves_model("gpt-4o"));
        assert!(local.has_capability_tags(&["code"]));
        assert!(local.has_capability_tags(&["code", "fast"]));
        assert!(!local.has_capability_tags(&["code", "general"]));

        let ext = &cfg.backends[1];
        assert_eq!(ext.privacy, Privacy::External);
        assert_eq!(ext.api_key.as_ref().unwrap().expose(), "test-key-abc");

        assert_eq!(cfg.virtual_keys.len(), 1);
        assert!(cfg.virtual_keys[0].enforce_private);
        assert_eq!(cfg.virtual_keys[0].key.expose(), "vk-dev-123");

        assert_eq!(cfg.policy, PolicySelection::EwmaLatency { alpha: 0.5 });
    }

    #[test]
    fn policy_defaults_to_static_priority() {
        let toml = r#"
            [[backends]]
            name = "b"
            base_url_env = "LLM_BASE_URL"
            models = ["m"]
            privacy = "local"
        "#;
        let cfg = GatewayConfig::from_toml_str(toml, env(&[("LLM_BASE_URL", "http://x")])).unwrap();
        assert_eq!(cfg.policy, PolicySelection::StaticPriority);
        assert!(cfg.virtual_keys.is_empty());
    }

    #[test]
    fn missing_backend_env_is_an_error() {
        let err = GatewayConfig::from_toml_str(FULL, env(&[])).unwrap_err();
        assert!(matches!(
            err,
            ConfigError::MissingEnv { ref var, .. } if var == "LLM_BASE_URL"
        ));
    }

    #[test]
    fn no_backends_is_an_error() {
        let err = GatewayConfig::from_toml_str("listen = \"0.0.0.0:1\"", env(&[])).unwrap_err();
        assert!(matches!(err, ConfigError::NoBackends));
    }

    #[test]
    fn duplicate_backend_names_rejected() {
        let toml = r#"
            [[backends]]
            name = "b"
            base_url_env = "LLM_BASE_URL"
            models = ["m"]
            privacy = "local"

            [[backends]]
            name = "b"
            base_url_env = "LLM_BASE_URL"
            models = ["m"]
            privacy = "external"
        "#;
        let err =
            GatewayConfig::from_toml_str(toml, env(&[("LLM_BASE_URL", "http://x")])).unwrap_err();
        assert!(matches!(err, ConfigError::DuplicateBackend(ref n) if n == "b"));
    }

    #[test]
    fn invalid_privacy_value_rejected() {
        let toml = r#"
            [[backends]]
            name = "b"
            base_url_env = "LLM_BASE_URL"
            models = ["m"]
            privacy = "cloudish"
        "#;
        let err =
            GatewayConfig::from_toml_str(toml, env(&[("LLM_BASE_URL", "http://x")])).unwrap_err();
        assert!(matches!(err, ConfigError::Parse(_)));
    }

    #[test]
    fn non_http_base_url_rejected() {
        let toml = r#"
            [[backends]]
            name = "b"
            base_url_env = "LLM_BASE_URL"
            models = ["m"]
            privacy = "local"
        "#;
        let err =
            GatewayConfig::from_toml_str(toml, env(&[("LLM_BASE_URL", "ftp://x")])).unwrap_err();
        assert!(matches!(err, ConfigError::InvalidBaseUrl { .. }));
    }

    #[test]
    fn empty_models_rejected() {
        let toml = r#"
            [[backends]]
            name = "b"
            base_url_env = "LLM_BASE_URL"
            models = []
            privacy = "local"
        "#;
        let err =
            GatewayConfig::from_toml_str(toml, env(&[("LLM_BASE_URL", "http://x")])).unwrap_err();
        assert!(matches!(err, ConfigError::NoModels(_)));
    }

    #[test]
    fn invalid_env_var_name_rejected() {
        let toml = r#"
            [[backends]]
            name = "b"
            base_url_env = "9BAD NAME"
            models = ["m"]
            privacy = "local"
        "#;
        let err = GatewayConfig::from_toml_str(toml, env(&[])).unwrap_err();
        assert!(matches!(
            err,
            ConfigError::InvalidEnvName {
                field: "base_url_env",
                ..
            }
        ));
    }

    #[test]
    fn alpha_out_of_range_rejected() {
        for alpha in ["0.0", "1.5", "-0.2"] {
            let toml = format!(
                r#"
                [policy]
                kind = "ewma_latency"
                alpha = {alpha}

                [[backends]]
                name = "b"
                base_url_env = "LLM_BASE_URL"
                models = ["m"]
                privacy = "local"
            "#
            );
            let err = GatewayConfig::from_toml_str(&toml, env(&[("LLM_BASE_URL", "http://x")]))
                .unwrap_err();
            assert!(matches!(err, ConfigError::InvalidAlpha(_)), "alpha={alpha}");
        }
    }

    #[test]
    fn duplicate_virtual_key_secrets_rejected() {
        let toml = r#"
            [[backends]]
            name = "b"
            base_url_env = "LLM_BASE_URL"
            models = ["m"]
            privacy = "local"

            [[virtual_keys]]
            name = "a"
            key_env = "KEY_A"

            [[virtual_keys]]
            name = "b"
            key_env = "KEY_B"
        "#;
        let err = GatewayConfig::from_toml_str(
            toml,
            env(&[
                ("LLM_BASE_URL", "http://x"),
                ("KEY_A", "same-secret"),
                ("KEY_B", "same-secret"),
            ]),
        )
        .unwrap_err();
        assert!(matches!(err, ConfigError::DuplicateVirtualKeySecret(..)));
    }

    #[test]
    fn unknown_fields_rejected() {
        let toml = r#"
            listen = "0.0.0.0:1"
            surprise = true
        "#;
        let err = GatewayConfig::from_toml_str(toml, env(&[])).unwrap_err();
        assert!(matches!(err, ConfigError::Parse(_)));
    }

    #[test]
    fn secret_debug_is_redacted() {
        let s = Secret::new("super-sensitive");
        assert_eq!(format!("{s:?}"), "Secret(<redacted>)");
    }
}
