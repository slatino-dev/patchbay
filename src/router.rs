//! Policy-driven backend selection with **type-enforced privacy routing**.
//!
//! # The privacy guarantee
//!
//! A request routed as private must never reach an [`Privacy::External`]
//! backend. patchbay does not enforce this with an `if` statement sprinkled
//! at call sites (which a refactor could silently drop) — it encodes the
//! constraint in the type system so that the forbidden program *does not
//! compile / cannot be constructed*:
//!
//! 1. Classification happens exactly once, at the trust boundary:
//!    [`ClassifiedQuery::classify`] turns a raw [`RouteQuery`] into either a
//!    `TypedQuery<Private>` or a `TypedQuery<Shareable>`. `Private` and
//!    `Shareable` are *uninhabited* marker types — they exist only at the
//!    type level — and the [`PrivacyClass`] trait they implement is sealed,
//!    so no downstream code can invent a third class.
//!
//! 2. The only way to obtain routing candidates is
//!    [`Candidates::gather`], whose filter is driven by
//!    [`PrivacyClass::admits`]. For `P = Private` that admits only
//!    `Privacy::Local` backends. `Candidates`' fields are private: there is
//!    no constructor that skips the filter.
//!
//! 3. Policies never hand back backends — they return an *index* into the
//!    candidate list ([`Policy::choose`]), and [`Candidates::select`]
//!    bounds-checks it. Even a buggy or adversarial `Policy` implementation
//!    that returns an out-of-range index, or tries to be clever, can only
//!    ever yield a backend that survived the type-witnessed filter. (The
//!    property tests include a malicious policy to demonstrate exactly
//!    this.)
//!
//! The result of selection is a [`Selection<'_, P>`] — a backend reference
//! *witnessed* by the privacy class it was filtered under. A
//! `Selection<'_, Private>` referencing an `External` backend is
//! unrepresentable.
//!
//! ```compile_fail
//! use patchbay::router::{Candidates, Private};
//! // Candidates' fields are private — the filtering constructor is the only
//! // way in, so this does not compile:
//! let c = Candidates::<Private> { items: vec![], _class: std::marker::PhantomData };
//! ```

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::config::{Backend, GatewayConfig, PolicySelection, Privacy};

mod sealed {
    pub trait Sealed {}
}

/// Type-level privacy class of a request. Sealed: `Private` and `Shareable`
/// are the only implementations, mirroring the closed [`Privacy`] enum.
pub trait PrivacyClass: sealed::Sealed {
    /// May a backend with this placement serve requests of this class?
    fn admits(privacy: Privacy) -> bool;
    /// Human-readable label for errors/logs.
    const LABEL: &'static str;
}

/// Marker for requests that must stay on operator-controlled infrastructure.
/// Uninhabited: used only as a type parameter.
#[derive(Debug)]
pub enum Private {}

/// Marker for requests free to use any backend.
/// Uninhabited: used only as a type parameter.
#[derive(Debug)]
pub enum Shareable {}

impl sealed::Sealed for Private {}
impl PrivacyClass for Private {
    fn admits(privacy: Privacy) -> bool {
        privacy == Privacy::Local
    }
    const LABEL: &'static str = "private";
}

impl sealed::Sealed for Shareable {}
impl PrivacyClass for Shareable {
    fn admits(_: Privacy) -> bool {
        true
    }
    const LABEL: &'static str = "shareable";
}

/// What a request asks of the routing layer, before classification.
#[derive(Debug, Clone)]
pub struct RouteQuery {
    /// Model identifier the client requested (exact match).
    pub model: String,
    /// Capability tags the serving backend must have (all of them).
    pub required_tags: Vec<String>,
}

impl RouteQuery {
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            required_tags: Vec::new(),
        }
    }

    pub fn with_tags<S: Into<String>>(mut self, tags: impl IntoIterator<Item = S>) -> Self {
        self.required_tags = tags.into_iter().map(Into::into).collect();
        self
    }
}

/// A query stamped with its privacy class. Constructed only via
/// [`ClassifiedQuery::classify`] — the single classification point.
#[derive(Debug)]
pub struct TypedQuery<P: PrivacyClass> {
    query: RouteQuery,
    _class: PhantomData<P>,
}

impl<P: PrivacyClass> TypedQuery<P> {
    pub fn model(&self) -> &str {
        &self.query.model
    }

    pub fn required_tags(&self) -> &[String] {
        &self.query.required_tags
    }
}

/// The one place a request's privacy is decided.
#[derive(Debug)]
pub enum ClassifiedQuery {
    Private(TypedQuery<Private>),
    Shareable(TypedQuery<Shareable>),
}

impl ClassifiedQuery {
    /// `mark_private` is the OR of every privacy signal the server layer
    /// collects (virtual key `enforce_private`, per-request markers, ...).
    pub fn classify(query: RouteQuery, mark_private: bool) -> Self {
        if mark_private {
            Self::Private(TypedQuery {
                query,
                _class: PhantomData,
            })
        } else {
            Self::Shareable(TypedQuery {
                query,
                _class: PhantomData,
            })
        }
    }
}

/// Backends eligible for one classified query, witnessed by privacy class
/// `P`. Fields are private — [`Candidates::gather`] is the only constructor,
/// and it filters through [`PrivacyClass::admits`].
pub struct Candidates<'a, P: PrivacyClass> {
    items: Vec<&'a Backend>,
    _class: PhantomData<P>,
}

impl<'a, P: PrivacyClass> Candidates<'a, P> {
    /// Filter `backends` down to those that may serve `query`: privacy class
    /// admits the backend, the model is served, and all required capability
    /// tags are present. Config order is preserved (policies treat earlier
    /// entries as higher static priority).
    pub fn gather(query: &TypedQuery<P>, backends: &'a [Backend]) -> Self {
        let items = backends
            .iter()
            .filter(|b| {
                P::admits(b.privacy)
                    && b.serves_model(query.model())
                    && b.has_capability_tags(query.required_tags())
            })
            .collect();
        Self {
            items,
            _class: PhantomData,
        }
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Let `policy` pick a candidate. The policy returns an index; anything
    /// out of range yields `None` rather than escaping the filtered set.
    pub fn select(self, policy: &dyn Policy) -> Option<Selection<'a, P>> {
        let idx = policy.choose(&self.items)?;
        let backend = *self.items.get(idx)?;
        debug_assert!(
            P::admits(backend.privacy),
            "type invariant violated: {} candidate set contained {:?} backend",
            P::LABEL,
            backend.privacy,
        );
        Some(Selection {
            backend,
            _class: PhantomData,
        })
    }
}

/// A routing decision witnessed by the privacy class it was filtered under.
/// A `Selection<'_, Private>` pointing at an `External` backend cannot be
/// constructed.
pub struct Selection<'a, P: PrivacyClass> {
    backend: &'a Backend,
    _class: PhantomData<P>,
}

impl<'a, P: PrivacyClass> Selection<'a, P> {
    pub fn backend(&self) -> &'a Backend {
        self.backend
    }
}

/// A routing policy ranks the pre-filtered candidates; it cannot add to
/// them. Implementations must be cheap and non-blocking — `choose` runs on
/// the request path.
pub trait Policy: Send + Sync {
    /// Index of the chosen candidate, or `None` to decline (e.g. empty set).
    fn choose(&self, candidates: &[&Backend]) -> Option<usize>;
}

/// Picks the first candidate — i.e. config order is the priority order.
#[derive(Debug, Default, Clone, Copy)]
pub struct StaticPriority;

impl Policy for StaticPriority {
    fn choose(&self, candidates: &[&Backend]) -> Option<usize> {
        if candidates.is_empty() {
            None
        } else {
            Some(0)
        }
    }
}

/// Picks the candidate with the lowest exponentially-weighted moving-average
/// latency. Backends with no recorded sample score 0 — i.e. they are tried
/// optimistically so cold backends get probed instead of starved. Ties break
/// toward config order.
#[derive(Debug)]
pub struct EwmaLatency {
    alpha: f64,
    /// backend name -> EWMA latency in milliseconds
    stats: RwLock<HashMap<String, f64>>,
}

impl EwmaLatency {
    /// # Panics
    /// If `alpha` is outside `(0, 1]`. Config validation rejects such values
    /// before they reach here; constructing directly with a bad alpha is a
    /// programming error.
    pub fn new(alpha: f64) -> Self {
        assert!(
            alpha > 0.0 && alpha <= 1.0,
            "EWMA alpha must be in (0, 1], got {alpha}"
        );
        Self {
            alpha,
            stats: RwLock::new(HashMap::new()),
        }
    }

    /// Record an observed request latency for `backend`.
    pub fn observe(&self, backend: &str, latency: Duration) {
        let sample = latency.as_secs_f64() * 1000.0;
        if !sample.is_finite() {
            return;
        }
        let mut stats = self.stats.write().unwrap_or_else(|e| e.into_inner());
        match stats.get_mut(backend) {
            Some(ewma) => *ewma = self.alpha * sample + (1.0 - self.alpha) * *ewma,
            None => {
                stats.insert(backend.to_string(), sample);
            }
        }
    }

    /// Current EWMA latency in milliseconds, if any sample was recorded.
    pub fn latency_ms(&self, backend: &str) -> Option<f64> {
        self.stats
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(backend)
            .copied()
    }
}

impl Policy for EwmaLatency {
    fn choose(&self, candidates: &[&Backend]) -> Option<usize> {
        let stats = self.stats.read().unwrap_or_else(|e| e.into_inner());
        candidates
            .iter()
            .enumerate()
            // Unseen backends score 0.0: optimistic cold start.
            .map(|(i, b)| (i, stats.get(b.name.as_str()).copied().unwrap_or(0.0)))
            .min_by(|(_, a), (_, b)| a.total_cmp(b))
            .map(|(i, _)| i)
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RouteError {
    #[error(
        "no eligible backend for model `{model}` ({privacy} request, required tags {required_tags:?})"
    )]
    NoEligibleBackend {
        model: String,
        required_tags: Vec<String>,
        /// Label of the privacy class the request was routed under.
        privacy: &'static str,
    },
}

/// The routing facade: owns the backend table and the active policy.
pub struct Router {
    backends: Vec<Backend>,
    policy: Arc<dyn Policy>,
}

impl Router {
    /// Build from validated config. For `ewma_latency` the policy instance
    /// is internal; callers that need to feed latency observations should
    /// construct via [`Router::with_policy`] and keep a handle.
    pub fn from_config(cfg: &GatewayConfig) -> Self {
        let policy: Arc<dyn Policy> = match cfg.policy {
            PolicySelection::StaticPriority => Arc::new(StaticPriority),
            PolicySelection::EwmaLatency { alpha } => Arc::new(EwmaLatency::new(alpha)),
        };
        Self::with_policy(cfg.backends.clone(), policy)
    }

    pub fn with_policy(backends: Vec<Backend>, policy: Arc<dyn Policy>) -> Self {
        Self { backends, policy }
    }

    pub fn backends(&self) -> &[Backend] {
        &self.backends
    }

    /// Resolve a query to a backend. `mark_private` flows into
    /// [`ClassifiedQuery::classify`]; from there the privacy constraint is
    /// carried by types, not by control flow in this function — note that
    /// both arms below are *structurally identical*. Forgetting a privacy
    /// check here is impossible because there is no privacy check here.
    pub fn route(&self, query: RouteQuery, mark_private: bool) -> Result<&Backend, RouteError> {
        match ClassifiedQuery::classify(query, mark_private) {
            ClassifiedQuery::Private(q) => self.resolve(&q),
            ClassifiedQuery::Shareable(q) => self.resolve(&q),
        }
    }

    fn resolve<P: PrivacyClass>(&self, query: &TypedQuery<P>) -> Result<&Backend, RouteError> {
        Candidates::gather(query, &self.backends)
            .select(&*self.policy)
            .map(|s| s.backend())
            .ok_or_else(|| RouteError::NoEligibleBackend {
                model: query.model().to_string(),
                required_tags: query.required_tags().to_vec(),
                privacy: P::LABEL,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Secret;

    pub(super) fn backend(name: &str, privacy: Privacy, models: &[&str], tags: &[&str]) -> Backend {
        Backend {
            name: name.to_string(),
            base_url: format!("http://{name}.test"),
            api_key: Some(Secret::new(format!("key-{name}"))),
            models: models.iter().map(|s| s.to_string()).collect(),
            capability_tags: tags.iter().map(|s| s.to_string()).collect(),
            privacy,
        }
    }

    fn query(model: &str, tags: &[&str]) -> RouteQuery {
        RouteQuery::new(model).with_tags(tags.iter().copied())
    }

    // -----------------------------------------------------------------
    // Table-driven policy/routing tests (StaticPriority)
    // -----------------------------------------------------------------

    struct Case {
        name: &'static str,
        backends: Vec<Backend>,
        model: &'static str,
        tags: &'static [&'static str],
        private: bool,
        expect: Result<&'static str, ()>,
    }

    #[test]
    fn static_priority_routing_table() {
        use Privacy::{External, Local};
        let cases = vec![
            Case {
                name: "shareable picks first eligible in config order",
                backends: vec![
                    backend("ext-a", External, &["m1"], &[]),
                    backend("loc-b", Local, &["m1"], &[]),
                ],
                model: "m1",
                tags: &[],
                private: false,
                expect: Ok("ext-a"),
            },
            Case {
                name: "private skips earlier external even though higher priority",
                backends: vec![
                    backend("ext-a", External, &["m1"], &[]),
                    backend("loc-b", Local, &["m1"], &[]),
                ],
                model: "m1",
                tags: &[],
                private: true,
                expect: Ok("loc-b"),
            },
            Case {
                name: "private with only external candidates fails closed",
                backends: vec![
                    backend("ext-a", External, &["m1"], &[]),
                    backend("ext-b", External, &["m1"], &[]),
                    backend("loc-c", Local, &["other-model"], &[]),
                ],
                model: "m1",
                tags: &[],
                private: true,
                expect: Err(()),
            },
            Case {
                name: "model filter skips non-serving backends",
                backends: vec![
                    backend("loc-a", Local, &["m2"], &[]),
                    backend("loc-b", Local, &["m1"], &[]),
                ],
                model: "m1",
                tags: &[],
                private: false,
                expect: Ok("loc-b"),
            },
            Case {
                name: "all required tags must be present",
                backends: vec![
                    backend("loc-a", Local, &["m1"], &["code"]),
                    backend("loc-b", Local, &["m1"], &["code", "fast"]),
                ],
                model: "m1",
                tags: &["code", "fast"],
                private: false,
                expect: Ok("loc-b"),
            },
            Case {
                name: "unknown model yields no backend",
                backends: vec![backend("loc-a", Local, &["m1"], &[])],
                model: "nope",
                tags: &[],
                private: false,
                expect: Err(()),
            },
            Case {
                name: "empty backend table yields no backend",
                backends: vec![],
                model: "m1",
                tags: &[],
                private: false,
                expect: Err(()),
            },
            Case {
                name: "private + tags + model all constrain together",
                backends: vec![
                    backend("ext-a", External, &["m1"], &["code"]),
                    backend("loc-b", Local, &["m1"], &[]),
                    backend("loc-c", Local, &["m1"], &["code"]),
                ],
                model: "m1",
                tags: &["code"],
                private: true,
                expect: Ok("loc-c"),
            },
        ];

        for case in cases {
            let router = Router::with_policy(case.backends, Arc::new(StaticPriority));
            let got = router.route(query(case.model, case.tags), case.private);
            match (case.expect, got) {
                (Ok(want), Ok(b)) => assert_eq!(b.name, want, "case: {}", case.name),
                (Err(()), Err(RouteError::NoEligibleBackend { .. })) => {}
                (want, got) => {
                    panic!(
                        "case `{}`: want {:?}, got {:?}",
                        case.name,
                        want,
                        got.map(|b| &b.name)
                    )
                }
            }
        }
    }

    // -----------------------------------------------------------------
    // EwmaLatency policy
    // -----------------------------------------------------------------

    #[test]
    fn ewma_prefers_lowest_observed_latency() {
        let backends = vec![
            backend("slow", Privacy::Local, &["m"], &[]),
            backend("fast", Privacy::Local, &["m"], &[]),
        ];
        let ewma = Arc::new(EwmaLatency::new(0.5));
        ewma.observe("slow", Duration::from_millis(800));
        ewma.observe("fast", Duration::from_millis(20));
        let router = Router::with_policy(backends, ewma);
        assert_eq!(router.route(query("m", &[]), false).unwrap().name, "fast");
    }

    #[test]
    fn ewma_unseen_backend_is_probed_first() {
        let backends = vec![
            backend("seen", Privacy::Local, &["m"], &[]),
            backend("cold", Privacy::Local, &["m"], &[]),
        ];
        let ewma = Arc::new(EwmaLatency::new(0.5));
        ewma.observe("seen", Duration::from_millis(5));
        let router = Router::with_policy(backends, ewma);
        // cold has no sample -> scores 0.0 -> chosen over 5ms.
        assert_eq!(router.route(query("m", &[]), false).unwrap().name, "cold");
    }

    #[test]
    fn ewma_blends_with_alpha() {
        let ewma = EwmaLatency::new(0.5);
        ewma.observe("b", Duration::from_millis(100)); // first sample taken as-is
        assert_eq!(ewma.latency_ms("b"), Some(100.0));
        ewma.observe("b", Duration::from_millis(200)); // 0.5*200 + 0.5*100
        assert_eq!(ewma.latency_ms("b"), Some(150.0));
        ewma.observe("b", Duration::from_millis(50)); // 0.5*50 + 0.5*150
        assert_eq!(ewma.latency_ms("b"), Some(100.0));
    }

    #[test]
    fn ewma_ties_break_toward_config_order() {
        let backends = vec![
            backend("first", Privacy::Local, &["m"], &[]),
            backend("second", Privacy::Local, &["m"], &[]),
        ];
        let ewma = Arc::new(EwmaLatency::new(0.5));
        ewma.observe("first", Duration::from_millis(10));
        ewma.observe("second", Duration::from_millis(10));
        let router = Router::with_policy(backends, ewma);
        assert_eq!(router.route(query("m", &[]), false).unwrap().name, "first");
    }

    #[test]
    fn ewma_private_ignores_faster_external() {
        let backends = vec![
            backend("ext-fast", Privacy::External, &["m"], &[]),
            backend("loc-slow", Privacy::Local, &["m"], &[]),
        ];
        let ewma = Arc::new(EwmaLatency::new(0.5));
        ewma.observe("ext-fast", Duration::from_millis(1));
        ewma.observe("loc-slow", Duration::from_millis(900));
        let router = Router::with_policy(backends, ewma);
        // Shareable traffic follows latency...
        assert_eq!(
            router.route(query("m", &[]), false).unwrap().name,
            "ext-fast"
        );
        // ...private traffic structurally cannot see the external backend.
        assert_eq!(
            router.route(query("m", &[]), true).unwrap().name,
            "loc-slow"
        );
    }

    #[test]
    #[should_panic(expected = "alpha must be in (0, 1]")]
    fn ewma_rejects_bad_alpha() {
        let _ = EwmaLatency::new(0.0);
    }

    #[test]
    fn route_error_carries_context() {
        let router = Router::with_policy(
            vec![backend("ext", Privacy::External, &["m"], &[])],
            Arc::new(StaticPriority),
        );
        let err = router.route(query("m", &["code"]), true).unwrap_err();
        assert_eq!(
            err,
            RouteError::NoEligibleBackend {
                model: "m".to_string(),
                required_tags: vec!["code".to_string()],
                privacy: "private",
            }
        );
    }

    #[test]
    fn from_config_builds_each_policy() {
        let toml = r#"
            [policy]
            kind = "ewma_latency"

            [[backends]]
            name = "b"
            base_url_env = "LLM_BASE_URL"
            models = ["m"]
            privacy = "local"
        "#;
        let cfg = GatewayConfig::from_toml_str(toml, |k| {
            (k == "LLM_BASE_URL").then(|| "http://x".to_string())
        })
        .unwrap();
        let router = Router::from_config(&cfg);
        assert_eq!(router.route(RouteQuery::new("m"), true).unwrap().name, "b");
    }
}

#[cfg(test)]
mod privacy_properties {
    //! Property tests for the privacy invariant: across arbitrary backend
    //! tables, queries, latency histories, and policies — including a
    //! deliberately malicious policy — a private request never resolves to
    //! an `External` backend.

    use std::sync::Arc;
    use std::time::Duration;

    use proptest::prelude::*;

    use super::tests::backend;
    use super::*;

    const MODEL_POOL: &[&str] = &["alpha", "beta", "gamma"];
    const TAG_POOL: &[&str] = &["code", "fast", "cheap"];

    /// A policy that ignores the candidates and returns whatever index it
    /// likes — including wildly out-of-range ones. Exists to demonstrate
    /// that the privacy invariant survives even a hostile `Policy` impl.
    #[derive(Debug)]
    struct MaliciousPolicy {
        index: usize,
    }

    impl Policy for MaliciousPolicy {
        fn choose(&self, _candidates: &[&Backend]) -> Option<usize> {
            Some(self.index)
        }
    }

    fn arb_backends() -> impl Strategy<Value = Vec<Backend>> {
        prop::collection::vec(
            (
                prop::bool::ANY,                                   // local?
                prop::collection::vec(0..MODEL_POOL.len(), 0..=3), // model picks
                prop::collection::vec(0..TAG_POOL.len(), 0..=3),   // tag picks
            ),
            0..8,
        )
        .prop_map(|specs| {
            specs
                .into_iter()
                .enumerate()
                .map(|(i, (local, models, tags))| {
                    let privacy = if local {
                        Privacy::Local
                    } else {
                        Privacy::External
                    };
                    let models: Vec<&str> = models.into_iter().map(|m| MODEL_POOL[m]).collect();
                    let tags: Vec<&str> = tags.into_iter().map(|t| TAG_POOL[t]).collect();
                    // Unique name per index; empty model list is permitted by
                    // the type (config validation forbids it) — routing must
                    // be safe even then.
                    backend(&format!("b{i}"), privacy, &models, &tags)
                })
                .collect()
        })
    }

    fn arb_query() -> impl Strategy<Value = RouteQuery> {
        (
            prop::sample::select(["alpha", "beta", "gamma", "missing"].as_slice()),
            prop::collection::vec(0..TAG_POOL.len(), 0..=3),
        )
            .prop_map(|(model, tags)| {
                RouteQuery::new(model).with_tags(tags.into_iter().map(|t| TAG_POOL[t]))
            })
    }

    /// Random latency observations: (backend index, latency ms).
    fn arb_observations() -> impl Strategy<Value = Vec<(usize, u64)>> {
        prop::collection::vec((0usize..8, 1u64..5_000), 0..16)
    }

    #[derive(Debug, Clone)]
    enum PolicySpec {
        Static,
        Ewma { alpha_milli: u16 },
        Malicious { index: usize },
    }

    fn arb_policy() -> impl Strategy<Value = PolicySpec> {
        prop_oneof![
            Just(PolicySpec::Static),
            (1u16..=1000).prop_map(|alpha_milli| PolicySpec::Ewma { alpha_milli }),
            prop::num::usize::ANY.prop_map(|index| PolicySpec::Malicious { index }),
        ]
    }

    fn build_policy(spec: &PolicySpec, observations: &[(usize, u64)]) -> Arc<dyn Policy> {
        match spec {
            PolicySpec::Static => Arc::new(StaticPriority),
            PolicySpec::Ewma { alpha_milli } => {
                let ewma = EwmaLatency::new(f64::from(*alpha_milli) / 1000.0);
                for (idx, ms) in observations {
                    ewma.observe(&format!("b{idx}"), Duration::from_millis(*ms));
                }
                Arc::new(ewma)
            }
            PolicySpec::Malicious { index } => Arc::new(MaliciousPolicy { index: *index }),
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1024))]

        /// THE invariant: no private request ever resolves to an External
        /// backend, for any config, query, latency history, or policy —
        /// including the malicious one.
        #[test]
        fn private_never_routes_external(
            backends in arb_backends(),
            query in arb_query(),
            spec in arb_policy(),
            observations in arb_observations(),
        ) {
            let policy = build_policy(&spec, &observations);
            let router = Router::with_policy(backends, policy);

            match router.route(query.clone(), true) {
                Ok(chosen) => {
                    prop_assert_eq!(
                        chosen.privacy,
                        Privacy::Local,
                        "private request reached external backend `{}` under {:?}",
                        &chosen.name,
                        &spec
                    );
                    prop_assert!(chosen.serves_model(&query.model));
                    prop_assert!(chosen.has_capability_tags(&query.required_tags));
                }
                Err(RouteError::NoEligibleBackend { privacy, .. }) => {
                    prop_assert_eq!(privacy, "private");
                }
            }
        }

        /// Shareable requests still respect model + tag eligibility, and a
        /// route is found whenever any eligible backend exists (no policy —
        /// even the malicious one returning garbage indexes — can conjure an
        /// ineligible backend; it can at worst decline).
        #[test]
        fn shareable_selection_is_always_eligible(
            backends in arb_backends(),
            query in arb_query(),
            spec in arb_policy(),
            observations in arb_observations(),
        ) {
            let policy = build_policy(&spec, &observations);
            let router = Router::with_policy(backends, policy);

            if let Ok(chosen) = router.route(query.clone(), false) {
                prop_assert!(chosen.serves_model(&query.model));
                prop_assert!(chosen.has_capability_tags(&query.required_tags));
            }
        }

        /// Well-behaved policies (static, EWMA) must find a backend exactly
        /// when an eligible one exists.
        #[test]
        fn honest_policies_are_complete(
            backends in arb_backends(),
            query in arb_query(),
            alpha_milli in 1u16..=1000,
            observations in arb_observations(),
            use_ewma in prop::bool::ANY,
        ) {
            let spec = if use_ewma {
                PolicySpec::Ewma { alpha_milli }
            } else {
                PolicySpec::Static
            };
            let policy = build_policy(&spec, &observations);

            let eligible_exists = |private: bool| {
                backends.iter().any(|b| {
                    (!private || b.privacy == Privacy::Local)
                        && b.serves_model(&query.model)
                        && b.has_capability_tags(&query.required_tags)
                })
            };

            let router = Router::with_policy(backends.clone(), policy);
            for private in [false, true] {
                let routed = router.route(query.clone(), private).is_ok();
                prop_assert_eq!(
                    routed,
                    eligible_exists(private),
                    "completeness mismatch (private={})", private
                );
            }
        }
    }
}
