use std::sync::Arc;
use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, Criterion};

use patchbay::config::{Backend, Privacy, Secret};
use patchbay::router::{EwmaLatency, RouteQuery, Router, StaticPriority};

fn backend(name: &str, privacy: Privacy, models: &[&str], tags: &[&str]) -> Backend {
    Backend {
        name: name.to_string(),
        base_url: format!("http://{name}.bench"),
        api_key: Some(Secret::new(format!("key-{name}"))),
        models: models.iter().map(|s| s.to_string()).collect(),
        capability_tags: tags.iter().map(|s| s.to_string()).collect(),
        privacy,
    }
}

/// A realistic mixed table: 16 backends, half local, varied models/tags.
fn backend_table() -> Vec<Backend> {
    (0..16)
        .map(|i| {
            let privacy = if i % 2 == 0 {
                Privacy::Local
            } else {
                Privacy::External
            };
            let models: Vec<&str> = match i % 3 {
                0 => vec!["qwen-coder", "qwen-chat"],
                1 => vec!["gpt-4o"],
                _ => vec!["qwen-coder", "gpt-4o"],
            };
            let tags: Vec<&str> = match i % 4 {
                0 => vec!["code", "fast"],
                1 => vec!["general"],
                2 => vec!["code"],
                _ => vec![],
            };
            backend(&format!("b{i}"), privacy, &models, &tags)
        })
        .collect()
}

fn bench_tag_routing(c: &mut Criterion) {
    let router = Router::with_policy(backend_table(), Arc::new(StaticPriority));

    c.bench_function("route/static/shareable", |b| {
        b.iter(|| {
            let q = RouteQuery::new("qwen-coder").with_tags(["code"]);
            black_box(router.route(q, false)).ok();
        })
    });

    c.bench_function("route/static/private", |b| {
        b.iter(|| {
            let q = RouteQuery::new("qwen-coder").with_tags(["code"]);
            black_box(router.route(q, true)).ok();
        })
    });
}

fn bench_latency_selection(c: &mut Criterion) {
    let ewma = Arc::new(EwmaLatency::new(0.3));
    // Seed realistic latency history for every backend.
    for i in 0..16 {
        for sample in [40, 60, 35, 90] {
            ewma.observe(&format!("b{i}"), Duration::from_millis(sample + i as u64));
        }
    }
    let router = Router::with_policy(backend_table(), ewma.clone());

    c.bench_function("route/ewma/private", |b| {
        b.iter(|| {
            let q = RouteQuery::new("qwen-coder").with_tags(["code"]);
            black_box(router.route(q, true)).ok();
        })
    });

    c.bench_function("ewma/observe", |b| {
        b.iter(|| ewma.observe(black_box("b3"), Duration::from_millis(42)))
    });
}

criterion_group!(benches, bench_tag_routing, bench_latency_selection);
criterion_main!(benches);
