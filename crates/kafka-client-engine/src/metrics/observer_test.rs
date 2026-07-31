//! Operational-metrics observation through the live embedded host.

use crate::{Engine, EngineConfig};

#[test]
fn live_engine_returns_one_bounded_metrics_snapshot() {
    let engine = Engine::start(EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("start engine: {error}"));

    let snapshot = engine
        .metrics()
        .unwrap_or_else(|error| panic!("admit metrics: {error}"))
        .wait()
        .unwrap_or_else(|error| panic!("observe metrics: {error}"));

    assert_eq!(snapshot.calls().admitted(), 0);
    assert_eq!(snapshot.calls().succeeded(), 0);
    assert_eq!(snapshot.mailbox().queued_work(), 0);
    assert_eq!(snapshot.latency().end_to_end().samples(), 0);

    engine
        .shutdown()
        .unwrap_or_else(|error| panic!("shutdown engine: {error}"));
}
