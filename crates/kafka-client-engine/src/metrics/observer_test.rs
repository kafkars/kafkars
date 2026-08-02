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
    let producer = snapshot.producer();
    assert_eq!(producer.active_records(), 0);
    assert_eq!(producer.active_bytes(), 0);
    assert_eq!(producer.waiting_records(), 0);
    assert_eq!(producer.waiting_bytes(), 0);
    assert_eq!(producer.prepared_batches(), 0);
    assert_eq!(producer.prepared_batch_bytes(), 0);
    assert_eq!(producer.terminal_backlog(), 0);
    assert!(producer.accepting());
    assert!(producer.healthy());

    engine
        .shutdown()
        .unwrap_or_else(|error| panic!("shutdown engine: {error}"));
}
