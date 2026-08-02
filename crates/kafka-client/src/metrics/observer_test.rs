//! Public operational-metrics observation through a live client host.

use crate::Client;

#[test]
fn client_returns_one_real_operational_snapshot() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("build client: {error}"));

    let snapshot = client
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

    client
        .shutdown()
        .wait()
        .unwrap_or_else(|error| panic!("shutdown client: {error}"));
}
