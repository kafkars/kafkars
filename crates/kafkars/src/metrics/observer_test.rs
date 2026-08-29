//! Public operational-metrics observation through a live client host.

use std::time::{Duration, Instant};

use crate::{Client, ErrorKind, RetryAdvice};

#[test]
fn client_returns_one_real_operational_snapshot() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("build client: {error}"));

    let admission_deadline = Instant::now() + Duration::from_secs(2);
    let metrics = loop {
        match client.metrics() {
            Ok(metrics) => break metrics,
            Err(error)
                if error.kind() == ErrorKind::Backpressure
                    && error.retry_advice() == RetryAdvice::RetrySafe
                    && Instant::now() < admission_deadline =>
            {
                core::hint::spin_loop();
            }
            Err(error) => panic!("admit metrics: {error}"),
        }
    };
    let snapshot = metrics
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
    assert_eq!(producer.produce_requests(), 0);
    assert_eq!(producer.produce_batches(), 0);
    assert_eq!(producer.produce_records(), 0);
    assert_eq!(producer.produce_encoded_bytes(), 0);
    assert!(producer.accepting());
    assert!(producer.healthy());

    client
        .shutdown()
        .wait()
        .unwrap_or_else(|error| panic!("shutdown client: {error}"));
}
