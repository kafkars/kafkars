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

    client
        .shutdown()
        .wait()
        .unwrap_or_else(|error| panic!("shutdown client: {error}"));
}
