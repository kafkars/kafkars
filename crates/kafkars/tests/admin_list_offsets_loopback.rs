//! Public Admin `ListOffsets` over Kafka 4.3 selectors, fencing, and read isolation.
#![expect(
    clippy::expect_used,
    reason = "integration fixtures require contextual failure messages"
)]

#[path = "admin_list_offsets_loopback/mod.rs"]
mod admin_list_offsets_loopback;

use std::{
    thread,
    time::{Duration, Instant},
};

use admin_list_offsets_loopback::{ListOffsetsBroker, Workflow};
use kafkars::{
    Client,
    admin::{ListOffsetsQuery, OffsetSpec},
    consumer::ReadIsolation,
    error::{DeliveryStatus, ErrorKind, RetryAdvice},
};

#[test]
fn kafka_43_selectors_preserve_order_isolation_fencing_and_leader_routing() {
    let broker = ListOffsetsBroker::start(Workflow::Kafka43);
    let client = Client::builder()
        .bootstrap_servers([broker.endpoint()])
        .client_id("admin-list-offsets-kafka-43-loopback")
        .build()
        .unwrap_or_else(|error| panic!("build Kafka 4.3 ListOffsets client: {error}"));
    wait_until_ready(&client, "Kafka 4.3 ListOffsets");

    let result = client
        .admin()
        .list_offsets([
            ListOffsetsQuery::new("orders", 0, OffsetSpec::max_timestamp())
                .current_leader_epoch(41),
            ListOffsetsQuery::new("orders", 1, OffsetSpec::earliest_local()),
            ListOffsetsQuery::new("orders", 2, OffsetSpec::latest_tiered()),
            ListOffsetsQuery::new("orders", 3, OffsetSpec::earliest_pending_upload()),
        ])
        .read_isolation(ReadIsolation::ReadCommitted)
        .deadline_after(Duration::from_secs(5))
        .submit()
        .wait()
        .unwrap_or_else(|error| panic!("complete Kafka 4.3 ListOffsets: {error}"));

    assert_eq!(result.throttle_time(), Duration::from_millis(11));
    let entries = result.into_offsets().into_entries();
    assert_eq!(
        entries
            .iter()
            .map(|(target, outcome)| {
                let info = outcome
                    .as_ref()
                    .unwrap_or_else(|error| panic!("ListOffsets outcome: {error}"));
                (
                    target.topic(),
                    target.partition(),
                    info.offset(),
                    info.timestamp_ms(),
                    info.leader_epoch(),
                )
            })
            .collect::<Vec<_>>(),
        [
            ("orders", 0, Some(71), Some(1_700), Some(3)),
            ("orders", 1, Some(10), None, Some(4)),
            ("orders", 2, None, None, Some(5)),
            ("orders", 3, Some(81), None, Some(6)),
        ]
    );

    client
        .shutdown()
        .wait()
        .unwrap_or_else(|error| panic!("Kafka 4.3 ListOffsets shutdown: {error}"));
    drop(client);
    broker.assert_complete();
}

#[test]
fn earliest_pending_upload_requires_v11_before_list_offsets_transport() {
    let broker = ListOffsetsBroker::start(Workflow::NoEarliestPendingUpload);
    let client = Client::builder()
        .bootstrap_servers([broker.endpoint()])
        .client_id("admin-list-offsets-v10-loopback")
        .build()
        .unwrap_or_else(|error| panic!("build v10 ListOffsets client: {error}"));
    wait_until_ready(&client, "v10 ListOffsets");

    let error = client
        .admin()
        .list_offsets([ListOffsetsQuery::new(
            "orders",
            0,
            OffsetSpec::earliest_pending_upload(),
        )])
        .deadline_after(Duration::from_secs(5))
        .submit()
        .wait()
        .expect_err("v11-only selector must reject a v10 broker");
    assert_eq!(error.kind(), ErrorKind::Compatibility);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));

    client
        .shutdown()
        .wait()
        .unwrap_or_else(|error| panic!("v10 ListOffsets shutdown: {error}"));
    drop(client);
    broker.assert_complete();
}

fn wait_until_ready(client: &Client, context: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match client.ready().wait() {
            Ok(()) => return,
            Err(error)
                if (error.retry_advice() == RetryAdvice::RetrySafe
                    || (error.kind() == ErrorKind::Transport
                        && error.delivery_status() == Some(DeliveryStatus::NotSent)))
                    && Instant::now() < deadline =>
            {
                thread::sleep(Duration::from_millis(1));
            }
            Err(error) => panic!("complete {context} readiness: {error}"),
        }
    }
}
