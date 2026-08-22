//! Public Admin reassignment listing over exact controller-routed API 46 v0 frames.
#![expect(
    clippy::expect_used,
    reason = "integration fixtures require contextual failure messages"
)]

#[path = "admin_list_partition_reassignments_loopback/mod.rs"]
mod admin_list_partition_reassignments_loopback;
#[path = "admin_list_offsets_loopback/frame.rs"]
mod shared_admin_frame;
#[path = "admin_list_partition_reassignments_loopback/wait.rs"]
mod shared_admin_wait;

use std::time::{Duration, Instant};

use admin_list_partition_reassignments_loopback::{
    ListPartitionReassignmentsBroker, Workflow, wait_within,
};
use kafkars::{
    Client, DeliveryStatus, ErrorKind, KafkaError, ListPartitionReassignmentsResult, RetryAdvice,
    TopicPartition,
};

#[test]
fn public_selected_reassignments_v0_route_once_and_restore_caller_order() {
    let broker = ListPartitionReassignmentsBroker::start(Workflow::Selected);
    let client = build_client(&broker, "admin-list-selected-reassignments-loopback");
    let targets = [
        TopicPartition::new("zeta", 2),
        TopicPartition::new("missing", 9),
        TopicPartition::new("alpha", 0),
        TopicPartition::new("zeta", 1),
    ];

    let listed = list_selected_within(
        &client.admin(),
        &targets,
        "selected ListPartitionReassignments",
    )
    .unwrap_or_else(|error| {
        panic!(
            "selected ListPartitionReassignments: {error}; observed {}",
            broker.observation_summary()
        )
    });

    assert_eq!(listed.throttle_time(), Duration::from_millis(31));
    let rows = listed.into_reassignments();
    assert_eq!(
        rows.iter()
            .map(|(target, _)| (target.topic(), target.partition()))
            .collect::<Vec<_>>(),
        [("zeta", 2), ("alpha", 0), ("zeta", 1)]
    );
    assert_eq!(rows[0].1.replicas(), [7, 2, 9]);
    assert_eq!(rows[0].1.adding_replicas(), [9]);
    assert_eq!(rows[0].1.removing_replicas(), [1]);
    assert_eq!(rows[1].1.replicas(), [2, 7]);
    assert_eq!(rows[1].1.adding_replicas(), [7]);
    assert!(rows[1].1.removing_replicas().is_empty());
    assert_eq!(rows[2].1.replicas(), [7, 9]);
    assert_eq!(rows[2].1.adding_replicas(), [9]);
    assert_eq!(rows[2].1.removing_replicas(), [2]);

    finish(client, broker, "selected ListPartitionReassignments");
}

#[test]
fn public_all_active_reassignments_use_nullable_selection_and_canonical_order() {
    let broker = ListPartitionReassignmentsBroker::start(Workflow::AllActive);
    let client = build_client(&broker, "admin-list-all-reassignments-loopback");

    let listed = list_all_within(&client.admin(), "all-active ListPartitionReassignments")
        .unwrap_or_else(|error| {
            panic!(
                "all-active ListPartitionReassignments: {error}; observed {}",
                broker.observation_summary()
            )
        });

    assert_eq!(listed.throttle_time(), Duration::from_millis(37));
    let rows = listed.into_reassignments();
    assert_eq!(
        rows.iter()
            .map(|(target, _)| (target.topic(), target.partition()))
            .collect::<Vec<_>>(),
        [("alpha", 1), ("zeta", 0), ("zeta", 2)]
    );
    assert_eq!(rows[0].1.replicas(), [2, 7]);
    assert_eq!(rows[1].1.replicas(), [7, 9]);
    assert_eq!(rows[2].1.replicas(), [9, 7]);

    finish(client, broker, "all-active ListPartitionReassignments");
}

#[test]
fn public_reassignment_listing_preserves_signed_controller_diagnostic() {
    let broker = ListPartitionReassignmentsBroker::start(Workflow::BrokerError);
    let client = build_client(&broker, "admin-list-reassignments-error-loopback");

    let error = list_all_within(&client.admin(), "rejected ListPartitionReassignments")
        .expect_err("scripted controller rejection must reach the public observer");

    assert_eq!(error.kind(), ErrorKind::Broker);
    assert_eq!(error.broker_code(), Some(-31_999));
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::PossiblySent));
    assert_eq!(error.to_string(), "reassignment-listing-denied");
    assert!(!error.diagnostic_truncated());

    finish(client, broker, "rejected ListPartitionReassignments");
}

#[test]
fn not_controller_refreshes_listing_route_before_caller_retry() {
    let broker = ListPartitionReassignmentsBroker::start(Workflow::ControllerRecovery);
    let client = build_client(&broker, "admin-list-reassignments-controller-recovery");
    let admin = client.admin();

    let stale = list_all_within(&admin, "stale-controller ListPartitionReassignments")
        .expect_err("the stale controller must retain its API 46 rejection");
    assert_eq!(stale.kind(), ErrorKind::Broker);
    assert_eq!(stale.broker_code(), Some(41));
    assert_eq!(stale.delivery_status(), Some(DeliveryStatus::PossiblySent));
    broker.assert_refreshed_without_replay();
    drop(stale);

    let recovered = list_all_within(&admin, "caller retry after API 46 controller refresh")
        .unwrap_or_else(|error| panic!("retry API 46 on refreshed controller: {error}"));
    assert_eq!(recovered.throttle_time(), Duration::from_millis(37));
    assert_eq!(recovered.into_reassignments().len(), 3);

    drop(admin);
    finish(
        client,
        broker,
        "controller-recovery ListPartitionReassignments",
    );
}

fn list_all_within(
    admin: &kafkars::Admin,
    phase: &str,
) -> Result<ListPartitionReassignmentsResult, KafkaError> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let now = Instant::now();
        let result = wait_within(
            admin
                .list_all_partition_reassignments()
                .deadline_after(deadline.saturating_duration_since(now))
                .submit(),
            phase,
        );
        match result {
            Err(error)
                if error.retry_advice() == RetryAdvice::RetrySafe && Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(1));
            }
            result => return result,
        }
    }
}

fn list_selected_within(
    admin: &kafkars::Admin,
    targets: &[TopicPartition],
    phase: &str,
) -> Result<ListPartitionReassignmentsResult, KafkaError> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let now = Instant::now();
        let result = wait_within(
            admin
                .list_partition_reassignments(targets.iter().cloned())
                .deadline_after(deadline.saturating_duration_since(now))
                .submit(),
            phase,
        );
        match result {
            Err(error)
                if error.retry_advice() == RetryAdvice::RetrySafe && Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(1));
            }
            result => return result,
        }
    }
}

fn build_client(broker: &ListPartitionReassignmentsBroker, client_id: &str) -> Client {
    let client = Client::builder()
        .bootstrap_servers([broker.bootstrap_endpoint()])
        .client_id(client_id)
        .build()
        .unwrap_or_else(|error| panic!("build ListPartitionReassignments client: {error}"));
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match client.ready().wait() {
            Ok(()) => break,
            Err(error)
                if (error.retry_advice() == RetryAdvice::RetrySafe
                    || (error.kind() == ErrorKind::Transport
                        && error.delivery_status() == Some(DeliveryStatus::NotSent)))
                    && Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(error) => panic!("complete ListPartitionReassignments readiness: {error}"),
        }
    }
    client
}

fn finish(client: Client, broker: ListPartitionReassignmentsBroker, phase: &str) {
    wait_within(client.shutdown(), &format!("{phase} shutdown"))
        .unwrap_or_else(|error| panic!("{phase} shutdown: {error}"));
    drop(client);
    broker.assert_complete();
}
