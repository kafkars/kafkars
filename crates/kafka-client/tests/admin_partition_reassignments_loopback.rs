//! Public Admin `AlterPartitionReassignments` over one exact controller-routed v1 frame.
#![expect(
    clippy::expect_used,
    reason = "integration fixtures require contextual failure messages"
)]

#[path = "admin_partition_reassignments_loopback/mod.rs"]
mod admin_partition_reassignments_loopback;
#[path = "admin_list_offsets_loopback/frame.rs"]
mod shared_admin_frame;
#[path = "admin_list_partition_reassignments_loopback/wait.rs"]
mod shared_admin_wait;

use std::time::{Duration, Instant};

use admin_partition_reassignments_loopback::{PartitionReassignmentsBroker, Workflow, wait_within};
use kafkars::{
    AlterPartitionReassignmentsResult, Client, DeliveryStatus, ErrorKind, KafkaError,
    PartitionReassignmentChange, TopicPartition,
};

#[test]
fn public_alter_partition_reassignments_v1_routes_once_and_restores_caller_order() {
    let broker = PartitionReassignmentsBroker::start(Workflow::Standard);
    let client = Client::builder()
        .bootstrap_servers([broker.endpoint()])
        .client_id("admin-alter-partition-reassignments-loopback")
        .build()
        .unwrap_or_else(|error| panic!("build AlterPartitionReassignments client: {error}"));
    wait_until_ready(&client, "AlterPartitionReassignments");

    let altered = wait_within(
        client
            .admin()
            .alter_partition_reassignments(changes())
            .allow_replication_factor_change(false)
            .deadline_after(Duration::from_secs(5))
            .submit(),
        "public AlterPartitionReassignments v1",
    )
    .unwrap_or_else(|error| {
        panic!(
            "public AlterPartitionReassignments v1: {error}; observed {}",
            broker.observation_summary()
        )
    });

    assert_eq!(altered.throttle_time(), Duration::from_millis(43));
    let partitions = altered.into_partitions().into_entries();
    assert_eq!(
        partitions
            .iter()
            .map(|(target, _result)| target.clone())
            .collect::<Vec<_>>(),
        [
            TopicPartition::new("zeta", 2),
            TopicPartition::new("alpha", 3),
            TopicPartition::new("zeta", 0),
            TopicPartition::new("beta", 4),
        ],
        "partition outcomes must retain original caller order"
    );
    assert!(partitions[0].1.is_ok());
    let rejected = partitions[1]
        .1
        .as_ref()
        .expect_err("alpha-3 must retain its controller rejection");
    assert_eq!(rejected.kind(), ErrorKind::Broker);
    assert_eq!(rejected.broker_code(), Some(-31_998));
    assert_eq!(
        rejected.delivery_status(),
        Some(DeliveryStatus::PossiblySent)
    );
    assert_eq!(
        rejected.to_string(),
        "Kafka rejected reassignment partition with broker code -31998: controller-denied"
    );
    assert!(!rejected.diagnostic_truncated());
    assert!(partitions[2].1.is_ok());
    assert!(partitions[3].1.is_ok());

    wait_within(
        client.shutdown(),
        "AlterPartitionReassignments client shutdown",
    )
    .unwrap_or_else(|error| panic!("AlterPartitionReassignments shutdown: {error}"));
    drop(client);
    broker.assert_complete();
}

#[test]
fn not_controller_refreshes_reassignment_route_before_caller_retry() {
    let broker = PartitionReassignmentsBroker::start(Workflow::ControllerRecovery);
    let client = Client::builder()
        .bootstrap_servers([broker.endpoint()])
        .client_id("admin-alter-reassignments-controller-recovery")
        .build()
        .unwrap_or_else(|error| panic!("build API 45 recovery client: {error}"));
    wait_until_ready(&client, "API 45 recovery");
    let admin = client.admin();

    let stale = alter_within(&admin, "stale-controller AlterPartitionReassignments")
        .expect_err("the stale controller must retain its API 45 rejection");
    assert_eq!(stale.kind(), ErrorKind::Broker);
    assert_eq!(stale.broker_code(), Some(41));
    assert_eq!(stale.delivery_status(), Some(DeliveryStatus::PossiblySent));
    broker.assert_refreshed_without_replay();
    drop(stale);

    let recovered = alter_within(&admin, "caller retry after API 45 controller refresh")
        .unwrap_or_else(|error| panic!("retry API 45 on refreshed controller: {error}"));
    assert_eq!(recovered.throttle_time(), Duration::from_millis(53));
    let entries = recovered.into_partitions().into_entries();
    assert_eq!(entries.len(), 4);
    assert!(entries.iter().all(|(_target, result)| result.is_ok()));

    drop(admin);
    wait_within(client.shutdown(), "API 45 recovery client shutdown")
        .unwrap_or_else(|error| panic!("complete API 45 recovery shutdown: {error}"));
    drop(client);
    broker.assert_complete();
}

fn alter_within(
    admin: &kafkars::Admin,
    phase: &str,
) -> Result<AlterPartitionReassignmentsResult, KafkaError> {
    let admission_deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let result = wait_within(
            admin
                .alter_partition_reassignments(changes())
                .allow_replication_factor_change(false)
                .deadline_after(Duration::from_secs(5))
                .submit(),
            phase,
        );
        match result {
            Err(error)
                if error.kind() == ErrorKind::Backpressure
                    && error.delivery_status() == Some(DeliveryStatus::NotSent)
                    && Instant::now() < admission_deadline =>
            {
                std::thread::sleep(Duration::from_millis(1));
            }
            result => return result,
        }
    }
}

fn wait_until_ready(client: &Client, context: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match client.ready().wait() {
            Ok(()) => return,
            Err(error)
                if error.kind() == ErrorKind::Transport
                    && error.delivery_status() == Some(DeliveryStatus::NotSent)
                    && Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(error) => panic!("complete {context} readiness: {error}"),
        }
    }
}

fn changes() -> [PartitionReassignmentChange; 4] {
    [
        PartitionReassignmentChange::new("zeta", 2, [7, 3]),
        PartitionReassignmentChange::cancel("alpha", 3),
        PartitionReassignmentChange::cancel("zeta", 0),
        PartitionReassignmentChange::new("beta", 4, [9, 2, 5]),
    ]
}
