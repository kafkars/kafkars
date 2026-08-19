//! Selected replica response correlation scenarios.

use kafka_client_core::{DescribeReplicaLogDirsInput, DescribeReplicaLogDirsReplica};

use crate::protocol::admin::describe_log_dirs::{
    NormalizedDescribeLogDir, NormalizedDescribeLogDirsPartition,
    NormalizedDescribeLogDirsResponse, NormalizedDescribeLogDirsTopic,
};

use super::response::{CorrelationFailure, normalized_input};

#[test]
fn response_restores_request_order_and_distinguishes_current_future_missing() {
    let replicas = vec![
        replica("orders", 1, 7),
        replica("orders", 2, 7),
        replica("audit", 0, 7),
    ];
    let normalized = response(vec![
        log_dir(
            "/logs/a",
            0,
            vec![topic(
                "orders",
                vec![partition(2, false, 3), partition(1, false, 1)],
            )],
        ),
        log_dir(
            "/logs/b",
            0,
            vec![topic("orders", vec![partition(1, true, 9)])],
        ),
    ]);

    let (input, retained) = normalized_input(7, &replicas, normalized)
        .unwrap_or_else(|error| panic!("valid correlation: {error:?}"));
    assert_eq!(retained, 512);
    let DescribeReplicaLogDirsInput::BrokerResponded {
        broker_id,
        throttle_time_ms,
        result: Ok(placements),
    } = input
    else {
        panic!("expected successful broker response");
    };
    assert_eq!(broker_id, 7);
    assert_eq!(throttle_time_ms, 11);
    assert_eq!(placements.len(), 3);
    assert_eq!(placements[0].replica(), &replicas[0]);
    assert_eq!(
        placements[0]
            .info()
            .current()
            .map(kafka_client_core::ReplicaLogDirLocation::path),
        Some("/logs/a")
    );
    assert_eq!(
        placements[0]
            .info()
            .future()
            .map(kafka_client_core::ReplicaLogDirLocation::path),
        Some("/logs/b")
    );
    assert_eq!(
        placements[1]
            .info()
            .current()
            .map(kafka_client_core::ReplicaLogDirLocation::offset_lag),
        Some(3)
    );
    assert!(placements[2].info().current().is_none());
    assert!(placements[2].info().future().is_none());
}

#[test]
fn directory_error_contributes_no_placement() {
    let replicas = vec![replica("orders", 1, 7)];
    let normalized = response(vec![log_dir(
        "/offline",
        56,
        vec![topic("orders", vec![partition(1, false, 0)])],
    )]);

    let (input, _) = normalized_input(7, &replicas, normalized)
        .unwrap_or_else(|error| panic!("directory error is absence: {error:?}"));
    let DescribeReplicaLogDirsInput::BrokerResponded {
        result: Ok(placements),
        ..
    } = input
    else {
        panic!("expected successful broker response");
    };
    assert!(placements[0].info().current().is_none());
    assert!(placements[0].info().future().is_none());
}

#[test]
fn duplicate_current_or_future_placement_is_invalid() {
    let replicas = vec![replica("orders", 1, 7)];
    let normalized = response(vec![
        log_dir(
            "/logs/a",
            0,
            vec![topic("orders", vec![partition(1, false, 0)])],
        ),
        log_dir(
            "/logs/b",
            0,
            vec![topic("orders", vec![partition(1, false, 0)])],
        ),
    ]);

    assert_eq!(
        normalized_input(7, &replicas, normalized),
        Err(CorrelationFailure::Invalid)
    );
}

#[test]
fn unknown_replica_is_invalid_even_after_protocol_normalization() {
    let replicas = vec![replica("orders", 1, 7)];
    let normalized = response(vec![log_dir(
        "/logs/a",
        0,
        vec![topic("other", vec![partition(1, false, 0)])],
    )]);

    assert_eq!(
        normalized_input(7, &replicas, normalized),
        Err(CorrelationFailure::Invalid)
    );
}

fn replica(topic: &str, partition: i32, broker_id: i32) -> DescribeReplicaLogDirsReplica {
    DescribeReplicaLogDirsReplica::new(topic.to_owned(), partition, broker_id)
}

fn response(log_dirs: Vec<NormalizedDescribeLogDir>) -> NormalizedDescribeLogDirsResponse {
    NormalizedDescribeLogDirsResponse::fixture(5, 11, 0, log_dirs, 512)
}

fn log_dir(
    path: &str,
    error_code: i16,
    topics: Vec<NormalizedDescribeLogDirsTopic>,
) -> NormalizedDescribeLogDir {
    NormalizedDescribeLogDir::fixture(
        error_code,
        path.to_owned(),
        topics,
        Some(100),
        Some(50),
        Some(false),
    )
}

fn topic(
    name: &str,
    partitions: Vec<NormalizedDescribeLogDirsPartition>,
) -> NormalizedDescribeLogDirsTopic {
    NormalizedDescribeLogDirsTopic::fixture(name.to_owned(), partitions)
}

fn partition(
    partition_index: i32,
    future: bool,
    offset_lag: i64,
) -> NormalizedDescribeLogDirsPartition {
    NormalizedDescribeLogDirsPartition::fixture(partition_index, 10, offset_lag, future)
}
