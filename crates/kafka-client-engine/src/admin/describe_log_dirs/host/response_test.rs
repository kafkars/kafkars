//! Fallible normalized-to-core conversion and retained-charge scenarios.

use kafka_client_core::{
    AdminDescribeLogDirsBrokerResult, AdminDescribeLogDirsInput, AdminLogDirResult,
};

use crate::protocol::admin::describe_log_dirs::{
    NormalizedDescribeLogDir, NormalizedDescribeLogDirsPartition,
    NormalizedDescribeLogDirsResponse, NormalizedDescribeLogDirsTopic,
};

use super::response::normalized_input;

#[test]
fn normalized_success_fallibly_copies_flattened_replica_names_and_keeps_charge() {
    let normalized = NormalizedDescribeLogDirsResponse::fixture(
        5,
        0,
        0,
        vec![log_dir(
            "/var/lib/kafka",
            0,
            vec![topic("orders", vec![partition(1), partition(3)])],
        )],
        173,
    );

    let (input, retained_bytes) =
        normalized_input(7, normalized).unwrap_or_else(|()| panic!("convert"));
    assert!(retained_bytes > 0);
    let AdminDescribeLogDirsInput::BrokerResponded {
        throttle_time_ms,
        outcome,
    } = input
    else {
        panic!("broker response expected");
    };
    assert_eq!(throttle_time_ms, 0);
    assert_eq!(outcome.broker_id(), 7);
    let AdminDescribeLogDirsBrokerResult::Described(log_dirs) = outcome.result() else {
        panic!("described broker expected");
    };
    assert_eq!(log_dirs[0].path(), "/var/lib/kafka");
    let AdminLogDirResult::Described(description) = log_dirs[0].result() else {
        panic!("described directory expected");
    };
    assert_eq!(description.replicas().len(), 2);
    assert_eq!(description.replicas()[0].topic(), "orders");
    assert_eq!(description.replicas()[0].partition(), 1);
    assert_eq!(description.replicas()[1].topic(), "orders");
    assert_eq!(description.replicas()[1].partition(), 3);
}

#[test]
fn exact_directory_error_moves_path_without_materializing_replica_terminal() {
    let normalized = NormalizedDescribeLogDirsResponse::fixture(
        5,
        0,
        0,
        vec![log_dir(
            "/offline",
            -17,
            vec![topic("ignored", vec![partition(2)])],
        )],
        181,
    );

    let (input, retained_bytes) =
        normalized_input(9, normalized).unwrap_or_else(|()| panic!("convert"));
    assert!(retained_bytes > 0);
    let AdminDescribeLogDirsInput::BrokerResponded { outcome, .. } = input else {
        panic!("broker response expected");
    };
    let AdminDescribeLogDirsBrokerResult::Described(log_dirs) = outcome.result() else {
        panic!("described broker expected");
    };
    assert_eq!(log_dirs[0].path(), "/offline");
    let AdminLogDirResult::BrokerFailed(error) = log_dirs[0].result() else {
        panic!("directory failure expected");
    };
    assert_eq!(error.code(), -17);
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
        Some(1_000),
        Some(750),
        Some(false),
    )
}

fn topic(
    name: &str,
    partitions: Vec<NormalizedDescribeLogDirsPartition>,
) -> NormalizedDescribeLogDirsTopic {
    NormalizedDescribeLogDirsTopic::fixture(name.to_owned(), partitions)
}

const fn partition(partition_index: i32) -> NormalizedDescribeLogDirsPartition {
    NormalizedDescribeLogDirsPartition::fixture(partition_index, 100, 0, false)
}
