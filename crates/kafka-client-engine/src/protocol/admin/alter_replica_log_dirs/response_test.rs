//! Selected-version, exact-code, and caller-order response scenarios.

use kafka_wire::{
    AlterReplicaLogDirsResponse,
    alter_replica_log_dirs_response::{
        AlterReplicaLogDirPartitionResult, AlterReplicaLogDirTopicResult,
    },
};

use super::{
    AlterReplicaLogDirAssignmentRef, AlterReplicaLogDirsResponseFailure,
    normalize_alter_replica_log_dirs_response,
};

#[test]
fn response_restores_caller_order_and_preserves_exact_signed_codes() {
    let assignments = [
        assignment("zeta", 3),
        assignment("alpha", 2),
        assignment("alpha", 1),
    ];
    let mut generated = response(vec![
        topic("alpha", vec![partition(1, -31_999), partition(2, 0)]),
        topic("zeta", vec![partition(3, i16::MAX)]),
    ]);
    generated.throttle_time_ms = 91;
    let normalized =
        normalize_alter_replica_log_dirs_response(&assignments, 2, &generated, usize::MAX)
            .unwrap_or_else(|error| panic!("valid response: {error:?}"));

    assert_eq!(normalized.selected_version(), 2);
    assert_eq!(normalized.throttle_time_ms(), 91);
    assert_eq!(normalized.outcomes()[0].topic(), "zeta");
    assert_eq!(normalized.outcomes()[0].partition(), 3);
    assert_eq!(normalized.outcomes()[0].error_code(), i16::MAX);
    assert_eq!(normalized.outcomes()[1].error_code(), 0);
    assert_eq!(normalized.outcomes()[2].error_code(), -31_999);
}

#[test]
fn unsupported_version_and_negative_throttle_are_rejected() {
    let assignments = [assignment("orders", 1)];
    let mut generated = response(vec![topic("orders", vec![partition(1, 0)])]);
    for actual in [i16::MIN, 0, 3, i16::MAX] {
        assert_eq!(
            normalize_alter_replica_log_dirs_response(&assignments, actual, &generated, usize::MAX),
            Err(AlterReplicaLogDirsResponseFailure::UnsupportedApiVersion { actual })
        );
    }
    generated.throttle_time_ms = -1;
    assert_eq!(
        normalize_alter_replica_log_dirs_response(&assignments, 1, &generated, usize::MAX),
        Err(AlterReplicaLogDirsResponseFailure::NegativeThrottleTime { actual: -1 })
    );
}

#[test]
fn missing_unexpected_and_duplicate_identities_never_correlate() {
    let assignments = [assignment("orders", 1), assignment("orders", 2)];
    assert_eq!(
        normalize_alter_replica_log_dirs_response(
            &assignments,
            2,
            &response(vec![topic("orders", vec![partition(1, 0)])]),
            usize::MAX,
        ),
        Err(AlterReplicaLogDirsResponseFailure::PartitionCount)
    );
    assert_eq!(
        normalize_alter_replica_log_dirs_response(
            &assignments,
            2,
            &response(vec![topic(
                "orders",
                vec![partition(1, 0), partition(3, 0)]
            )]),
            usize::MAX,
        ),
        Err(AlterReplicaLogDirsResponseFailure::MissingPartition { actual: 2 })
    );
    assert_eq!(
        normalize_alter_replica_log_dirs_response(
            &assignments,
            2,
            &response(vec![topic(
                "orders",
                vec![partition(1, 0), partition(1, 7)]
            )]),
            usize::MAX,
        ),
        Err(AlterReplicaLogDirsResponseFailure::DuplicatePartition { actual: 1 })
    );
}

fn assignment(topic: &str, partition: i32) -> AlterReplicaLogDirAssignmentRef<'_> {
    AlterReplicaLogDirAssignmentRef::new(topic, partition, "/data")
}

fn partition(partition_index: i32, error_code: i16) -> AlterReplicaLogDirPartitionResult {
    let mut partition = AlterReplicaLogDirPartitionResult::default();
    partition.partition_index = partition_index;
    partition.error_code = error_code;
    partition
}

fn topic(
    name: &str,
    partitions: Vec<AlterReplicaLogDirPartitionResult>,
) -> AlterReplicaLogDirTopicResult {
    let mut topic = AlterReplicaLogDirTopicResult::default();
    topic.topic_name = name.into();
    topic.partitions = partitions;
    topic
}

fn response(results: Vec<AlterReplicaLogDirTopicResult>) -> AlterReplicaLogDirsResponse {
    let mut response = AlterReplicaLogDirsResponse::default();
    response.results = results;
    response
}
