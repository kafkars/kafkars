//! Response correlation and owned-result peak capacity scenarios.

use kafka_wire::{
    AlterReplicaLogDirsResponse,
    alter_replica_log_dirs_response::{
        AlterReplicaLogDirPartitionResult, AlterReplicaLogDirTopicResult,
    },
};

use super::{
    AlterReplicaLogDirAssignmentRef, AlterReplicaLogDirsResponseFailure,
    normalize_alter_replica_log_dirs_response, retention::response_peak_charge,
};

#[test]
fn response_and_correlation_peak_must_fit_before_owned_copying() {
    let assignments = [
        AlterReplicaLogDirAssignmentRef::new("orders", 1, "/data"),
        AlterReplicaLogDirAssignmentRef::new("orders", 2, "/data"),
    ];
    let response = response();
    let required = response_peak_charge(&assignments, 2).expect("bounded response peak");
    assert_eq!(
        normalize_alter_replica_log_dirs_response(&assignments, 2, &response, required - 1,),
        Err(AlterReplicaLogDirsResponseFailure::RetainedBytes {
            required,
            limit: required - 1,
        })
    );
    normalize_alter_replica_log_dirs_response(&assignments, 2, &response, required)
        .unwrap_or_else(|error| panic!("exact response peak: {error:?}"));
}

fn response() -> AlterReplicaLogDirsResponse {
    let mut first = AlterReplicaLogDirPartitionResult::default();
    first.partition_index = 1;
    let mut second = AlterReplicaLogDirPartitionResult::default();
    second.partition_index = 2;
    let mut topic = AlterReplicaLogDirTopicResult::default();
    topic.topic_name = "orders".into();
    topic.partitions = vec![first, second];
    let mut response = AlterReplicaLogDirsResponse::default();
    response.results = vec![topic];
    response
}
