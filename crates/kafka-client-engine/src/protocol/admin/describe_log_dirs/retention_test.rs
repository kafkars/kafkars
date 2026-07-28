//! Normalized result and scratch capacity rejection scenarios.

use core::mem::size_of;

use kafka_wire::{
    DescribeLogDirsResponse,
    describe_log_dirs_response::{
        DescribeLogDirsPartition, DescribeLogDirsResult, DescribeLogDirsTopic,
    },
};

use super::{
    DescribeLogDirsResponseFailure, DescribeLogDirsSelectionRef,
    normalize_describe_log_dirs_response, retention::response_peak_charge,
};

#[test]
fn complete_normalization_peak_must_fit_before_owned_copying() {
    let response = populated_response();
    let required = response_peak_charge(DescribeLogDirsSelectionRef::AllTopics, &response)
        .expect("bounded charge");
    assert!(required > generated_response_floor(&response).expect("generated floor"));

    assert_eq!(
        normalize_describe_log_dirs_response(
            DescribeLogDirsSelectionRef::AllTopics,
            5,
            &response,
            required - 1,
        ),
        Err(DescribeLogDirsResponseFailure::RetainedBytes {
            required,
            limit: required - 1,
        })
    );
    normalize_describe_log_dirs_response(
        DescribeLogDirsSelectionRef::AllTopics,
        5,
        &response,
        required,
    )
    .unwrap_or_else(|error| panic!("exact complete peak must fit: {error:?}"));
}

fn populated_response() -> DescribeLogDirsResponse {
    let mut partition = DescribeLogDirsPartition::default();
    partition.partition_index = 3;
    partition.partition_size = 42;
    let mut topic = DescribeLogDirsTopic::default();
    topic.name = "orders".into();
    topic.partitions = vec![partition];
    let mut log_dir = DescribeLogDirsResult::default();
    log_dir.log_dir = "/var/lib/kafka".into();
    log_dir.topics = vec![topic];
    let mut response = DescribeLogDirsResponse::default();
    response.results = vec![log_dir];
    response
}

fn generated_response_floor(response: &DescribeLogDirsResponse) -> Option<usize> {
    let mut charge = response
        .results
        .len()
        .checked_mul(size_of::<DescribeLogDirsResult>())?;
    for log_dir in &response.results {
        charge = charge.checked_add(log_dir.log_dir.len())?.checked_add(
            log_dir
                .topics
                .len()
                .checked_mul(size_of::<DescribeLogDirsTopic>())?,
        )?;
        for topic in &log_dir.topics {
            charge = charge.checked_add(topic.name.len())?.checked_add(
                topic
                    .partitions
                    .len()
                    .checked_mul(size_of::<DescribeLogDirsPartition>())?,
            )?;
        }
    }
    Some(charge)
}
