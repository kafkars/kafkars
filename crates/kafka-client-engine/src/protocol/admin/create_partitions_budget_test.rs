//! UTF-8-safe bounded `CreatePartitions` diagnostic scenarios.

use kafka_client_core::{
    CreatePartitionsPlan, CreatePartitionsSpecification, PartitionIncreaseResult,
};
use kafka_wire::{
    CreatePartitionsResponse, create_partitions_response::CreatePartitionsTopicResult,
};

use super::create_partitions::normalize_create_partitions_response_bounded;
use crate::admin::retention::RESULT_DIAGNOSTIC_BYTES_PER_TOPIC;

#[test]
fn oversized_diagnostic_is_bounded_and_reports_truncation() {
    let plan = CreatePartitionsPlan::new(
        vec![CreatePartitionsSpecification::new("orders".to_owned(), 8)],
        false,
    )
    .unwrap_or_else(|error| panic!("valid partition plan: {error}"));
    let mut result = CreatePartitionsTopicResult::default();
    result.name = "orders".into();
    result.error_code = -1;
    result.error_message = Some("é".repeat(RESULT_DIAGNOSTIC_BYTES_PER_TOPIC).into());
    let mut response = CreatePartitionsResponse::default();
    response.results = vec![result];
    let outcomes = normalize_create_partitions_response_bounded(&plan, &response, usize::MAX)
        .unwrap_or_else(|error| panic!("bounded response: {error:?}"));
    let (_, PartitionIncreaseResult::Failed(error)) = outcomes
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("one outcome expected"))
        .into_parts()
    else {
        panic!("broker failure expected");
    };
    assert!(error.message_truncated());
    assert_eq!(
        error.message().map(str::len),
        Some(RESULT_DIAGNOSTIC_BYTES_PER_TOPIC)
    );
}
