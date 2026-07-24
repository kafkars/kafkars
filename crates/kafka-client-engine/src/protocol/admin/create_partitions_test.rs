//! Generated automatic-assignment request and response scenarios.

use kafka_client_core::{
    CreatePartitionsPlan, CreatePartitionsSpecification, PartitionIncreaseResult,
};
use kafka_wire::{
    CreatePartitionsResponse, create_partitions_response::CreatePartitionsTopicResult,
};

use super::create_partitions::{
    CreatePartitionsProtocolFailure, CreatePartitionsRequestError, create_partitions_request,
    normalize_create_partitions_response_bounded,
};

fn plan() -> CreatePartitionsPlan {
    CreatePartitionsPlan::new(
        vec![
            CreatePartitionsSpecification::new("orders".to_owned(), 8),
            CreatePartitionsSpecification::new("audit".to_owned(), 4),
        ],
        true,
    )
    .unwrap_or_else(|error| panic!("valid partition plan: {error}"))
}

fn result(topic: &str, error_code: i16, message: Option<&str>) -> CreatePartitionsTopicResult {
    let mut result = CreatePartitionsTopicResult::default();
    result.name = topic.into();
    result.error_code = error_code;
    result.error_message = message.map(Into::into);
    result
}

fn response(results: Vec<CreatePartitionsTopicResult>) -> CreatePartitionsResponse {
    let mut response = CreatePartitionsResponse::default();
    response.results = results;
    response
}

#[test]
fn request_uses_generated_automatic_assignments_and_original_options() {
    let request = create_partitions_request(&plan(), 12_345)
        .unwrap_or_else(|error| panic!("valid generated request: {error:?}"));
    assert_eq!(request.timeout_ms, 12_345);
    assert!(request.validate_only);
    assert_eq!(request.topics[0].name.as_str(), "orders");
    assert_eq!(request.topics[0].count, 8);
    assert_eq!(request.topics[0].assignments, None);
    assert_eq!(
        create_partitions_request(&plan(), -1),
        Err(CreatePartitionsRequestError::NegativeTimeout)
    );
}

#[test]
fn response_is_reordered_and_unknown_code_is_lossless() {
    let response = response(vec![
        result("audit", -32_000, Some("future broker code")),
        result("orders", 0, None),
    ]);
    let outcomes = normalize_create_partitions_response_bounded(&plan(), &response, usize::MAX)
        .unwrap_or_else(|error| panic!("correlatable response: {error:?}"));
    assert_eq!(outcomes[0].topic(), "orders");
    assert!(matches!(
        outcomes[0].clone().into_parts().1,
        PartitionIncreaseResult::Increased
    ));
    let (_, PartitionIncreaseResult::Failed(error)) = outcomes[1].clone().into_parts() else {
        panic!("unknown broker code became a false success");
    };
    assert_eq!(error.code(), -32_000);
    assert_eq!(error.message(), Some("future broker code"));
}

#[test]
fn ambiguous_names_never_bind_to_the_wrong_topic() {
    let unexpected = response(vec![result("orders", 0, None), result("payments", 0, None)]);
    assert_eq!(
        normalize_create_partitions_response_bounded(&plan(), &unexpected, usize::MAX),
        Err(CreatePartitionsProtocolFailure::UnexpectedTopic)
    );
    let duplicate = response(vec![result("orders", 0, None), result("orders", 0, None)]);
    assert_eq!(
        normalize_create_partitions_response_bounded(&plan(), &duplicate, usize::MAX),
        Err(CreatePartitionsProtocolFailure::DuplicateTopic)
    );
}
