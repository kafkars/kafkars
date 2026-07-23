//! Generated `CreateTopics` request and response boundary scenarios.

use kafka_client_core::{
    CreateTopicConfig, CreateTopicResult, CreateTopicSpecification, CreateTopicsPlan,
};
use kafka_wire::{CreateTopicsResponse, create_topics_response::CreatableTopicResult};

use super::create_topics::{
    CreateTopicsProtocolFailure, CreateTopicsRequestError, create_topics_request,
    normalize_create_topics_response_bounded, remaining_timeout_ms,
};

fn plan() -> CreateTopicsPlan {
    CreateTopicsPlan::new(
        vec![
            CreateTopicSpecification::new(
                "orders",
                3,
                2,
                vec![
                    CreateTopicConfig::new("cleanup.policy", Some("compact".to_owned())),
                    CreateTopicConfig::new("retention.ms", None),
                ],
            ),
            CreateTopicSpecification::new("audit", 1, -1, Vec::new()),
        ],
        true,
    )
    .unwrap_or_else(|error| panic!("valid CreateTopics test plan: {error}"))
}

fn result(topic: &str, error_code: i16, message: Option<&str>) -> CreatableTopicResult {
    let mut result = CreatableTopicResult::default();
    result.name = topic.into();
    result.error_code = error_code;
    result.error_message = message.map(Into::into);
    result
}

fn response(topics: Vec<CreatableTopicResult>) -> CreateTopicsResponse {
    let mut response = CreateTopicsResponse::default();
    response.topics = topics;
    response
}

#[test]
fn generated_request_preserves_order_nullable_configs_and_broker_timeout() {
    let request = create_topics_request(&plan(), 12_345)
        .unwrap_or_else(|error| panic!("valid generated request: {error:?}"));

    assert_eq!(request.timeout_ms, 12_345);
    assert!(request.validate_only);
    assert_eq!(request.topics[0].name.as_str(), "orders");
    assert_eq!(request.topics[1].name.as_str(), "audit");
    assert_eq!(request.topics[0].num_partitions, 3);
    assert_eq!(request.topics[0].replication_factor, 2);
    assert_eq!(request.topics[0].configs[0].name.as_str(), "cleanup.policy");
    assert!(matches!(
        request.topics[0].configs[0].value.as_ref(),
        Some(value) if value.as_str() == "compact"
    ));
    assert_eq!(request.topics[0].configs[1].value, None);
    assert_eq!(
        create_topics_request(&plan(), -1),
        Err(CreateTopicsRequestError::NegativeTimeout)
    );
}

#[test]
fn remaining_timeout_uses_the_original_absolute_deadline() {
    assert_eq!(
        remaining_timeout_ms(
            kafka_client_core::Moment::from_tick(1_000_001),
            kafka_client_core::Deadline::from_tick(2_000_000),
        ),
        Ok(1)
    );
    assert_eq!(
        remaining_timeout_ms(
            kafka_client_core::Moment::from_tick(2_000_000),
            kafka_client_core::Deadline::from_tick(2_000_000),
        ),
        Err(CreateTopicsRequestError::DeadlineElapsed)
    );
}

#[test]
fn response_is_reordered_to_request_order_and_unknown_code_is_lossless() {
    let response = response(vec![
        result("audit", -32_000, Some("future broker code")),
        result("orders", 0, None),
    ]);
    let outcomes = normalize_create_topics_response_bounded(&plan(), &response, usize::MAX)
        .unwrap_or_else(|error| panic!("correlatable response: {error:?}"));

    assert_eq!(outcomes[0].topic(), "orders");
    assert_eq!(outcomes[0].result(), &CreateTopicResult::Created);
    assert_eq!(outcomes[1].topic(), "audit");
    let CreateTopicResult::Failed(error) = outcomes[1].result() else {
        panic!("unknown broker code became a false success");
    };
    assert_eq!(error.code(), -32_000);
    assert_eq!(error.message(), Some("future broker code"));
}

#[test]
fn structural_mismatches_never_bind_results_to_the_wrong_topic() {
    let count = response(vec![result("orders", 0, None)]);
    assert_eq!(
        normalize_create_topics_response_bounded(&plan(), &count, usize::MAX),
        Err(CreateTopicsProtocolFailure::TopicCount {
            expected: 2,
            actual: 1,
        })
    );

    let unexpected = response(vec![result("orders", 0, None), result("payments", 0, None)]);
    assert_eq!(
        normalize_create_topics_response_bounded(&plan(), &unexpected, usize::MAX),
        Err(CreateTopicsProtocolFailure::UnexpectedTopic {
            topic: "payments".to_owned(),
        })
    );

    let duplicate = response(vec![result("orders", 0, None), result("orders", 0, None)]);
    assert_eq!(
        normalize_create_topics_response_bounded(&plan(), &duplicate, usize::MAX),
        Err(CreateTopicsProtocolFailure::DuplicateTopic {
            topic: "orders".to_owned(),
        })
    );
}
