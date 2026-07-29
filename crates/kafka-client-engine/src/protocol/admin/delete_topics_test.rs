//! Generated `DeleteTopics` request and response correlation scenarios.

use kafka_client_core::{DeleteTopicResult, DeleteTopicsPlan};
use kafka_wire::{DeleteTopicsResponse, delete_topics_response::DeletableTopicResult};
use kafka_wire_core::Uuid;

use super::delete_topics::{
    DeleteTopicsProtocolFailure, DeleteTopicsRequestError, delete_topics_request,
    normalize_delete_topic_ids_response_bounded, normalize_delete_topics_response_bounded,
};

fn plan() -> DeleteTopicsPlan {
    DeleteTopicsPlan::new(vec!["orders".to_owned(), "audit".to_owned()])
        .unwrap_or_else(|error| panic!("valid deletion plan: {error}"))
}

fn result(topic: Option<&str>, error_code: i16, message: Option<&str>) -> DeletableTopicResult {
    let mut result = DeletableTopicResult::default();
    result.name = topic.map(Into::into);
    result.error_code = error_code;
    result.error_message = message.map(Into::into);
    result
}

fn id_result(
    topic_id: [u8; 16],
    topic: Option<&str>,
    error_code: i16,
    message: Option<&str>,
) -> DeletableTopicResult {
    let mut result = result(topic, error_code, message);
    result.topic_id = Uuid::from_bytes(topic_id);
    result
}

fn response(results: Vec<DeletableTopicResult>) -> DeleteTopicsResponse {
    let mut response = DeleteTopicsResponse::default();
    response.responses = results;
    response
}

#[test]
fn generated_topic_id_request_uses_only_v6_identity_structs() {
    let first = [1; 16];
    let second = [2; 16];
    let plan = DeleteTopicsPlan::by_ids(vec![first, second])
        .unwrap_or_else(|error| panic!("valid topic-ID deletion plan: {error}"));
    let request = delete_topics_request(&plan, 54_321)
        .unwrap_or_else(|error| panic!("valid generated request: {error:?}"));
    assert!(request.topic_names.is_empty());
    assert_eq!(request.timeout_ms, 54_321);
    assert_eq!(request.topics.len(), 2);
    assert_eq!(request.topics[0].name, None);
    assert_eq!(request.topics[0].topic_id, Uuid::from_bytes(first));
    assert_eq!(request.topics[1].name, None);
    assert_eq!(request.topics[1].topic_id, Uuid::from_bytes(second));
}

#[test]
fn generated_name_request_preserves_order_and_timeout() {
    let request = delete_topics_request(&plan(), 12_345)
        .unwrap_or_else(|error| panic!("valid generated request: {error:?}"));
    assert!(request.topics.is_empty());
    assert_eq!(request.timeout_ms, 12_345);
    assert_eq!(request.topic_names[0].as_str(), "orders");
    assert_eq!(request.topic_names[1].as_str(), "audit");
    assert_eq!(
        delete_topics_request(&plan(), -1),
        Err(DeleteTopicsRequestError::NegativeTimeout)
    );
}

#[test]
fn response_is_reordered_to_request_order_and_unknown_code_is_lossless() {
    let response = response(vec![
        result(Some("audit"), -32_000, Some("future broker code")),
        result(Some("orders"), 0, None),
    ]);
    let outcomes = normalize_delete_topics_response_bounded(&plan(), &response, usize::MAX)
        .unwrap_or_else(|error| panic!("correlatable response: {error:?}"));
    assert_eq!(outcomes[0].topic(), "orders");
    assert!(matches!(
        outcomes[0].clone().into_parts().1,
        DeleteTopicResult::Deleted
    ));
    let (_, DeleteTopicResult::Failed(error)) = outcomes[1].clone().into_parts() else {
        panic!("unknown broker code became a false success");
    };
    assert_eq!(error.code(), -32_000);
    assert_eq!(error.message(), Some("future broker code"));
}

#[test]
fn nullable_or_ambiguous_names_never_bind_to_the_wrong_topic() {
    let missing_name = response(vec![result(None, 0, None), result(Some("audit"), 0, None)]);
    assert_eq!(
        normalize_delete_topics_response_bounded(&plan(), &missing_name, usize::MAX),
        Err(DeleteTopicsProtocolFailure::MissingResponseName)
    );
    let unexpected = response(vec![
        result(Some("orders"), 0, None),
        result(Some("payments"), 0, None),
    ]);
    assert_eq!(
        normalize_delete_topics_response_bounded(&plan(), &unexpected, usize::MAX),
        Err(DeleteTopicsProtocolFailure::UnexpectedTopic)
    );
    let duplicate = response(vec![
        result(Some("orders"), 0, None),
        result(Some("orders"), 0, None),
    ]);
    assert_eq!(
        normalize_delete_topics_response_bounded(&plan(), &duplicate, usize::MAX),
        Err(DeleteTopicsProtocolFailure::DuplicateTopic)
    );
}

#[test]
fn oversized_unexpected_name_is_rejected_without_retaining_a_copy() {
    let hostile_name = "x".repeat(4 * 1024 * 1024);
    let unexpected = response(vec![
        result(Some("orders"), 0, None),
        result(Some(&hostile_name), 0, None),
    ]);

    assert_eq!(
        normalize_delete_topics_response_bounded(&plan(), &unexpected, usize::MAX),
        Err(DeleteTopicsProtocolFailure::UnexpectedTopic)
    );
}

#[test]
fn topic_id_response_uses_ids_in_caller_order_and_accepts_null_names() {
    let first = [1; 16];
    let second = [2; 16];
    let plan = DeleteTopicsPlan::by_ids(vec![first, second])
        .unwrap_or_else(|error| panic!("valid topic-ID deletion plan: {error}"));
    let response = response(vec![
        id_result(second, None, -32_000, Some("future broker code")),
        id_result(first, None, 0, None),
    ]);
    let outcomes = normalize_delete_topic_ids_response_bounded(&plan, &response, usize::MAX)
        .unwrap_or_else(|error| panic!("correlatable topic-ID response: {error:?}"));
    assert_eq!(outcomes[0].topic_id(), first);
    assert!(matches!(
        outcomes[0].clone().into_parts().1,
        DeleteTopicResult::Deleted
    ));
    let (_, DeleteTopicResult::Failed(error)) = outcomes[1].clone().into_parts() else {
        panic!("unknown broker code became a false success");
    };
    assert_eq!(outcomes[1].topic_id(), second);
    assert_eq!(error.code(), -32_000);
    assert_eq!(error.message(), Some("future broker code"));
}

#[test]
fn topic_id_response_rejects_unknown_and_duplicate_ids_without_using_names() {
    let first = [1; 16];
    let second = [2; 16];
    let plan = DeleteTopicsPlan::by_ids(vec![first, second])
        .unwrap_or_else(|error| panic!("valid topic-ID deletion plan: {error}"));
    let unknown = response(vec![
        id_result(first, Some("wrong-name"), 0, None),
        id_result([9; 16], None, 0, None),
    ]);
    assert_eq!(
        normalize_delete_topic_ids_response_bounded(&plan, &unknown, usize::MAX),
        Err(DeleteTopicsProtocolFailure::UnexpectedTopicId)
    );
    let duplicate = response(vec![
        id_result(first, None, 0, None),
        id_result(first, None, 0, None),
    ]);
    assert_eq!(
        normalize_delete_topic_ids_response_bounded(&plan, &duplicate, usize::MAX),
        Err(DeleteTopicsProtocolFailure::DuplicateTopicId)
    );
}
