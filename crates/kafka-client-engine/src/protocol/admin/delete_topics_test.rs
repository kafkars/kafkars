//! Generated `DeleteTopics` request and response correlation scenarios.

use kafka_client_core::{DeleteTopicResult, DeleteTopicsPlan};
use kafka_wire::{DeleteTopicsResponse, delete_topics_response::DeletableTopicResult};

use super::delete_topics::{
    DeleteTopicsProtocolFailure, DeleteTopicsRequestError, delete_topics_request,
    normalize_delete_topics_response_bounded,
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

fn response(results: Vec<DeletableTopicResult>) -> DeleteTopicsResponse {
    let mut response = DeleteTopicsResponse::default();
    response.responses = results;
    response
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
