//! Lossless and bounded terminal normalization for tracked `CreateTopics` calls.

use kafka_client_core::{
    CreateTopicResult, CreateTopicSpecification, CreateTopicsInput, CreateTopicsPlan,
};
use kafka_wire::{CreateTopicsResponse, create_topics_response::CreatableTopicResult};

use super::create_topics_terminal::normalize_terminal;

fn plan() -> CreateTopicsPlan {
    CreateTopicsPlan::new(
        vec![CreateTopicSpecification::new("orders", 3, -1, Vec::new())],
        false,
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"))
}

#[test]
fn exact_unknown_code_survives_tracked_terminal_normalization() {
    let mut topic = CreatableTopicResult::default();
    topic.name = "orders".into();
    topic.error_code = -32123;
    topic.error_message = Some("future broker code".into());
    let mut response = CreateTopicsResponse::default();
    response.topics = vec![topic];

    let input = normalize_terminal(&plan(), 32 * 1024, Ok(response))
        .unwrap_or_else(|error| panic!("normalize tracked terminal: {error:?}"));
    let CreateTopicsInput::BrokerResponded { outcomes } = input else {
        panic!("ordered broker outcomes expected");
    };
    let CreateTopicResult::Failed(error) = outcomes[0].result() else {
        panic!("broker error expected");
    };
    assert_eq!(error.code(), -32123);
    assert_eq!(error.message(), Some("future broker code"));
}

#[test]
fn oversized_diagnostic_preserves_code_and_marks_bounded_prefix() {
    let mut topic = CreatableTopicResult::default();
    topic.name = "orders".into();
    topic.error_code = 1;
    topic.error_message = Some("x".repeat(64 * 1024).into());
    let mut response = CreateTopicsResponse::default();
    response.topics = vec![topic];

    let CreateTopicsInput::BrokerResponded { outcomes } =
        normalize_terminal(&plan(), 16 * 1024, Ok(response))
            .unwrap_or_else(|error| panic!("bound tracked terminal: {error:?}"))
    else {
        panic!("bounded per-topic outcomes expected");
    };
    let CreateTopicResult::Failed(error) = outcomes[0].result() else {
        panic!("exact broker error expected");
    };
    assert_eq!(error.code(), 1);
    assert!(error.message_truncated());
    assert!(
        error
            .message()
            .is_some_and(|message| message.len() < 64 * 1024)
    );
}

#[test]
fn malformed_response_is_distinct_from_transport_failure() {
    let mut response = CreateTopicsResponse::default();
    response.topics = Vec::new();

    assert_eq!(
        normalize_terminal(&plan(), 16 * 1024, Ok(response)),
        Ok(CreateTopicsInput::InvalidResponse)
    );
}

#[test]
fn fixed_result_structure_overflow_remains_explicit() {
    let mut topic = CreatableTopicResult::default();
    topic.name = "orders".into();
    let mut response = CreateTopicsResponse::default();
    response.topics = vec![topic];

    assert_eq!(
        normalize_terminal(&plan(), 1, Ok(response)),
        Err(crate::protocol::admin::create_topics::CreateTopicsProtocolFailure::RetainedBytes)
    );
}
