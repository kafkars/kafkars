//! Bounded UTF-8-safe `DeleteTopics` diagnostic scenarios.

use kafka_client_core::{DeleteTopicResult, DeleteTopicsPlan};
use kafka_wire::{DeleteTopicsResponse, delete_topics_response::DeletableTopicResult};

use super::delete_topics::normalize_delete_topics_response_bounded;
use crate::admin::retention::RESULT_DIAGNOSTIC_BYTES_PER_TOPIC;

#[test]
fn oversized_utf8_diagnostic_is_bounded_and_reports_truncation() {
    let plan = DeleteTopicsPlan::new(vec!["orders".to_owned()])
        .unwrap_or_else(|error| panic!("valid deletion plan: {error}"));
    let mut result = DeletableTopicResult::default();
    result.name = Some("orders".into());
    result.error_code = -1;
    result.error_message = Some("é".repeat(RESULT_DIAGNOSTIC_BYTES_PER_TOPIC).into());
    let mut response = DeleteTopicsResponse::default();
    response.responses = vec![result];
    let outcomes = normalize_delete_topics_response_bounded(&plan, &response, usize::MAX)
        .unwrap_or_else(|error| panic!("bounded response: {error:?}"));
    let (_, DeleteTopicResult::Failed(error)) = outcomes
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
