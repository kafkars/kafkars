//! Semantic `DeleteTopics` terminal normalization scenarios.

use kafka_client_core::{DeleteTopicsInput, DeleteTopicsPlan};
use kafka_wire::{DeleteTopicsResponse, delete_topics_response::DeletableTopicResult};

use super::delete_topics_terminal::normalize_terminal;

#[test]
fn broker_results_normalize_without_losing_exact_codes() {
    let plan = DeleteTopicsPlan::new(vec!["orders".to_owned()])
        .unwrap_or_else(|error| panic!("valid deletion plan: {error}"));
    let mut topic = DeletableTopicResult::default();
    topic.name = Some("orders".into());
    topic.error_code = -32_000;
    let mut response = DeleteTopicsResponse::default();
    response.responses = vec![topic];
    let input = normalize_terminal(&plan, usize::MAX, Ok(response))
        .unwrap_or_else(|error| panic!("normalize terminal: {error:?}"));
    let DeleteTopicsInput::BrokerResponded { outcomes } = input else {
        panic!("broker response fact expected");
    };
    let (_, kafka_client_core::DeleteTopicResult::Failed(error)) = outcomes
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("one outcome"))
        .into_parts()
    else {
        panic!("broker failure expected");
    };
    assert_eq!(error.code(), -32_000);
}
