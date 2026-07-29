//! Semantic `DeleteTopics` terminal normalization scenarios.

use kafka_client_core::{DeleteTopicsInput, DeleteTopicsPlan};
use kafka_wire::{DeleteTopicsResponse, delete_topics_response::DeletableTopicResult};
use kafka_wire_core::Uuid;

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

#[test]
fn topic_id_results_normalize_in_original_order_with_null_names() {
    let first = [1; 16];
    let second = [2; 16];
    let plan = DeleteTopicsPlan::by_ids(vec![first, second])
        .unwrap_or_else(|error| panic!("valid topic-ID deletion plan: {error}"));
    let mut returned_second = DeletableTopicResult::default();
    returned_second.name = None;
    returned_second.topic_id = Uuid::from_bytes(second);
    returned_second.error_code = -32_000;
    let mut returned_first = DeletableTopicResult::default();
    returned_first.name = None;
    returned_first.topic_id = Uuid::from_bytes(first);
    let mut response = DeleteTopicsResponse::default();
    response.responses = vec![returned_second, returned_first];
    let input = normalize_terminal(&plan, usize::MAX, Ok(response))
        .unwrap_or_else(|error| panic!("normalize topic-ID terminal: {error:?}"));
    let DeleteTopicsInput::BrokerRespondedById { outcomes } = input else {
        panic!("topic-ID broker response fact expected");
    };
    assert_eq!(outcomes[0].topic_id(), first);
    let (_, kafka_client_core::DeleteTopicResult::Failed(error)) = outcomes[1].clone().into_parts()
    else {
        panic!("topic-ID broker failure expected");
    };
    assert_eq!(outcomes[1].topic_id(), second);
    assert_eq!(error.code(), -32_000);
}
