//! Bounded `CreateTopics` result-retention scenarios.

use kafka_client_core::{CreateTopicResult, CreateTopicSpecification, CreateTopicsPlan};
use kafka_wire::{CreateTopicsResponse, create_topics_response::CreatableTopicResult};

use super::result_budget::{bounded_message, normalize};
use crate::admin::retention::{RESULT_DIAGNOSTIC_BYTES_PER_TOPIC, result_fixed_charge};

fn plan(names: &[&str]) -> CreateTopicsPlan {
    let topics = names
        .iter()
        .map(|name| CreateTopicSpecification::new(*name, 1, -1, Vec::new()))
        .collect();
    CreateTopicsPlan::new(topics, false)
        .unwrap_or_else(|error| panic!("valid CreateTopics plan: {error}"))
}

fn response(results: Vec<(&str, i16, Option<&str>)>) -> CreateTopicsResponse {
    let mut response = CreateTopicsResponse::default();
    response.topics = results
        .into_iter()
        .map(|(name, code, message)| {
            let mut result = CreatableTopicResult::default();
            result.name = name.into();
            result.error_code = code;
            result.error_message = message.map(Into::into);
            result
        })
        .collect();
    response
}

fn reserved_bytes(plan: &CreateTopicsPlan) -> usize {
    let topic_bytes = plan.topics().iter().map(|topic| topic.name().len()).sum();
    result_fixed_charge(plan.topics().len(), topic_bytes)
        .and_then(|fixed| {
            fixed.checked_add(plan.topics().len() * RESULT_DIAGNOSTIC_BYTES_PER_TOPIC)
        })
        .unwrap_or_else(|| panic!("small result reservation fits"))
}

#[test]
fn diagnostic_cap_truncates_on_a_utf8_boundary_and_preserves_exact_code() {
    let plan = plan(&["orders"]);
    let message = "€".repeat(RESULT_DIAGNOSTIC_BYTES_PER_TOPIC);
    let outcomes = normalize(
        &plan,
        &response(vec![("orders", -32_000, Some(&message))]),
        reserved_bytes(&plan),
    )
    .unwrap_or_else(|error| panic!("bounded result: {error:?}"));
    let CreateTopicResult::Failed(error) = outcomes[0].result() else {
        panic!("broker error expected");
    };

    assert_eq!(error.code(), -32_000);
    assert_eq!(
        error.message().map(str::len),
        Some(RESULT_DIAGNOSTIC_BYTES_PER_TOPIC - 1)
    );
    assert!(
        error
            .message()
            .is_some_and(|message| message.is_char_boundary(message.len()))
    );
    assert!(error.message_truncated());
}

#[test]
fn absent_message_differs_from_a_present_message_truncated_to_empty() {
    assert_eq!(bounded_message(None, 0), (None, false));
    assert_eq!(
        bounded_message(Some("present"), 0),
        (Some(String::new()), true)
    );
}

#[test]
fn fixed_results_and_one_kibibyte_per_topic_must_fit_the_admitted_reservation() {
    let plan = plan(&["orders"]);
    let required = reserved_bytes(&plan);
    let broker = response(vec![("orders", 1, Some("broker diagnostic"))]);

    assert!(normalize(&plan, &broker, required).is_ok());
    assert_eq!(
        normalize(&plan, &broker, required - 1),
        Err(super::create_topics::CreateTopicsProtocolFailure::RetainedBytes)
    );
}
