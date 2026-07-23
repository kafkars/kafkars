//! Stable bounded retention of ordered `CreateTopics` broker diagnostics.

use core::num::NonZeroI16;

use kafka_client_core::{CreateTopicBrokerError, CreateTopicOutcome, CreateTopicsPlan};
use kafka_wire::CreateTopicsResponse;

use super::create_topics::{CreateTopicsProtocolFailure, matching_result, validate_response_shape};
use crate::admin::retention::{RESULT_DIAGNOSTIC_BYTES_PER_TOPIC, result_fixed_charge};

pub(super) fn normalize(
    plan: &CreateTopicsPlan,
    response: &CreateTopicsResponse,
    retained_bytes: usize,
) -> Result<Vec<CreateTopicOutcome>, CreateTopicsProtocolFailure> {
    validate_response_shape(plan, response)?;
    let topic_bytes = plan
        .topics()
        .iter()
        .try_fold(0usize, |bytes, topic| bytes.checked_add(topic.name().len()))
        .ok_or(CreateTopicsProtocolFailure::RetainedBytes)?;
    let fixed = result_fixed_charge(plan.topics().len(), topic_bytes)
        .ok_or(CreateTopicsProtocolFailure::RetainedBytes)?;
    let required_diagnostics = plan
        .topics()
        .len()
        .checked_mul(RESULT_DIAGNOSTIC_BYTES_PER_TOPIC)
        .ok_or(CreateTopicsProtocolFailure::RetainedBytes)?;
    let available = retained_bytes
        .checked_sub(fixed)
        .ok_or(CreateTopicsProtocolFailure::RetainedBytes)?;
    if available < required_diagnostics {
        return Err(CreateTopicsProtocolFailure::RetainedBytes);
    }
    let mut outcomes = Vec::with_capacity(plan.topics().len());
    for topic in plan.topics() {
        let result = matching_result(topic.name(), &response.topics)?;
        let Some(code) = NonZeroI16::new(result.error_code) else {
            outcomes.push(CreateTopicOutcome::created(topic.name()));
            continue;
        };
        #[allow(
            clippy::redundant_closure_for_method_calls,
            reason = "the generated string type is intentionally not a direct engine dependency"
        )]
        let message = result.error_message.as_ref().map(|value| value.as_str());
        let (message, truncated) = bounded_message(message, RESULT_DIAGNOSTIC_BYTES_PER_TOPIC);
        outcomes.push(CreateTopicOutcome::failed(
            topic.name(),
            CreateTopicBrokerError::with_bounded_message(code, message, truncated),
        ));
    }
    Ok(outcomes)
}

pub(super) fn bounded_message(message: Option<&str>, allowance: usize) -> (Option<String>, bool) {
    let Some(message) = message else {
        return (None, false);
    };
    let retained = floor_char_boundary(message, allowance.min(message.len()));
    (
        Some(message[..retained].to_owned()),
        retained < message.len(),
    )
}

fn floor_char_boundary(value: &str, mut index: usize) -> usize {
    while !value.is_char_boundary(index) {
        index = index.saturating_sub(1);
    }
    index
}
