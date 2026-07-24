//! Stable bounded retention of ordered `DeleteTopics` broker diagnostics.

use core::num::NonZeroI16;

use kafka_client_core::{DeleteTopicBrokerError, DeleteTopicOutcome, DeleteTopicsPlan};
use kafka_wire::DeleteTopicsResponse;

use super::{
    delete_topics::{DeleteTopicsProtocolFailure, matching_result, validate_response_shape},
    result_budget::bounded_message,
};
use crate::admin::retention::{RESULT_DIAGNOSTIC_BYTES_PER_TOPIC, result_fixed_charge};

pub(super) fn normalize(
    plan: &DeleteTopicsPlan,
    response: &DeleteTopicsResponse,
    retained_bytes: usize,
) -> Result<Vec<DeleteTopicOutcome>, DeleteTopicsProtocolFailure> {
    validate_response_shape(plan, response)?;
    let topic_bytes = plan
        .topics()
        .iter()
        .try_fold(0usize, |bytes, topic| bytes.checked_add(topic.len()))
        .ok_or(DeleteTopicsProtocolFailure::RetainedBytes)?;
    let fixed = result_fixed_charge(plan.topics().len(), topic_bytes)
        .ok_or(DeleteTopicsProtocolFailure::RetainedBytes)?;
    let required_diagnostics = plan
        .topics()
        .len()
        .checked_mul(RESULT_DIAGNOSTIC_BYTES_PER_TOPIC)
        .ok_or(DeleteTopicsProtocolFailure::RetainedBytes)?;
    let available = retained_bytes
        .checked_sub(fixed)
        .ok_or(DeleteTopicsProtocolFailure::RetainedBytes)?;
    if available < required_diagnostics {
        return Err(DeleteTopicsProtocolFailure::RetainedBytes);
    }
    let mut outcomes = Vec::with_capacity(plan.topics().len());
    for topic in plan.topics() {
        let result = matching_result(topic, &response.responses)?;
        let Some(code) = NonZeroI16::new(result.error_code) else {
            outcomes.push(DeleteTopicOutcome::deleted(topic));
            continue;
        };
        #[allow(
            clippy::redundant_closure_for_method_calls,
            reason = "the generated string type is intentionally not a direct engine dependency"
        )]
        let message = result.error_message.as_ref().map(|value| value.as_str());
        let (message, truncated) = bounded_message(message, RESULT_DIAGNOSTIC_BYTES_PER_TOPIC);
        outcomes.push(DeleteTopicOutcome::failed(
            topic,
            DeleteTopicBrokerError::with_bounded_message(code, message, truncated),
        ));
    }
    Ok(outcomes)
}
