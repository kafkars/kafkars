//! Stable bounded retention of `CreatePartitions` broker diagnostics.

use core::num::NonZeroI16;

use kafka_client_core::{
    CreatePartitionsPlan, PartitionIncreaseBrokerError, PartitionIncreaseOutcome,
};
use kafka_wire::CreatePartitionsResponse;

use super::{
    create_partitions::{
        CreatePartitionsProtocolFailure, matching_result, validate_response_shape,
    },
    result_budget::bounded_message,
};
use crate::admin::retention::{RESULT_DIAGNOSTIC_BYTES_PER_TOPIC, result_fixed_charge};

pub(super) fn normalize(
    plan: &CreatePartitionsPlan,
    response: &CreatePartitionsResponse,
    retained_bytes: usize,
) -> Result<Vec<PartitionIncreaseOutcome>, CreatePartitionsProtocolFailure> {
    validate_response_shape(plan, response)?;
    let topic_bytes = plan
        .topics()
        .iter()
        .try_fold(0usize, |bytes, topic| {
            bytes.checked_add(topic.topic().len())
        })
        .ok_or(CreatePartitionsProtocolFailure::RetainedBytes)?;
    let fixed = result_fixed_charge(plan.topics().len(), topic_bytes)
        .ok_or(CreatePartitionsProtocolFailure::RetainedBytes)?;
    let required_diagnostics = plan
        .topics()
        .len()
        .checked_mul(RESULT_DIAGNOSTIC_BYTES_PER_TOPIC)
        .ok_or(CreatePartitionsProtocolFailure::RetainedBytes)?;
    let available = retained_bytes
        .checked_sub(fixed)
        .ok_or(CreatePartitionsProtocolFailure::RetainedBytes)?;
    if available < required_diagnostics {
        return Err(CreatePartitionsProtocolFailure::RetainedBytes);
    }
    let mut outcomes = Vec::with_capacity(plan.topics().len());
    for topic in plan.topics() {
        let result = matching_result(topic.topic(), &response.results)?;
        let Some(code) = NonZeroI16::new(result.error_code) else {
            outcomes.push(PartitionIncreaseOutcome::increased(topic.topic()));
            continue;
        };
        #[allow(
            clippy::redundant_closure_for_method_calls,
            reason = "the generated string type is intentionally not a direct engine dependency"
        )]
        let message = result.error_message.as_ref().map(|value| value.as_str());
        let (message, truncated) = bounded_message(message, RESULT_DIAGNOSTIC_BYTES_PER_TOPIC);
        outcomes.push(PartitionIncreaseOutcome::failed(
            topic.topic(),
            PartitionIncreaseBrokerError::with_bounded_message(code, message, truncated),
        ));
    }
    Ok(outcomes)
}
