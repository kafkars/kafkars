//! Conservative terminal reservation for ordered topics and bounded diagnostics.

use kafka_client_core::IncrementalAlterConfigsPlan;

use crate::admin::retention::{RESULT_DIAGNOSTIC_BYTES_PER_TOPIC, result_fixed_charge};

use super::response::IncrementalAlterConfigsProtocolFailure;

pub(super) fn ensure_result_fits(
    plan: &IncrementalAlterConfigsPlan,
    retained_bytes: usize,
) -> Result<(), IncrementalAlterConfigsProtocolFailure> {
    if required_result_reservation(plan)? > retained_bytes {
        return Err(IncrementalAlterConfigsProtocolFailure::RetainedBytes);
    }
    Ok(())
}

pub(super) fn required_result_reservation(
    plan: &IncrementalAlterConfigsPlan,
) -> Result<usize, IncrementalAlterConfigsProtocolFailure> {
    let topic_bytes = plan.topics().iter().try_fold(0usize, |bytes, topic| {
        bytes.checked_add(topic.topic().len())
    });
    let fixed = result_fixed_charge(
        plan.topics().len(),
        topic_bytes.ok_or(IncrementalAlterConfigsProtocolFailure::RetainedBytes)?,
    )
    .ok_or(IncrementalAlterConfigsProtocolFailure::RetainedBytes)?;
    let diagnostics = plan
        .topics()
        .len()
        .checked_mul(RESULT_DIAGNOSTIC_BYTES_PER_TOPIC)
        .ok_or(IncrementalAlterConfigsProtocolFailure::RetainedBytes)?;
    fixed
        .checked_add(diagnostics)
        .ok_or(IncrementalAlterConfigsProtocolFailure::RetainedBytes)
}
