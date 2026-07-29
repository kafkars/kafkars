//! Conservative terminal reservation for ordered resources and bounded diagnostics.

use kafka_client_core::LegacyAlterConfigsPlan;

use crate::admin::retention::{RESULT_DIAGNOSTIC_BYTES_PER_TOPIC, result_fixed_charge};

use super::response::LegacyAlterConfigsProtocolFailure;

pub(super) fn ensure_result_fits(
    plan: &LegacyAlterConfigsPlan,
    retained_bytes: usize,
) -> Result<(), LegacyAlterConfigsProtocolFailure> {
    if required_result_reservation(plan)? > retained_bytes {
        return Err(LegacyAlterConfigsProtocolFailure::RetainedBytes);
    }
    Ok(())
}

pub(super) fn required_result_reservation(
    plan: &LegacyAlterConfigsPlan,
) -> Result<usize, LegacyAlterConfigsProtocolFailure> {
    let resource_name_bytes = plan.resources().iter().try_fold(0usize, |bytes, resource| {
        bytes.checked_add(resource.resource_name().len())
    });
    let fixed = result_fixed_charge(
        plan.resources().len(),
        resource_name_bytes.ok_or(LegacyAlterConfigsProtocolFailure::RetainedBytes)?,
    )
    .ok_or(LegacyAlterConfigsProtocolFailure::RetainedBytes)?;
    let diagnostics = plan
        .resources()
        .len()
        .checked_mul(RESULT_DIAGNOSTIC_BYTES_PER_TOPIC)
        .ok_or(LegacyAlterConfigsProtocolFailure::RetainedBytes)?;
    fixed
        .checked_add(diagnostics)
        .ok_or(LegacyAlterConfigsProtocolFailure::RetainedBytes)
}
