//! Response-correlation, normalized-result, and projected-terminal accounting.

use core::mem::size_of;

use kafka_wire::AlterClientQuotasResponse;

use super::{
    AlterClientQuotasRequestRef, NormalizedAlterClientQuotaEntityComponent,
    NormalizedAlterClientQuotaOutcome, NormalizedAlterClientQuotasResponse,
    model::CanonicalEntityComponentRef,
    response_validation::CanonicalResponseEntryRef,
    retention::{
        MAX_DIAGNOSTIC_BYTES, caller_reference_scratch_charge, generated_request_retained_charge,
        request_canonical_scratch_charge, request_peak_charge,
    },
};

pub(super) fn response_peak_charge(
    request: AlterClientQuotasRequestRef<'_>,
    response: &AlterClientQuotasResponse,
) -> Option<usize> {
    let request_peak = request_peak_charge(request)?;
    let request_retained = generated_request_retained_charge(request)?;
    let response_working = response_working_peak_charge(request, response)?;
    Some(request_peak.max(request_retained.checked_add(response_working)?))
}

fn response_working_peak_charge(
    request: AlterClientQuotasRequestRef<'_>,
    response: &AlterClientQuotasResponse,
) -> Option<usize> {
    let scratch = response_scratch_charge(request, response)?;
    let normalized = normalized_output_charge(response)?;
    let projected_core_and_public = normalized.checked_mul(2)?;
    scratch
        .checked_add(normalized)?
        .checked_add(projected_core_and_public)
}

fn response_scratch_charge(
    request: AlterClientQuotasRequestRef<'_>,
    response: &AlterClientQuotasResponse,
) -> Option<usize> {
    let request_scratch = request_canonical_scratch_charge(request)?
        .checked_add(caller_reference_scratch_charge(request)?)?;
    response.entries.iter().try_fold(
        request_scratch
            .checked_add(size_of::<Vec<CanonicalResponseEntryRef<'static>>>())?
            .checked_add(
                response
                    .entries
                    .len()
                    .checked_mul(size_of::<CanonicalResponseEntryRef<'static>>())?,
            )?
            .checked_add(
                request
                    .alterations()
                    .len()
                    .checked_mul(size_of::<usize>())?,
            )?
            .checked_add(
                request
                    .alterations()
                    .len()
                    .checked_mul(size_of::<&'static ()>())?,
            )?,
        |bytes, entry| {
            bytes.checked_add(
                entry
                    .entity
                    .len()
                    .checked_mul(size_of::<CanonicalEntityComponentRef<'static>>())?,
            )
        },
    )
}

fn normalized_output_charge(response: &AlterClientQuotasResponse) -> Option<usize> {
    response.entries.iter().try_fold(
        size_of::<NormalizedAlterClientQuotasResponse>().checked_add(
            response
                .entries
                .len()
                .checked_mul(size_of::<NormalizedAlterClientQuotaOutcome>())?,
        )?,
        |bytes, entry| {
            let bytes = bytes
                .checked_add(
                    entry
                        .entity
                        .len()
                        .checked_mul(size_of::<NormalizedAlterClientQuotaEntityComponent>())?,
                )?
                .checked_add(bounded_diagnostic_len(entry.error_message.as_deref()))?;
            entry.entity.iter().try_fold(bytes, |bytes, component| {
                bytes
                    .checked_add(component.entity_type.len())?
                    .checked_add(component.entity_name.as_ref().map_or(0, |name| name.len()))
            })
        },
    )
}

pub(super) fn normalized_retained_charge(
    response: &NormalizedAlterClientQuotasResponse,
) -> Option<usize> {
    response.outcomes.iter().try_fold(
        size_of::<NormalizedAlterClientQuotasResponse>().checked_add(
            response
                .outcomes
                .capacity()
                .checked_mul(size_of::<NormalizedAlterClientQuotaOutcome>())?,
        )?,
        |bytes, outcome| {
            let bytes = bytes
                .checked_add(
                    outcome
                        .entity
                        .capacity()
                        .checked_mul(size_of::<NormalizedAlterClientQuotaEntityComponent>())?,
                )?
                .checked_add(outcome.error_message.as_ref().map_or(0, String::capacity))?;
            outcome.entity.iter().try_fold(bytes, |bytes, component| {
                bytes
                    .checked_add(component.entity_type.capacity())?
                    .checked_add(component.entity_name.as_ref().map_or(0, String::capacity))
            })
        },
    )
}

pub(super) fn bounded_diagnostic_len(message: Option<&str>) -> usize {
    let Some(message) = message else {
        return 0;
    };
    floor_char_boundary(message, MAX_DIAGNOSTIC_BYTES.min(message.len()))
}

fn floor_char_boundary(value: &str, mut index: usize) -> usize {
    while !value.is_char_boundary(index) {
        index = index.saturating_sub(1);
    }
    index
}
