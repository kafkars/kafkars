//! Validate-first bounded response ownership and deterministic input translation.

use core::mem::size_of;

use kafka_client_core::{DeleteAclFilterResult, DeleteAclsInput, DeliveryStatus};

use crate::{
    driver::{DeleteAclsDriverFailureKind, DeleteAclsRawTerminal, DeleteAclsTerminalFact},
    protocol::admin::delete_acls::{DeleteAclsResponseFailure, normalize_delete_acls_response},
};

use super::super::{DeleteAclMatchingBinding as StableMatchingBinding, DeleteAclsPreparedOutcomes};

pub(super) fn terminal_input(
    raw: &DeleteAclsRawTerminal,
    expected_filters: usize,
    prepared_results: Vec<DeleteAclFilterResult>,
    matching_counts: &mut Vec<usize>,
    prepared_outcomes: &mut DeleteAclsPreparedOutcomes,
    prepared_outcome_bytes: usize,
    prepared_core_result_bytes: usize,
    remaining_response_bytes: usize,
) -> (DeleteAclsInput, usize) {
    matching_counts.clear();
    match raw.fact() {
        DeleteAclsTerminalFact::Response {
            selected_version: Some(selected_version),
            response,
        } => {
            let response_limit =
                match remaining_response_bytes.checked_add(prepared_core_result_bytes) {
                    Some(limit) => limit,
                    None => return (DeleteAclsInput::ResponseTooLarge, 0),
                };
            let normalized = normalize_delete_acls_response(
                selected_version,
                expected_filters,
                response,
                response_limit,
                prepared_results,
                |_filter_index, count| {
                    let mut matching = Vec::new();
                    matching.try_reserve_exact(count).map_err(|_error| ())?;
                    Ok(matching)
                },
            );
            let normalized = match normalized {
                Ok(normalized) => normalized,
                Err(error) => return (protocol_failure(error), 0),
            };
            let (throttle_time_ms, results, retained_bytes) = normalized.into_parts();
            if record_positional_matching_capacities(&results, expected_filters, matching_counts)
                .is_err()
            {
                return (DeleteAclsInput::ResponseTooLarge, 0);
            }
            let protocol_dynamic = match retained_bytes.checked_sub(prepared_core_result_bytes) {
                Some(bytes) if bytes <= remaining_response_bytes => bytes,
                _ => return (DeleteAclsInput::ResponseTooLarge, 0),
            };
            let requested_stable_nested =
                match matching_counts.iter().try_fold(0usize, |bytes, count| {
                    bytes.checked_add(count.checked_mul(size_of::<StableMatchingBinding>())?)
                }) {
                    Some(bytes) => bytes,
                    None => return (DeleteAclsInput::ResponseTooLarge, 0),
                };
            if protocol_dynamic
                .checked_add(requested_stable_nested)
                .is_none_or(|bytes| bytes > remaining_response_bytes)
            {
                return (DeleteAclsInput::ResponseTooLarge, 0);
            }
            if prepared_outcomes
                .try_prepare_matching(matching_counts.iter().copied())
                .is_err()
            {
                return (DeleteAclsInput::ResponseTooLarge, 0);
            }
            let prepared_bytes = match prepared_outcomes.retained_heap_bytes() {
                Some(bytes) => bytes,
                None => return (DeleteAclsInput::ResponseTooLarge, 0),
            };
            let stable_dynamic = match prepared_bytes.checked_sub(prepared_outcome_bytes) {
                Some(bytes) => bytes,
                None => return (DeleteAclsInput::ResponseTooLarge, 0),
            };
            let total_dynamic = match protocol_dynamic.checked_add(stable_dynamic) {
                Some(bytes) if bytes <= remaining_response_bytes => bytes,
                _ => return (DeleteAclsInput::ResponseTooLarge, 0),
            };
            (
                DeleteAclsInput::BrokerResponded {
                    throttle_time_ms,
                    results,
                },
                total_dynamic,
            )
        }
        DeleteAclsTerminalFact::Response {
            selected_version: None,
            ..
        } => (
            DeleteAclsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            },
            0,
        ),
        DeleteAclsTerminalFact::Failed { kind, delivery } => (driver_failure(kind, delivery), 0),
    }
}

pub(super) fn record_positional_matching_capacities(
    filter_results: &[DeleteAclFilterResult],
    expected_filters: usize,
    matching_counts: &mut Vec<usize>,
) -> Result<(), ()> {
    matching_counts.clear();
    if filter_results.len() != expected_filters || matching_counts.capacity() < expected_filters {
        return Err(());
    }
    for result in filter_results {
        matching_counts.push(match result {
            DeleteAclFilterResult::Matched(matching) => matching.len(),
            DeleteAclFilterResult::BrokerFailed(_) => 0,
        });
    }
    Ok(())
}

pub(super) const fn protocol_failure(error: DeleteAclsResponseFailure) -> DeleteAclsInput {
    match error {
        DeleteAclsResponseFailure::UnsupportedApiVersion { .. } => {
            DeleteAclsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        }
        DeleteAclsResponseFailure::RetainedBytes { .. }
        | DeleteAclsResponseFailure::OuterResultStorage
        | DeleteAclsResponseFailure::MatchingResultStorage { .. }
        | DeleteAclsResponseFailure::OwnedValueStorage => DeleteAclsInput::ResponseTooLarge,
        DeleteAclsResponseFailure::EmptyExpectedFilters
        | DeleteAclsResponseFailure::TooManyExpectedFilters { .. }
        | DeleteAclsResponseFailure::NegativeThrottleTime { .. }
        | DeleteAclsResponseFailure::FilterResultCount { .. }
        | DeleteAclsResponseFailure::FilterErrorWithMatches { .. }
        | DeleteAclsResponseFailure::TooManyMatchesForFilter { .. }
        | DeleteAclsResponseFailure::TooManyMatchingAcls { .. }
        | DeleteAclsResponseFailure::InvalidResourceType { .. }
        | DeleteAclsResponseFailure::EmptyResourceName
        | DeleteAclsResponseFailure::ResourceNameTooLong { .. }
        | DeleteAclsResponseFailure::InvalidPatternType { .. }
        | DeleteAclsResponseFailure::EmptyPrincipal
        | DeleteAclsResponseFailure::PrincipalTooLong { .. }
        | DeleteAclsResponseFailure::EmptyHost
        | DeleteAclsResponseFailure::HostTooLong { .. }
        | DeleteAclsResponseFailure::InvalidOperation { .. }
        | DeleteAclsResponseFailure::InvalidPermissionType { .. }
        | DeleteAclsResponseFailure::DuplicateMatchingAcl { .. } => {
            DeleteAclsInput::InvalidResponse
        }
    }
}

const fn driver_failure(
    kind: DeleteAclsDriverFailureKind,
    delivery: DeliveryStatus,
) -> DeleteAclsInput {
    match kind {
        DeleteAclsDriverFailureKind::DeadlineElapsed => {
            DeleteAclsInput::DriverDeadlineElapsed { delivery }
        }
        DeleteAclsDriverFailureKind::Compatibility => {
            DeleteAclsInput::ProtocolIncompatible { delivery }
        }
        DeleteAclsDriverFailureKind::InvalidResponse => DeleteAclsInput::InvalidResponse,
        DeleteAclsDriverFailureKind::Transport => DeleteAclsInput::TransportFailed { delivery },
    }
}
