//! Bounded validation for generated partition-reassignment responses.

mod correlation;
#[cfg(test)]
mod correlation_test;

use core::num::NonZeroI16;

use kafka_client_core::{AlterPartitionReassignmentBrokerError, AlterPartitionReassignmentsBatch};
use kafka_wire::AlterPartitionReassignmentsResponse;
use kafka_wire_core::StrBytes;

use super::{
    AlterPartitionReassignmentRef, ValidatedAlterPartitionReassignmentsResponse,
    retention::result_charge, version::validate_selected_version,
};

const DIAGNOSTIC_LIMIT: usize = 1024;

/// Generated response facts unsafe to bind to the requested change set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AlterPartitionReassignmentsProtocolFailure {
    UnsupportedApiVersion {
        minimum: i16,
        maximum: i16,
        actual: i16,
    },
    NegativeThrottleTime,
    ReplicationFactorPolicyMismatch,
    TopicCount,
    PartitionCount,
    UnexpectedTopic,
    MissingTopic,
    DuplicateTopic,
    UnexpectedPartition,
    MissingPartition,
    DuplicatePartition,
    NegativePartition,
    RetainedBytes,
}

/// Validates the selected version and generated response before owned copying.
pub(crate) fn validate_alter_partition_reassignments_response(
    changes: &[AlterPartitionReassignmentRef<'_>],
    allow_replication_factor_change: bool,
    response: &AlterPartitionReassignmentsResponse,
    selected_version: i16,
    result_limit: usize,
) -> Result<ValidatedAlterPartitionReassignmentsResponse, AlterPartitionReassignmentsProtocolFailure>
{
    validate_selected_version(selected_version, allow_replication_factor_change).map_err(
        |failure| AlterPartitionReassignmentsProtocolFailure::UnsupportedApiVersion {
            minimum: failure.minimum,
            maximum: failure.maximum,
            actual: failure.actual,
        },
    )?;
    let throttle_time_ms = u32::try_from(response.throttle_time_ms)
        .map_err(|_| AlterPartitionReassignmentsProtocolFailure::NegativeThrottleTime)?;
    if selected_version >= 1
        && response.allow_replication_factor_change != allow_replication_factor_change
    {
        return Err(AlterPartitionReassignmentsProtocolFailure::ReplicationFactorPolicyMismatch);
    }
    if let Some(code) = NonZeroI16::new(response.error_code) {
        let error = bounded_error(code, response.error_message.as_ref().map(StrBytes::as_str));
        ensure_top_level_limit(&error, result_limit)?;
        return Ok(ValidatedAlterPartitionReassignmentsResponse::BrokerRejected(error));
    }
    let returned = correlation::correlate_shape(changes, response)?;
    let diagnostic_bytes = returned
        .iter()
        .try_fold(0usize, |bytes, partition| {
            bytes.checked_add(bounded_len(partition.error_message()))
        })
        .ok_or(AlterPartitionReassignmentsProtocolFailure::RetainedBytes)?;
    let charge = result_charge(changes.iter().copied(), diagnostic_bytes)
        .ok_or(AlterPartitionReassignmentsProtocolFailure::RetainedBytes)?;
    if charge > result_limit {
        return Err(AlterPartitionReassignmentsProtocolFailure::RetainedBytes);
    }
    let outcomes = correlation::normalize_in_caller_order(changes, &returned, bounded_error)?;
    Ok(ValidatedAlterPartitionReassignmentsResponse::Batch(
        AlterPartitionReassignmentsBatch::new(throttle_time_ms, outcomes),
    ))
}

fn ensure_top_level_limit(
    error: &AlterPartitionReassignmentBrokerError,
    result_limit: usize,
) -> Result<(), AlterPartitionReassignmentsProtocolFailure> {
    let required = core::mem::size_of::<AlterPartitionReassignmentBrokerError>()
        .checked_add(error.message().map_or(0, str::len))
        .ok_or(AlterPartitionReassignmentsProtocolFailure::RetainedBytes)?;
    (required <= result_limit)
        .then_some(())
        .ok_or(AlterPartitionReassignmentsProtocolFailure::RetainedBytes)
}

fn bounded_error(code: NonZeroI16, message: Option<&str>) -> AlterPartitionReassignmentBrokerError {
    let (message, truncated) = match message {
        None => (None, false),
        Some(message) if message.len() <= DIAGNOSTIC_LIMIT => (Some(message.to_owned()), false),
        Some(message) => {
            let mut boundary = DIAGNOSTIC_LIMIT.min(message.len());
            while boundary > 0 && !message.is_char_boundary(boundary) {
                boundary -= 1;
            }
            (Some(message[..boundary].to_owned()), true)
        }
    };
    AlterPartitionReassignmentBrokerError::with_bounded_message(code, message, truncated)
}

fn bounded_len(message: Option<&str>) -> usize {
    message.map_or(0, |message| message.len().min(DIAGNOSTIC_LIMIT))
}
