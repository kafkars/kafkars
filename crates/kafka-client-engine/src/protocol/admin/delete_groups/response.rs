//! Strict singleton Admin `DeleteConsumerGroups` response correlation.

use core::num::NonZeroI16;

use kafka_client_core::{
    DELETE_CONSUMER_GROUPS_DIAGNOSTIC_BYTES, DeleteConsumerGroupsBrokerError,
    DeleteConsumerGroupsOutcome, DeleteConsumerGroupsTarget,
};
use kafka_wire::{DeleteGroupsResponse, delete_groups_response::DeletableGroupResult};

use super::NormalizedDeleteConsumerGroupsResponse;

/// Structural or scalar response facts unsafe to bind to the current group.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeleteConsumerGroupsResponseFailure {
    /// The selected version lies outside the supported range.
    UnsupportedApiVersion {
        /// Exact selected Kafka API version.
        actual: i16,
    },
    /// Kafka supplied a negative throttle duration.
    NegativeThrottleTime {
        /// Exact invalid duration.
        actual: i32,
    },
    /// The requested group result was absent.
    MissingGroup,
    /// The requested group result appeared more than once.
    DuplicateGroup,
    /// A result named a different group.
    UnexpectedGroup,
    /// The normalized result exceeded its admitted retained capacity.
    RetainedBytes,
}

/// Correlates one generated response before exposing owned scalar facts.
pub(crate) fn normalize_delete_consumer_groups_response(
    target: &DeleteConsumerGroupsTarget,
    selected_version: i16,
    response: &DeleteGroupsResponse,
    retained_limit: usize,
) -> Result<NormalizedDeleteConsumerGroupsResponse, DeleteConsumerGroupsResponseFailure> {
    if !(0..=3).contains(&selected_version) {
        return Err(DeleteConsumerGroupsResponseFailure::UnsupportedApiVersion {
            actual: selected_version,
        });
    }
    let throttle_time_ms = u32::try_from(response.throttle_time_ms).map_err(|_| {
        DeleteConsumerGroupsResponseFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        }
    })?;
    let group = matching_group(target.group_id(), &response.results)?;
    let diagnostic_len = bounded_diagnostic_len(group.error_message.as_deref());
    let retained_bytes = target
        .group_id()
        .len()
        .checked_add(diagnostic_len)
        .ok_or(DeleteConsumerGroupsResponseFailure::RetainedBytes)?;
    if retained_bytes > retained_limit {
        return Err(DeleteConsumerGroupsResponseFailure::RetainedBytes);
    }
    let group_id = copy_string(target.group_id())?;
    let (outcome, retained_bytes) = if let Some(code) = NonZeroI16::new(group.error_code) {
        let (message, message_truncated) = bounded_message(group.error_message.as_deref())?;
        let retained_bytes = group_id
            .capacity()
            .checked_add(message.as_ref().map_or(0, String::capacity))
            .ok_or(DeleteConsumerGroupsResponseFailure::RetainedBytes)?;
        (
            DeleteConsumerGroupsOutcome::failed(
                group_id,
                DeleteConsumerGroupsBrokerError::with_bounded_message(
                    code,
                    message,
                    message_truncated,
                ),
            ),
            retained_bytes,
        )
    } else {
        let retained_bytes = group_id.capacity();
        (
            DeleteConsumerGroupsOutcome::deleted(group_id),
            retained_bytes,
        )
    };
    if retained_bytes > retained_limit {
        return Err(DeleteConsumerGroupsResponseFailure::RetainedBytes);
    }
    Ok(NormalizedDeleteConsumerGroupsResponse::new(
        throttle_time_ms,
        outcome,
        retained_bytes,
    ))
}

fn copy_string(source: &str) -> Result<String, DeleteConsumerGroupsResponseFailure> {
    let mut owned = String::new();
    owned
        .try_reserve_exact(source.len())
        .map_err(|_| DeleteConsumerGroupsResponseFailure::RetainedBytes)?;
    owned.push_str(source);
    Ok(owned)
}

fn bounded_message(
    message: Option<&str>,
) -> Result<(Option<String>, bool), DeleteConsumerGroupsResponseFailure> {
    let Some(message) = message else {
        return Ok((None, false));
    };
    let retained = bounded_diagnostic_len(Some(message));
    Ok((
        Some(copy_string(&message[..retained])?),
        retained < message.len(),
    ))
}

fn bounded_diagnostic_len(message: Option<&str>) -> usize {
    let Some(message) = message else {
        return 0;
    };
    let mut retained = DELETE_CONSUMER_GROUPS_DIAGNOSTIC_BYTES.min(message.len());
    while !message.is_char_boundary(retained) {
        retained = retained.saturating_sub(1);
    }
    retained
}

fn matching_group<'a>(
    expected: &str,
    groups: &'a [DeletableGroupResult],
) -> Result<&'a DeletableGroupResult, DeleteConsumerGroupsResponseFailure> {
    let mut matching = None;
    for group in groups {
        if group.group_id.as_str() != expected {
            return Err(DeleteConsumerGroupsResponseFailure::UnexpectedGroup);
        }
        if matching.replace(group).is_some() {
            return Err(DeleteConsumerGroupsResponseFailure::DuplicateGroup);
        }
    }
    matching.ok_or(DeleteConsumerGroupsResponseFailure::MissingGroup)
}
