//! Bounded normalization of one exact-broker `ListGroups` response.

use core::num::NonZeroI16;

use kafka_client_core::{
    AdminConsumerGroupListing, AdminListConsumerGroupsBrokerError,
    AdminListConsumerGroupsBrokerOutcome,
};
use kafka_wire::ListGroupsResponse;

const MIN_VERSION: i16 = 0;
const MAX_VERSION: i16 = 5;
const MAX_GROUPS_PER_BROKER: usize = 16 * 1024;
const MAX_GROUP_ID_BYTES: usize = i16::MAX as usize;
const MAX_SCALAR_BYTES: usize = 4096;
const BASE_RESULT_BYTES: usize = 4096;
const GROUP_OWNER_BYTES: usize = 192;
const TEXT_COPIES: usize = 2;

/// Structural, compatibility, or retained-capacity rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ListConsumerGroupsProtocolFailure {
    Compatibility,
    ResponseTooLarge,
    InvalidResponse,
}

/// One correlated response safe for deterministic core policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedListConsumerGroupsResponse {
    throttle_time_ms: u32,
    outcome: AdminListConsumerGroupsBrokerOutcome,
    retained_bytes: usize,
}

impl NormalizedListConsumerGroupsResponse {
    pub(crate) fn into_parts(self) -> (u32, AdminListConsumerGroupsBrokerOutcome, usize) {
        (self.throttle_time_ms, self.outcome, self.retained_bytes)
    }
}

/// Validates version-dependent fields and copies only bounded stable values.
pub(crate) fn normalize_list_consumer_groups_response(
    broker_id: i32,
    selected_version: Option<i16>,
    response: &ListGroupsResponse,
    retained_bytes: usize,
) -> Result<NormalizedListConsumerGroupsResponse, ListConsumerGroupsProtocolFailure> {
    if broker_id < 0 {
        return Err(ListConsumerGroupsProtocolFailure::InvalidResponse);
    }
    let version = selected_version.ok_or(ListConsumerGroupsProtocolFailure::Compatibility)?;
    if !(MIN_VERSION..=MAX_VERSION).contains(&version) {
        return Err(ListConsumerGroupsProtocolFailure::Compatibility);
    }
    let throttle_time_ms = if version == 0 {
        0
    } else {
        u32::try_from(response.throttle_time_ms)
            .map_err(|_| ListConsumerGroupsProtocolFailure::InvalidResponse)?
    };
    if let Some(code) = NonZeroI16::new(response.error_code) {
        return Ok(NormalizedListConsumerGroupsResponse {
            throttle_time_ms,
            outcome: AdminListConsumerGroupsBrokerOutcome::Rejected(
                AdminListConsumerGroupsBrokerError::new(broker_id, code),
            ),
            retained_bytes: 0,
        });
    }
    if response.groups.len() > MAX_GROUPS_PER_BROKER {
        return Err(ListConsumerGroupsProtocolFailure::ResponseTooLarge);
    }
    let charge = response_charge(response, version)?;
    if charge > retained_bytes {
        return Err(ListConsumerGroupsProtocolFailure::ResponseTooLarge);
    }
    let groups = response
        .groups
        .iter()
        .map(|group| {
            if group.group_id.is_empty() || group.group_id.len() > MAX_GROUP_ID_BYTES {
                return Err(ListConsumerGroupsProtocolFailure::InvalidResponse);
            }
            if group.protocol_type.len() > MAX_SCALAR_BYTES
                || (version >= 4 && group.group_state.len() > MAX_SCALAR_BYTES)
                || (version >= 5 && group.group_type.len() > MAX_SCALAR_BYTES)
            {
                return Err(ListConsumerGroupsProtocolFailure::ResponseTooLarge);
            }
            Ok(AdminConsumerGroupListing::new(
                canonical_string(group.group_id.as_str()),
                canonical_string(group.protocol_type.as_str()),
                (version >= 4).then(|| canonical_string(group.group_state.as_str())),
                (version >= 5).then(|| canonical_string(group.group_type.as_str())),
            ))
        })
        .collect::<Result<Vec<_>, ListConsumerGroupsProtocolFailure>>()?;
    Ok(NormalizedListConsumerGroupsResponse {
        throttle_time_ms,
        outcome: AdminListConsumerGroupsBrokerOutcome::Groups { broker_id, groups },
        retained_bytes: charge,
    })
}

fn response_charge(
    response: &ListGroupsResponse,
    version: i16,
) -> Result<usize, ListConsumerGroupsProtocolFailure> {
    let text_bytes = response.groups.iter().try_fold(0usize, |bytes, group| {
        bytes
            .checked_add(group.group_id.len())
            .and_then(|value| value.checked_add(group.protocol_type.len()))
            .and_then(|value| {
                if version >= 4 {
                    value.checked_add(group.group_state.len())
                } else {
                    Some(value)
                }
            })
            .and_then(|value| {
                if version >= 5 {
                    value.checked_add(group.group_type.len())
                } else {
                    Some(value)
                }
            })
            .ok_or(ListConsumerGroupsProtocolFailure::ResponseTooLarge)
    })?;
    BASE_RESULT_BYTES
        .checked_add(
            response
                .groups
                .len()
                .checked_mul(GROUP_OWNER_BYTES)
                .ok_or(ListConsumerGroupsProtocolFailure::ResponseTooLarge)?,
        )
        .and_then(|bytes| {
            text_bytes
                .checked_mul(TEXT_COPIES)
                .and_then(|text| bytes.checked_add(text))
        })
        .ok_or(ListConsumerGroupsProtocolFailure::ResponseTooLarge)
}

fn canonical_string(value: &str) -> String {
    value.to_owned().into_boxed_str().into_string()
}
