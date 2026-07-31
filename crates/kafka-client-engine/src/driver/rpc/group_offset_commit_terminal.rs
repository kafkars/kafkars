//! Authoritative terminal normalization for classic-group `OffsetCommit`.

use kafka_client_core::{DeliveryStatus, GroupOffsetCommitInput};
use kafka_driver::{ApiVersion, CallFailure, RequestError};
use kafka_wire::OffsetCommitResponse;

use crate::protocol::consumer::{
    GroupOffsetCommitProtocolFailure, PreparedGroupOffsetCommit,
    normalize_group_offset_commit_response,
};

pub(super) fn normalize_group_offset_commit_terminal(
    prepared: PreparedGroupOffsetCommit,
    selected_version: Option<ApiVersion>,
    result: Result<OffsetCommitResponse, RequestError>,
) -> GroupOffsetCommitInput {
    let response = match result {
        Ok(response) => response,
        Err(
            error @ RequestError::Rejected {
                failure: CallFailure::DeadlineExceeded,
                ..
            },
        ) => {
            return GroupOffsetCommitInput::DeadlineElapsed {
                delivery: super::super::request_failure_delivery(&error),
            };
        }
        Err(
            RequestError::Rejected {
                failure: CallFailure::CorrelationMismatch { .. },
                ..
            }
            | RequestError::Decode(_),
        ) => return GroupOffsetCommitInput::InvalidResponse,
        Err(error) if is_compatibility_failure(&error) => {
            return GroupOffsetCommitInput::ProtocolIncompatible {
                delivery: super::super::request_failure_delivery(&error),
            };
        }
        Err(error) => {
            return GroupOffsetCommitInput::TransportFailed {
                delivery: super::super::request_failure_delivery(&error),
            };
        }
    };
    let Some(version) = selected_version.map(ApiVersion::value) else {
        return GroupOffsetCommitInput::InvalidResponse;
    };
    if !(2..=9).contains(&version)
        || prepared.requires_leader_epoch() && version < 6
        || prepared.requires_consumer_group_version() && version < 9
    {
        return GroupOffsetCommitInput::ProtocolIncompatible {
            delivery: DeliveryStatus::PossiblySent,
        };
    }
    match normalize_group_offset_commit_response(prepared, &response) {
        Ok((throttle_time_ms, outcomes)) => GroupOffsetCommitInput::BrokerResponded {
            throttle_time_ms,
            outcomes,
        },
        Err(
            GroupOffsetCommitProtocolFailure::ThrottleTime
            | GroupOffsetCommitProtocolFailure::TopicCount
            | GroupOffsetCommitProtocolFailure::ResultCount
            | GroupOffsetCommitProtocolFailure::UnexpectedTopic
            | GroupOffsetCommitProtocolFailure::DuplicateTopic
            | GroupOffsetCommitProtocolFailure::MissingTopic
            | GroupOffsetCommitProtocolFailure::UnexpectedPartition
            | GroupOffsetCommitProtocolFailure::DuplicatePartition
            | GroupOffsetCommitProtocolFailure::MissingPartition,
        ) => GroupOffsetCommitInput::InvalidResponse,
    }
}

const fn is_compatibility_failure(error: &RequestError) -> bool {
    matches!(
        error,
        RequestError::Encode(_)
            | RequestError::UnsupportedVersion { .. }
            | RequestError::ApiUnavailable { .. }
            | RequestError::VersionLimitUnavailable { .. }
            | RequestError::VersionFloorUnavailable { .. }
            | RequestError::VersionBoundsInvalid { .. }
    )
}
