//! Semantic normalization for tracked `IncrementalAlterConfigs` calls.

use kafka_client_core::{
    DeliveryStatus, IncrementalAlterConfigsInput, IncrementalAlterConfigsPlan,
};
use kafka_driver::{ApiVersion, CallFailure, RequestError};
use kafka_wire::IncrementalAlterConfigsResponse;

use crate::protocol::admin::incremental_alter_configs::{
    IncrementalAlterConfigsProtocolFailure, normalize_incremental_alter_configs_response_bounded,
};

pub(super) fn normalize_terminal(
    plan: &IncrementalAlterConfigsPlan,
    result_limit: usize,
    selected_version: Option<ApiVersion>,
    result: Result<IncrementalAlterConfigsResponse, RequestError>,
) -> IncrementalAlterConfigsInput {
    let response = match result {
        Ok(response) => response,
        Err(
            error @ RequestError::Rejected {
                failure: CallFailure::DeadlineExceeded,
                ..
            },
        ) => {
            return IncrementalAlterConfigsInput::DriverDeadlineElapsed {
                delivery: super::super::request_failure_delivery(&error),
            };
        }
        Err(
            RequestError::Rejected {
                failure: CallFailure::CorrelationMismatch { .. },
                ..
            }
            | RequestError::Decode(_),
        ) => {
            return IncrementalAlterConfigsInput::InvalidResponse;
        }
        Err(error) if is_compatibility_failure(&error) => {
            return IncrementalAlterConfigsInput::ProtocolIncompatible {
                delivery: super::super::request_failure_delivery(&error),
            };
        }
        Err(error) => {
            return IncrementalAlterConfigsInput::TransportFailed {
                delivery: super::super::request_failure_delivery(&error),
            };
        }
    };
    let Some(version) = selected_version.map(ApiVersion::value) else {
        return IncrementalAlterConfigsInput::InvalidResponse;
    };
    if !(0..=1).contains(&version) {
        return IncrementalAlterConfigsInput::ProtocolIncompatible {
            delivery: DeliveryStatus::PossiblySent,
        };
    }
    match normalize_incremental_alter_configs_response_bounded(plan, &response, result_limit) {
        Ok(batch) => IncrementalAlterConfigsInput::BrokerResponded { batch },
        Err(IncrementalAlterConfigsProtocolFailure::RetainedBytes) => {
            IncrementalAlterConfigsInput::ResponseTooLarge
        }
        Err(
            IncrementalAlterConfigsProtocolFailure::ThrottleTime
            | IncrementalAlterConfigsProtocolFailure::ResourceCount
            | IncrementalAlterConfigsProtocolFailure::NonPositiveResourceType
            | IncrementalAlterConfigsProtocolFailure::UnexpectedResource
            | IncrementalAlterConfigsProtocolFailure::MissingResource
            | IncrementalAlterConfigsProtocolFailure::DuplicateResource,
        ) => IncrementalAlterConfigsInput::InvalidResponse,
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
