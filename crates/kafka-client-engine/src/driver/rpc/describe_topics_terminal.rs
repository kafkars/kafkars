//! Semantic terminal normalization for plain transient Metadata calls.

use kafka_client_core::{DeliveryStatus, DescribeTopicsInput, DescribeTopicsPlan};
use kafka_driver::{CallFailure, RequestError};
use kafka_wire::MetadataResponse;

use crate::protocol::admin::describe_topics_response::{
    DescribeTopicsProtocolFailure, normalize_describe_topics_response,
};

pub(super) fn normalize_terminal_at(
    plan: &DescribeTopicsPlan,
    retained_bytes: usize,
    deadline_elapsed: bool,
    result: Result<MetadataResponse, RequestError>,
) -> DescribeTopicsInput {
    if deadline_elapsed {
        let delivery = result.as_ref().err().map_or(
            DeliveryStatus::PossiblySent,
            super::super::request_failure_delivery,
        );
        return DescribeTopicsInput::DriverDeadlineElapsed { delivery };
    }
    let response = match result {
        Ok(response) => response,
        Err(
            error @ RequestError::Rejected {
                failure: CallFailure::DeadlineExceeded,
                ..
            },
        ) => {
            return DescribeTopicsInput::DriverDeadlineElapsed {
                delivery: super::super::request_failure_delivery(&error),
            };
        }
        // This adapter builds one internally bounded Metadata request. Its only
        // negotiated-version encode failure is the required false
        // AllowAutoTopicCreation field on Metadata versions below v4.
        Err(
            RequestError::Encode(_)
            | RequestError::VersionFloorUnavailable { .. }
            | RequestError::VersionBoundsInvalid { .. },
        ) => {
            return DescribeTopicsInput::ProtocolIncompatible;
        }
        Err(error) => {
            return DescribeTopicsInput::TransportFailed {
                delivery: super::super::request_failure_delivery(&error),
            };
        }
    };
    match normalize_describe_topics_response(plan, &response, retained_bytes) {
        Ok(input) => input,
        Err(DescribeTopicsProtocolFailure::RetainedBytes) => DescribeTopicsInput::ResponseTooLarge,
        Err(_invalid_response) => DescribeTopicsInput::InvalidResponse,
    }
}
