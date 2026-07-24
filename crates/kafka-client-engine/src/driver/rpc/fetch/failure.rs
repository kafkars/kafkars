//! Closed translation from Fetch driver failures into deterministic core facts.

use kafka_client_core::FetchFailure;
use kafka_driver::{
    CallFailure, ConnectionCloseReason, NegotiationFailure, RequestError, ResponseCloseReason,
};

use crate::protocol::fetch::FetchRequestFailure;

use super::admission::FetchAdmissionFailureSource;

use super::failure_wire::{classify_wire_decode_error, classify_wire_encode_error};

/// Classifies one definitely-unsent tracked-call admission rejection.
pub(crate) fn classify_fetch_admission(failure: &FetchAdmissionFailureSource) -> FetchFailure {
    match failure {
        FetchAdmissionFailureSource::DeadlineElapsed => FetchFailure::DeadlineElapsed,
        FetchAdmissionFailureSource::Request(failure) => classify_request_failure(failure),
        FetchAdmissionFailureSource::Driver(_failure) => FetchFailure::DriverRejected,
    }
}

/// Classifies one driver-owned terminal without exposing driver vocabulary to core.
pub(crate) fn classify_fetch_request_error(failure: &RequestError) -> FetchFailure {
    match failure {
        RequestError::Encode(failure) => classify_wire_encode_error(failure),
        RequestError::Decode(failure) => classify_wire_decode_error(failure),
        RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. } => FetchFailure::Compatibility,
        RequestError::ResponseCapacityReached { .. }
        | RequestError::IdentityConflict
        | RequestError::DeadlineOverflow
        | RequestError::RouteCapacityReached { .. }
        | RequestError::MetadataQueryCapacityReached { .. }
        | RequestError::CoordinatorCapacityReached { .. }
        | RequestError::NameResolutionCapacityReached { .. } => FetchFailure::DriverRejected,
        RequestError::RouteUnavailable | RequestError::NameResolutionFailed { .. } => {
            FetchFailure::Transport
        }
        RequestError::Rejected { failure, .. } => classify_call_failure(*failure),
        RequestError::ConnectionClosed(reason) => classify_response_close(*reason),
    }
}

fn classify_request_failure(failure: &FetchRequestFailure) -> FetchFailure {
    match failure {
        FetchRequestFailure::EmptyTopic
        | FetchRequestFailure::TopicTooLong { .. }
        | FetchRequestFailure::PartitionOutOfRange { .. }
        | FetchRequestFailure::NegativeFetchOffset { .. }
        | FetchRequestFailure::MaxWaitOutOfRange { .. }
        | FetchRequestFailure::MinBytesOutOfRange { .. }
        | FetchRequestFailure::MaxBytesOutOfRange { .. }
        | FetchRequestFailure::PartitionMaxBytesOutOfRange { .. }
        | FetchRequestFailure::MinBytesExceedMaxBytes { .. }
        | FetchRequestFailure::InvalidIsolationLevel { .. } => FetchFailure::DriverRejected,
    }
}

const fn classify_call_failure(failure: CallFailure) -> FetchFailure {
    match failure {
        CallFailure::DeadlineExceeded => FetchFailure::DeadlineElapsed,
        CallFailure::CapacityReached { .. }
        | CallFailure::CorrelationSpaceExhausted
        | CallFailure::LocallyRejected => FetchFailure::DriverRejected,
        CallFailure::CorrelationMismatch { .. } => FetchFailure::InvalidResponse,
        CallFailure::ConnectionClosed { reason } => classify_connection_close(reason),
        CallFailure::NotReady | CallFailure::Draining | CallFailure::Closed => {
            FetchFailure::Transport
        }
    }
}

const fn classify_connection_close(reason: ConnectionCloseReason) -> FetchFailure {
    match reason {
        ConnectionCloseReason::NegotiationFailed(NegotiationFailure::Malformed)
        | ConnectionCloseReason::CorrelationMismatch { .. }
        | ConnectionCloseReason::UnexpectedResponse
        | ConnectionCloseReason::MalformedResponse => FetchFailure::InvalidResponse,
        ConnectionCloseReason::NegotiationFailed(NegotiationFailure::Capacity) => {
            FetchFailure::ResponseTooLarge
        }
        ConnectionCloseReason::Drained
        | ConnectionCloseReason::Requested
        | ConnectionCloseReason::OpenFailed(_)
        | ConnectionCloseReason::TransportLost(_)
        | ConnectionCloseReason::NegotiationFailed(
            NegotiationFailure::Broker | NegotiationFailure::Timeout,
        )
        | ConnectionCloseReason::AuthenticationFailed(_)
        | ConnectionCloseReason::DeadlineExceeded { .. } => FetchFailure::Transport,
    }
}

const fn classify_response_close(reason: ResponseCloseReason) -> FetchFailure {
    match reason {
        ResponseCloseReason::ProtocolFault => FetchFailure::InvalidResponse,
        ResponseCloseReason::TransportClosed | ResponseCloseReason::Shutdown => {
            FetchFailure::Transport
        }
    }
}
