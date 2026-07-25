//! Classification scenarios for driver-owned Fetch terminals.

use kafka_client_core::FetchFailure;
use kafka_driver::{
    ApiKey, ApiVersion, CallFailure, ConnectionCloseReason, Delivery, NegotiationFailure,
    RequestError, ResponseCloseReason,
};
use kafka_wire_core::{DecodeError, EncodeError};

use super::failure::classify_fetch_request_error;

#[test]
fn exact_call_deadline_is_distinct_from_connection_loss() {
    assert_eq!(
        classify(&RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            delivery: Delivery::NotSent,
        }),
        FetchFailure::DeadlineElapsed
    );
    assert_eq!(
        classify(&RequestError::Rejected {
            failure: CallFailure::ConnectionClosed {
                reason: ConnectionCloseReason::DeadlineExceeded {
                    call_id: kafka_driver::CallId::from_raw(7),
                },
            },
            delivery: Delivery::NotSent,
        }),
        FetchFailure::Transport
    );
}

#[test]
fn driver_local_capacity_and_identity_failures_are_rejections() {
    let failures = [
        RequestError::ResponseCapacityReached { limit: 1 },
        RequestError::IdentityConflict,
        RequestError::DeadlineOverflow,
        RequestError::RouteCapacityReached {
            call_limit: 1,
            byte_limit: 2,
        },
        RequestError::MetadataQueryCapacityReached { limit: 1 },
        RequestError::CoordinatorCapacityReached { limit: 1 },
        RequestError::NameResolutionCapacityReached { limit: 1 },
    ];
    for failure in failures {
        assert_eq!(classify(&failure), FetchFailure::DriverRejected);
    }
    for failure in [
        CallFailure::CapacityReached { limit: 1 },
        CallFailure::CorrelationSpaceExhausted,
        CallFailure::LocallyRejected,
    ] {
        assert_eq!(
            classify(&RequestError::Rejected {
                failure,
                delivery: Delivery::NotSent,
            }),
            FetchFailure::DriverRejected
        );
    }
}

#[test]
fn routing_and_connection_availability_failures_are_transport() {
    assert_eq!(
        classify(&RequestError::RouteUnavailable),
        FetchFailure::Transport
    );
    for failure in [
        CallFailure::NotReady,
        CallFailure::Draining,
        CallFailure::Closed,
    ] {
        assert_eq!(
            classify(&RequestError::Rejected {
                failure,
                delivery: Delivery::NotSent,
            }),
            FetchFailure::Transport
        );
    }
    for reason in [
        ResponseCloseReason::TransportClosed,
        ResponseCloseReason::Shutdown,
    ] {
        assert_eq!(
            classify(&RequestError::ConnectionClosed(reason)),
            FetchFailure::Transport
        );
    }
}

#[test]
fn version_and_api_failures_are_compatibility() {
    let api_key = ApiKey::new(1);
    let failures = [
        RequestError::Encode(EncodeError::FieldNotRepresentable {
            message: "FetchRequest",
            field: "isolation_level",
            version: ApiVersion::new(3),
        }),
        RequestError::UnsupportedVersion {
            message: "FetchRequest",
            version: ApiVersion::new(3),
        },
        RequestError::ApiUnavailable { api_key },
        RequestError::VersionLimitUnavailable {
            api_key,
            maximum: ApiVersion::new(12),
            negotiated_minimum: ApiVersion::new(13),
        },
        RequestError::VersionFloorUnavailable {
            api_key,
            minimum: ApiVersion::new(4),
            negotiated_maximum: ApiVersion::new(3),
        },
        RequestError::VersionBoundsInvalid {
            api_key,
            minimum: ApiVersion::new(4),
            maximum: ApiVersion::new(3),
        },
    ];
    for failure in failures {
        assert_eq!(classify(&failure), FetchFailure::Compatibility);
    }
}

#[test]
fn protocol_faults_and_correlation_mismatches_are_invalid_responses() {
    assert_eq!(
        classify(&RequestError::ConnectionClosed(
            ResponseCloseReason::ProtocolFault
        )),
        FetchFailure::InvalidResponse
    );
    assert_eq!(
        classify(&RequestError::Decode(DecodeError::UnexpectedEnd {
            offset: 0,
            needed: 1,
            remaining: 0,
        })),
        FetchFailure::InvalidResponse
    );
    assert_eq!(
        classify(&RequestError::Rejected {
            failure: CallFailure::ConnectionClosed {
                reason: ConnectionCloseReason::NegotiationFailed(NegotiationFailure::Malformed),
            },
            delivery: Delivery::NotSent,
        }),
        FetchFailure::InvalidResponse
    );
}

#[test]
fn negotiation_capacity_is_an_over_budget_response() {
    assert_eq!(
        classify(&RequestError::Rejected {
            failure: CallFailure::ConnectionClosed {
                reason: ConnectionCloseReason::NegotiationFailed(NegotiationFailure::Capacity),
            },
            delivery: Delivery::NotSent,
        }),
        FetchFailure::ResponseTooLarge
    );
}

fn classify(failure: &RequestError) -> FetchFailure {
    classify_fetch_request_error(failure)
}
