//! Version and failure normalization for tracked topic `IncrementalAlterConfigs`.

use kafka_client_core::{
    ConfigAlteration, DeliveryStatus, IncrementalAlterConfigResult, IncrementalAlterConfigsInput,
    IncrementalAlterConfigsPlan, TopicConfigAlteration,
};
use kafka_driver::{ApiKey, ApiVersion, CallFailure, Delivery, RequestError};
use kafka_wire::{
    IncrementalAlterConfigsResponse,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
};
use kafka_wire_core::{DecodeError, EncodeError};

use super::incremental_alter_configs_terminal::normalize_terminal;

const LARGE_BUDGET: usize = 1 << 20;

#[test]
fn selected_v0_and_v1_restore_ordered_results_and_throttle() {
    for version in [0, 1] {
        let IncrementalAlterConfigsInput::BrokerResponded { batch } = normalize_terminal(
            &plan(),
            LARGE_BUDGET,
            Some(ApiVersion::new(version)),
            Ok(response(17, "orders")),
        ) else {
            panic!("bounded v{version} response expected");
        };
        assert_eq!(batch.throttle_time_ms(), 17);
        assert_eq!(batch.topics()[0].topic(), "orders");
        assert!(matches!(
            batch.topics()[0].result(),
            IncrementalAlterConfigResult::Altered
        ));
    }
}

#[test]
fn selected_version_is_required_and_cannot_exceed_v1() {
    assert_eq!(
        normalize_terminal(&plan(), LARGE_BUDGET, None, Ok(response(0, "orders"))),
        IncrementalAlterConfigsInput::InvalidResponse
    );
    assert_eq!(
        normalize_terminal(
            &plan(),
            LARGE_BUDGET,
            Some(ApiVersion::new(2)),
            Ok(response(0, "orders")),
        ),
        IncrementalAlterConfigsInput::ProtocolIncompatible {
            delivery: DeliveryStatus::PossiblySent
        }
    );
}

#[test]
fn retained_overflow_and_malformed_correlation_remain_distinct() {
    assert_eq!(
        normalize_terminal(
            &plan(),
            1,
            Some(ApiVersion::new(1)),
            Ok(response(0, "orders")),
        ),
        IncrementalAlterConfigsInput::ResponseTooLarge
    );
    assert_eq!(
        normalize_terminal(
            &plan(),
            LARGE_BUDGET,
            Some(ApiVersion::new(1)),
            Ok(response(0, "other")),
        ),
        IncrementalAlterConfigsInput::InvalidResponse
    );
}

#[test]
fn decode_and_driver_deadline_map_without_inventing_transport_policy() {
    assert_eq!(
        normalize_terminal(
            &plan(),
            LARGE_BUDGET,
            Some(ApiVersion::new(1)),
            Err(RequestError::Decode(DecodeError::UnexpectedEnd {
                offset: 1,
                needed: 4,
                remaining: 0,
            })),
        ),
        IncrementalAlterConfigsInput::InvalidResponse
    );
    assert_eq!(
        normalize_terminal(
            &plan(),
            LARGE_BUDGET,
            Some(ApiVersion::new(1)),
            Err(RequestError::Rejected {
                failure: CallFailure::DeadlineExceeded,
                delivery: Delivery::PossiblySent,
            }),
        ),
        IncrementalAlterConfigsInput::DriverDeadlineElapsed {
            delivery: DeliveryStatus::PossiblySent
        }
    );
}

#[test]
fn protocol_compatibility_failures_preserve_driver_certainty() {
    let failures = [
        RequestError::Encode(EncodeError::LengthOverflow {
            kind: "configuration value",
            length: usize::MAX,
            maximum: i32::MAX as usize,
        }),
        RequestError::UnsupportedVersion {
            message: "IncrementalAlterConfigs request",
            version: ApiVersion::new(2),
        },
        RequestError::ApiUnavailable {
            api_key: ApiKey::new(44),
        },
        RequestError::VersionLimitUnavailable {
            api_key: ApiKey::new(44),
            maximum: ApiVersion::new(1),
            negotiated_minimum: ApiVersion::new(2),
        },
    ];
    for failure in failures {
        assert_eq!(
            normalize_terminal(&plan(), LARGE_BUDGET, None, Err(failure)),
            IncrementalAlterConfigsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::NotSent
            }
        );
    }
}

#[test]
fn other_driver_failures_are_transport_failures_with_driver_certainty() {
    assert_eq!(
        normalize_terminal(
            &plan(),
            LARGE_BUDGET,
            None,
            Err(RequestError::RouteUnavailable),
        ),
        IncrementalAlterConfigsInput::TransportFailed {
            delivery: DeliveryStatus::NotSent
        }
    );
}

fn plan() -> IncrementalAlterConfigsPlan {
    IncrementalAlterConfigsPlan::new(
        vec![TopicConfigAlteration::new(
            "orders".to_owned(),
            vec![ConfigAlteration::set(
                "cleanup.policy".to_owned(),
                "compact".to_owned(),
            )],
        )],
        false,
    )
    .unwrap_or_else(|error| panic!("valid IncrementalAlterConfigs plan: {error}"))
}

fn response(throttle_time_ms: i32, topic: &str) -> IncrementalAlterConfigsResponse {
    let mut topic_result = AlterConfigsResourceResponse::default();
    topic_result.resource_type = 2;
    topic_result.resource_name = topic.into();
    let mut response = IncrementalAlterConfigsResponse::default();
    response.throttle_time_ms = throttle_time_ms;
    response.responses = vec![topic_result];
    response
}
