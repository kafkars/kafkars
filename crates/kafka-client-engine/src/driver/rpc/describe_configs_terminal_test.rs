//! Version-authoritative terminal normalization for generic `DescribeConfigs`.

use kafka_client_core::{
    DeliveryStatus, DescribeConfigResult, DescribeConfigsInput, DescribeConfigsPlan,
    DescribeConfigsResourceQuery,
};
use kafka_driver::{ApiKey, ApiVersion, CallFailure, Delivery, RequestError};
use kafka_wire::{
    DescribeConfigsResponse,
    describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResult},
};

use super::describe_configs_terminal::normalize_terminal;

const LARGE_BUDGET: usize = 1 << 20;

fn plan() -> DescribeConfigsPlan {
    DescribeConfigsPlan::new(
        vec![DescribeConfigsResourceQuery::new(
            2,
            "orders".to_owned(),
            None,
        )],
        true,
        true,
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"))
}

fn response() -> DescribeConfigsResponse {
    let mut config = DescribeConfigsResourceResult::default();
    config.name = "cleanup.policy".into();
    config.value = Some("compact".into());
    config.config_type = 2;
    config.documentation = Some("cleanup docs".into());
    let mut resource = DescribeConfigsResult::default();
    resource.resource_type = 2;
    resource.resource_name = "orders".into();
    resource.configs = vec![config];
    let mut response = DescribeConfigsResponse::default();
    response.throttle_time_ms = 77;
    response.results = vec![resource];
    response
}

#[test]
fn selected_v2_does_not_fabricate_v3_fields_but_retains_throttle() {
    let DescribeConfigsInput::BrokerResponded { batch } = normalize_terminal(
        &plan(),
        LARGE_BUDGET,
        Some(ApiVersion::new(2)),
        Ok(response()),
    ) else {
        panic!("bounded broker response expected");
    };
    assert_eq!(batch.throttle_time_ms(), 77);
    let DescribeConfigResult::Configs(configs) = batch.resources()[0].result() else {
        panic!("successful configs expected");
    };
    assert_eq!(configs[0].config_type(), None);
    assert_eq!(configs[0].documentation(), None);
}

#[test]
fn selected_v1_is_accepted_without_v3_fields() {
    let DescribeConfigsInput::BrokerResponded { batch } = normalize_terminal(
        &plan(),
        LARGE_BUDGET,
        Some(ApiVersion::new(1)),
        Ok(response()),
    ) else {
        panic!("v1 broker response expected");
    };
    let DescribeConfigResult::Configs(configs) = batch.resources()[0].result() else {
        panic!("successful configs expected");
    };
    assert_eq!(configs[0].config_type(), None);
    assert_eq!(configs[0].documentation(), None);
}

#[test]
fn selected_v3_retains_type_and_documentation() {
    let DescribeConfigsInput::BrokerResponded { batch } = normalize_terminal(
        &plan(),
        LARGE_BUDGET,
        Some(ApiVersion::new(3)),
        Ok(response()),
    ) else {
        panic!("bounded broker response expected");
    };
    let DescribeConfigResult::Configs(configs) = batch.resources()[0].result() else {
        panic!("successful configs expected");
    };
    assert_eq!(configs[0].config_type(), Some(2));
    assert_eq!(configs[0].documentation(), Some("cleanup docs"));
}

#[test]
fn missing_selected_version_is_an_operation_local_invalid_response() {
    assert_eq!(
        normalize_terminal(&plan(), LARGE_BUDGET, None, Ok(response())),
        DescribeConfigsInput::InvalidResponse
    );
}

#[test]
fn selected_v0_is_possibly_sent_compatibility_not_transport() {
    assert_eq!(
        normalize_terminal(
            &plan(),
            LARGE_BUDGET,
            Some(ApiVersion::new(0)),
            Ok(response()),
        ),
        DescribeConfigsInput::ProtocolIncompatible {
            delivery: DeliveryStatus::PossiblySent
        }
    );
}

#[test]
fn request_bytes_cannot_inflate_the_response_capacity() {
    assert_eq!(
        normalize_terminal(&plan(), 1, Some(ApiVersion::new(4)), Ok(response())),
        DescribeConfigsInput::ResponseTooLarge
    );
}

#[test]
fn version_failure_preserves_driver_authoritative_delivery() {
    let api_key = ApiKey::new(32);
    for failure in [
        RequestError::UnsupportedVersion {
            message: "DescribeConfigs request",
            version: ApiVersion::new(0),
        },
        RequestError::VersionFloorUnavailable {
            api_key,
            minimum: ApiVersion::new(1),
            negotiated_maximum: ApiVersion::new(0),
        },
        RequestError::VersionBoundsInvalid {
            api_key,
            minimum: ApiVersion::new(4),
            maximum: ApiVersion::new(1),
        },
    ] {
        assert_eq!(
            normalize_terminal(&plan(), LARGE_BUDGET, None, Err(failure)),
            DescribeConfigsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::NotSent
            }
        );
    }
}

#[test]
fn driver_deadline_preserves_possibly_sent_certainty() {
    assert_eq!(
        normalize_terminal(
            &plan(),
            LARGE_BUDGET,
            Some(ApiVersion::new(4)),
            Err(RequestError::Rejected {
                failure: CallFailure::DeadlineExceeded,
                delivery: Delivery::PossiblySent,
            }),
        ),
        DescribeConfigsInput::DriverDeadlineElapsed {
            delivery: DeliveryStatus::PossiblySent
        }
    );
}
