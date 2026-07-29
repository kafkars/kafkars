//! Selected-version and driver-authoritative failure facts for legacy AlterConfigs.

use kafka_client_core::{
    DeliveryStatus, LegacyAlterConfigsPlan, LegacyAlterConfigsRoute, LegacyConfigEntry,
    LegacyTopicConfigReplacement,
};
use kafka_driver::{ApiKey, ApiVersion, CallFailure, Delivery, RequestError};
use kafka_wire::AlterConfigsResponse;
use kafka_wire_core::DecodeError;

use super::legacy_alter_configs_terminal::{
    LegacyAlterConfigsDriverFailureKind, LegacyAlterConfigsTerminalFact,
    RecoveredLegacyAlterConfigsCall, retain_legacy_alter_configs_terminal,
};

#[test]
fn response_fact_preserves_every_selected_stable_version() {
    for version in [0, 1, 2] {
        let mut response = AlterConfigsResponse::default();
        response.throttle_time_ms = 23;
        let plan = plan();
        let terminal = retain_legacy_alter_configs_terminal(
            Some(ApiVersion::new(version)),
            Ok(response),
            None,
            LegacyAlterConfigsRoute::AnyBroker,
            plan.clone(),
        );
        let LegacyAlterConfigsTerminalFact::Response {
            selected_version,
            response,
        } = terminal.fact()
        else {
            panic!("response expected");
        };
        assert_eq!(selected_version, Some(version));
        assert_eq!(response.throttle_time_ms, 23);
        assert_eq!(terminal.route(), LegacyAlterConfigsRoute::AnyBroker);
        assert_eq!(terminal.plan(), &plan);
        terminal.discard();
    }
}

#[test]
fn failures_preserve_delivery_certainty_and_stable_classification() {
    let cases = [
        (
            RequestError::Rejected {
                failure: CallFailure::DeadlineExceeded,
                delivery: Delivery::PossiblySent,
            },
            LegacyAlterConfigsDriverFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            RequestError::VersionLimitUnavailable {
                api_key: ApiKey::new(33),
                maximum: ApiVersion::new(2),
                negotiated_minimum: ApiVersion::new(3),
            },
            LegacyAlterConfigsDriverFailureKind::Compatibility,
            DeliveryStatus::NotSent,
        ),
        (
            RequestError::Decode(DecodeError::UnexpectedEnd {
                offset: 1,
                needed: 4,
                remaining: 0,
            }),
            LegacyAlterConfigsDriverFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        ),
        (
            RequestError::RouteUnavailable,
            LegacyAlterConfigsDriverFailureKind::Transport,
            DeliveryStatus::NotSent,
        ),
    ];
    for (error, expected_kind, expected_delivery) in cases {
        let terminal = retain_legacy_alter_configs_terminal(
            None,
            Err(error),
            None,
            LegacyAlterConfigsRoute::AnyBroker,
            plan(),
        );
        let LegacyAlterConfigsTerminalFact::Failed { kind, delivery } = terminal.fact() else {
            panic!("failure expected");
        };
        assert_eq!(kind, expected_kind);
        assert_eq!(delivery, expected_delivery);
        terminal.discard();
    }
}

#[test]
fn shutdown_recovery_token_seals_linearly() {
    let plan = plan();
    let recovered =
        RecoveredLegacyAlterConfigsCall::new(LegacyAlterConfigsRoute::AnyBroker, plan.clone());
    assert!(recovered.matches_correlation(LegacyAlterConfigsRoute::AnyBroker, &plan,));
    recovered.seal();
}

fn plan() -> LegacyAlterConfigsPlan {
    LegacyAlterConfigsPlan::new(
        vec![LegacyTopicConfigReplacement::new(
            "orders".to_owned(),
            vec![LegacyConfigEntry::new(
                "cleanup.policy".to_owned(),
                Some("compact".to_owned()),
            )],
        )],
        false,
    )
    .unwrap_or_else(|error| panic!("plan: {error}"))
}
