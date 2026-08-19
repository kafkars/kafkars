//! `CreateAcls` route, version, deadline, lane, and failure classification tests.

use std::time::{Duration, Instant};

use kafka_client_core::{CreateAclBinding, CreateAclsPlan, DeliveryStatus};
use kafka_driver::{
    ApiKey, ApiVersion, CallFailure, CompletionError, Delivery, RequestError, Route, TrafficClass,
};
use kafka_wire::CreateAclsResponse;
use kafka_wire_core::{DecodeError, EncodeError};

use crate::{EngineConfig, driver::DriverOwner};

use super::{
    CreateAclsCall,
    create_acls_call::CreateAclsEvidence,
    create_acls_submission::{create_acls_options, create_acls_route},
    create_acls_terminal::{
        CreateAclsDriverFailureKind, CreateAclsTerminalFact, create_acls_failure_kind,
        retain_create_acls_terminal,
    },
};

#[test]
fn route_is_any_broker_without_controller_or_metadata_authority() {
    assert_eq!(create_acls_route(), Route::AnyBroker);
}

#[test]
fn options_preserve_original_deadline_lane_and_exact_generated_window() {
    let deadline = Instant::now() + Duration::from_secs(7);
    let options = create_acls_options(deadline);

    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(1)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(3)));
}

#[test]
fn completion_fault_remains_recoverable_after_driver_shutdown() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let plan = plan("orders");
    let expected_plan = plan.clone();
    let mut call = CreateAclsCall::submit(
        &driver,
        plan,
        8 * 1024 * 1024,
        8 * 1024 * 1024,
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap_or_else(|_error| panic!("accepted call"));
    assert!(call.matches_evidence(&expected_plan, 8 * 1024 * 1024, 8 * 1024 * 1024));
    drop(driver);

    assert!(matches!(
        call.try_terminal(),
        Some(Err(CompletionError::Closed))
    ));
    let recovered = call
        .recover_after_driver_shutdown()
        .unwrap_or_else(|| panic!("completion fault must retain accepted call ownership"));
    assert!(recovered.matches_evidence(&expected_plan, 8 * 1024 * 1024, 8 * 1024 * 1024));
    recovered.seal();
}

#[test]
fn synchronous_rejection_returns_exact_plan_and_bounds() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let plan = plan("orders");
    let expected_plan = plan.clone();
    let rejection = match CreateAclsCall::submit(
        &driver,
        plan,
        0,
        8 * 1024 * 1024,
        Instant::now() + Duration::from_secs(1),
    ) {
        Ok(_call) => panic!("zero request bound must reject"),
        Err(rejection) => rejection,
    };

    assert_eq!(
        rejection.into_submission_evidence(),
        (expected_plan, 0, 8 * 1024 * 1024)
    );
}

#[test]
fn request_failure_categories_are_stable_and_exhaustive_in_the_adapter() {
    let cases = [
        (
            RequestError::Rejected {
                failure: CallFailure::DeadlineExceeded,
                delivery: Delivery::PossiblySent,
            },
            CreateAclsDriverFailureKind::DeadlineElapsed,
        ),
        (
            RequestError::Decode(DecodeError::UnexpectedEnd {
                offset: 0,
                needed: 1,
                remaining: 0,
            }),
            CreateAclsDriverFailureKind::InvalidResponse,
        ),
        (
            RequestError::Encode(EncodeError::LengthOverflow {
                kind: "ACL principal",
                length: usize::MAX,
                maximum: i16::MAX as usize,
            }),
            CreateAclsDriverFailureKind::Compatibility,
        ),
        (
            RequestError::UnsupportedVersion {
                message: "CreateAcls request",
                version: ApiVersion::new(4),
            },
            CreateAclsDriverFailureKind::Compatibility,
        ),
        (
            RequestError::ApiUnavailable {
                api_key: ApiKey::new(30),
            },
            CreateAclsDriverFailureKind::Compatibility,
        ),
        (
            RequestError::RouteUnavailable,
            CreateAclsDriverFailureKind::Transport,
        ),
    ];

    for (error, expected) in cases {
        assert_eq!(create_acls_failure_kind(&error), expected);
    }
}

#[test]
fn raw_terminal_preserves_driver_authoritative_delivery() {
    for (delivery, expected) in [
        (Delivery::NotSent, DeliveryStatus::NotSent),
        (Delivery::PossiblySent, DeliveryStatus::PossiblySent),
    ] {
        let terminal = retain_create_acls_terminal(
            None,
            Err(RequestError::Rejected {
                failure: CallFailure::DeadlineExceeded,
                delivery,
            }),
            None,
            CreateAclsEvidence::new(plan("orders"), 4_096, 8_192),
        );
        assert!(terminal.matches_evidence(&plan("orders"), 4_096, 8_192));
        assert!(!terminal.matches_evidence(&plan("payments"), 4_096, 8_192));
        assert!(!terminal.matches_evidence(&plan("orders"), 4_095, 8_192));
        assert!(!terminal.matches_evidence(&plan("orders"), 4_096, 8_191));
        let CreateAclsTerminalFact::Failed {
            kind,
            delivery: actual,
        } = terminal.fact()
        else {
            panic!("expected failed terminal");
        };
        assert_eq!(kind, CreateAclsDriverFailureKind::DeadlineElapsed);
        assert_eq!(actual, expected);
        terminal.discard();
    }
}

#[test]
fn raw_success_retains_the_selected_version_until_settlement() {
    let terminal = retain_create_acls_terminal(
        Some(ApiVersion::new(2)),
        Ok(CreateAclsResponse::default()),
        None,
        CreateAclsEvidence::new(plan("orders"), 4_096, 8_192),
    );
    let CreateAclsTerminalFact::Response {
        selected_version,
        response,
    } = terminal.fact()
    else {
        panic!("expected response terminal");
    };

    assert_eq!(selected_version, Some(2));
    assert_eq!(response.throttle_time_ms, 0);
    terminal.discard();
}

fn plan(resource_name: &str) -> CreateAclsPlan {
    CreateAclsPlan::new(vec![CreateAclBinding::new(
        2,
        resource_name.to_owned(),
        3,
        "User:alice".to_owned(),
        "*".to_owned(),
        3,
        3,
    )])
    .unwrap_or_else(|error| panic!("valid plan: {error}"))
}
