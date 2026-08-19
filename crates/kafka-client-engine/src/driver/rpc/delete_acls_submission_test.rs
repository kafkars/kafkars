//! `DeleteAcls` route, deadline, version, retention, and terminal evidence tests.

use std::{
    mem::size_of,
    time::{Duration, Instant},
};

use kafka_client_core::{DeleteAclsFilter, DeleteAclsPlan, DeliveryStatus};
use kafka_driver::{
    ApiKey, ApiVersion, CallFailure, CompletionError, Delivery, RequestError, Route, TrafficClass,
};
use kafka_wire::DeleteAclsResponse;
use kafka_wire_core::{DecodeError, EncodeError};

use super::{
    delete_acls_call::{
        DeleteAclsCall, DeleteAclsCallAdmissionSource, DeleteAclsEvidence,
        prepare_delete_acls_filter_refs,
    },
    delete_acls_submission::{delete_acls_options, delete_acls_route},
    delete_acls_terminal::{
        DeleteAclsDriverFailureKind, DeleteAclsTerminalFact, delete_acls_failure_kind,
        retain_delete_acls_terminal,
    },
};
use crate::{EngineConfig, driver::DriverOwner, protocol::admin::delete_acls::DeleteAclsFilterRef};

#[test]
fn route_is_any_broker_without_controller_or_metadata_authority() {
    assert_eq!(delete_acls_route(), Route::AnyBroker);
}

#[test]
fn options_preserve_original_deadline_lane_and_exact_generated_window() {
    let deadline = Instant::now() + Duration::from_secs(7);
    let options = delete_acls_options(deadline);

    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(1)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(3)));
}

#[test]
fn borrowed_filter_storage_preserves_plan_order_and_charges_actual_capacity() {
    let duplicate = filter(2, Some("orders"), 3, Some("User:a"), Some("*"), 3, 3);
    let plan = DeleteAclsPlan::new(vec![duplicate.clone(), duplicate])
        .unwrap_or_else(|error| panic!("duplicate filters retain distinct positions: {error:?}"));
    let retained_limit = usize::MAX;
    let (filters, request_limit) = prepare_delete_acls_filter_refs(plan.filters(), retained_limit)
        .unwrap_or_else(|error| panic!("borrowed filter storage: {error:?}"));
    let actual_bytes = filters.capacity() * size_of::<DeleteAclsFilterRef<'static>>();

    assert_eq!(filters.len(), 2);
    assert_eq!(filters[0], filters[1]);
    assert_eq!(filters[0].resource_name(), Some("orders"));
    assert_eq!(retained_limit - request_limit, actual_bytes);
}

#[test]
fn borrowed_filter_storage_rejects_before_allocating_beyond_its_limit() {
    let plan = DeleteAclsPlan::new(vec![filter(1, None, 1, None, None, 1, 1)])
        .unwrap_or_else(|error| panic!("valid filter: {error:?}"));
    let minimum = size_of::<DeleteAclsFilterRef<'static>>();

    assert_eq!(
        prepare_delete_acls_filter_refs(plan.filters(), minimum - 1),
        Err(DeleteAclsCallAdmissionSource::Request)
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
            DeleteAclsDriverFailureKind::DeadlineElapsed,
        ),
        (
            RequestError::Decode(DecodeError::UnexpectedEnd {
                offset: 0,
                needed: 1,
                remaining: 0,
            }),
            DeleteAclsDriverFailureKind::InvalidResponse,
        ),
        (
            RequestError::Encode(EncodeError::LengthOverflow {
                kind: "ACL principal",
                length: usize::MAX,
                maximum: i16::MAX as usize,
            }),
            DeleteAclsDriverFailureKind::Compatibility,
        ),
        (
            RequestError::UnsupportedVersion {
                message: "DeleteAcls request",
                version: ApiVersion::new(4),
            },
            DeleteAclsDriverFailureKind::Compatibility,
        ),
        (
            RequestError::ApiUnavailable {
                api_key: ApiKey::new(31),
            },
            DeleteAclsDriverFailureKind::Compatibility,
        ),
        (
            RequestError::RouteUnavailable,
            DeleteAclsDriverFailureKind::Transport,
        ),
    ];

    for (error, expected) in cases {
        assert_eq!(delete_acls_failure_kind(&error), expected);
    }
}

#[test]
fn raw_terminal_preserves_driver_authoritative_delivery() {
    for (delivery, expected) in [
        (Delivery::NotSent, DeliveryStatus::NotSent),
        (Delivery::PossiblySent, DeliveryStatus::PossiblySent),
    ] {
        let plan = plan();
        let terminal = retain_delete_acls_terminal(
            None,
            Err(RequestError::Rejected {
                failure: CallFailure::DeadlineExceeded,
                delivery,
            }),
            None,
            evidence(plan.clone(), 4_096, 1, 1, 1),
        );
        assert!(terminal.matches(&plan, 4_096, 1, 1, 1));
        assert!(!terminal.matches(&plan, 4_096, 2, 1, 1));
        let DeleteAclsTerminalFact::Failed {
            kind,
            delivery: actual,
        } = terminal.fact()
        else {
            panic!("expected failed terminal");
        };
        assert_eq!(kind, DeleteAclsDriverFailureKind::DeadlineElapsed);
        assert_eq!(actual, expected);
        terminal.discard();
    }
}

#[test]
fn raw_success_retains_the_selected_version_until_settlement() {
    let plan = plan();
    let terminal = retain_delete_acls_terminal(
        Some(ApiVersion::new(2)),
        Ok(DeleteAclsResponse::default()),
        None,
        evidence(plan.clone(), 4_096, 1, 1, 1),
    );
    assert!(terminal.matches(&plan, 4_096, 1, 1, 1));
    assert!(!terminal.matches(&plan, 4_095, 1, 1, 1));
    let DeleteAclsTerminalFact::Response {
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

#[test]
fn completion_fault_retains_the_accepted_call_for_recovery() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let plan = DeleteAclsPlan::new(vec![filter(1, None, 1, None, None, 1, 1)])
        .unwrap_or_else(|error| panic!("valid filter: {error:?}"));
    let mut call = DeleteAclsCall::submit(
        &driver,
        plan.clone(),
        4 * 1024 * 1024,
        1,
        1,
        1,
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap_or_else(|_error| panic!("accepted call"));
    assert!(call.matches(&plan, 4 * 1024 * 1024, 1, 1, 1));
    drop(driver);

    assert!(matches!(
        call.try_terminal(),
        Some(Err(CompletionError::Closed))
    ));
    let recovered = call
        .recover_after_driver_shutdown()
        .unwrap_or_else(|| panic!("completion fault must retain accepted call ownership"));
    assert!(recovered.matches(&plan, 4 * 1024 * 1024, 1, 1, 1));
    recovered.seal();
}

#[test]
fn synchronous_rejection_returns_ordered_filters_and_every_bound() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let expected = DeleteAclsPlan::new(vec![
        filter(2, Some("orders"), 3, Some("User:a"), Some("*"), 3, 3),
        filter(3, Some("payments"), 4, Some("User:b"), Some("host"), 4, 2),
    ])
    .unwrap_or_else(|error| panic!("ordered filters: {error:?}"));
    let rejection = match DeleteAclsCall::submit(
        &driver,
        expected.clone(),
        0,
        17,
        19,
        23,
        Instant::now() + Duration::from_secs(1),
    ) {
        Err(rejection) => rejection,
        Ok(_call) => panic!("zero request bound must reject before driver ownership"),
    };
    let (actual, request_limit, nested_limit, result_limit, outcome_limit) =
        rejection.into_evidence();
    assert_eq!(actual, expected);
    assert_eq!(
        (request_limit, nested_limit, result_limit, outcome_limit),
        (0, 17, 19, 23)
    );
}

fn plan() -> DeleteAclsPlan {
    DeleteAclsPlan::new(vec![filter(1, None, 1, None, None, 1, 1)])
        .unwrap_or_else(|error| panic!("valid filter: {error:?}"))
}

fn evidence(
    plan: DeleteAclsPlan,
    request_limit: usize,
    nested_limit: usize,
    result_limit: usize,
    outcome_limit: usize,
) -> DeleteAclsEvidence {
    DeleteAclsEvidence::new(
        plan,
        request_limit,
        nested_limit,
        result_limit,
        outcome_limit,
    )
}

fn filter(
    resource_type: i8,
    resource_name: Option<&str>,
    pattern_type: i8,
    principal: Option<&str>,
    host: Option<&str>,
    operation: i8,
    permission_type: i8,
) -> DeleteAclsFilter {
    DeleteAclsFilter::new(
        resource_type,
        resource_name.map(str::to_owned),
        pattern_type,
        principal.map(str::to_owned),
        host.map(str::to_owned),
        operation,
        permission_type,
    )
}
