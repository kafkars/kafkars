//! Selected-version and driver-failure classification scenarios.

use core::num::NonZeroI16;

use kafka_client_core::{
    DeliveryStatus, ListPartitionReassignmentTarget, ListPartitionReassignmentsBrokerError,
    ListPartitionReassignmentsInput, ListPartitionReassignmentsPlan,
};
use kafka_driver::{ApiKey, ApiVersion, CallFailure, Delivery, RequestError};
use kafka_wire::ListPartitionReassignmentsResponse;

use super::list_partition_reassignments_terminal::{
    ListPartitionReassignmentsDriverFailureKind, ListPartitionReassignmentsTerminalFact,
    input_requires_controller_refresh, retain_list_partition_reassignments_terminal,
};

#[test]
fn only_exact_normalized_not_controller_requires_refresh() {
    assert!(input_requires_controller_refresh(&broker_rejected(41)));
    assert!(!input_requires_controller_refresh(&broker_rejected(42)));
    assert!(!input_requires_controller_refresh(
        &ListPartitionReassignmentsInput::InvalidResponse
    ));
}

#[test]
fn response_fact_borrows_exact_v0_and_generated_response() {
    let mut response = ListPartitionReassignmentsResponse::default();
    response.throttle_time_ms = 19;
    let plan = ListPartitionReassignmentsPlan::all_active();
    let terminal = retain_list_partition_reassignments_terminal(
        plan.clone(),
        4096,
        Some(ApiVersion::new(0)),
        Ok(response),
        None,
    );
    let ListPartitionReassignmentsTerminalFact::Response {
        selected_version,
        response,
    } = terminal.fact()
    else {
        panic!("generated response expected");
    };
    assert_eq!(selected_version, Some(0));
    assert_eq!(response.throttle_time_ms, 19);
    assert!(terminal.matches(&plan, 4096));
    assert!(!terminal.matches(&plan, 4097));
    terminal.discard();
}

#[test]
fn failures_preserve_driver_authoritative_delivery() {
    let cases = [
        (
            RequestError::Rejected {
                failure: CallFailure::DeadlineExceeded,
                delivery: Delivery::PossiblySent,
            },
            ListPartitionReassignmentsDriverFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            RequestError::VersionFloorUnavailable {
                api_key: ApiKey::new(46),
                minimum: ApiVersion::new(0),
                negotiated_maximum: ApiVersion::new(-1),
            },
            ListPartitionReassignmentsDriverFailureKind::Compatibility,
            DeliveryStatus::NotSent,
        ),
        (
            RequestError::RouteUnavailable,
            ListPartitionReassignmentsDriverFailureKind::Transport,
            DeliveryStatus::NotSent,
        ),
    ];
    for (error, expected_kind, expected_delivery) in cases {
        let terminal = retain_list_partition_reassignments_terminal(
            ListPartitionReassignmentsPlan::all_active(),
            4096,
            None,
            Err(error),
            None,
        );
        let ListPartitionReassignmentsTerminalFact::Failed { kind, delivery } = terminal.fact()
        else {
            panic!("failure fact expected");
        };
        assert_eq!(kind, expected_kind);
        assert_eq!(delivery, expected_delivery);
        terminal.discard();
    }
}

#[test]
fn raw_terminal_rejects_a_different_partition_filter() {
    let expected =
        ListPartitionReassignmentsPlan::selected(vec![ListPartitionReassignmentTarget::new(
            "orders".to_owned(),
            0,
        )])
        .unwrap_or_else(|error| panic!("expected plan: {error}"));
    let different =
        ListPartitionReassignmentsPlan::selected(vec![ListPartitionReassignmentTarget::new(
            "orders".to_owned(),
            1,
        )])
        .unwrap_or_else(|error| panic!("different plan: {error}"));
    let terminal = retain_list_partition_reassignments_terminal(
        expected.clone(),
        4096,
        Some(ApiVersion::new(0)),
        Ok(ListPartitionReassignmentsResponse::default()),
        None,
    );

    assert!(terminal.matches(&expected, 4096));
    assert!(!terminal.matches(&different, 4096));
    terminal.discard();
}

fn broker_rejected(code: i16) -> ListPartitionReassignmentsInput {
    ListPartitionReassignmentsInput::BrokerRejected {
        error: ListPartitionReassignmentsBrokerError::new(
            NonZeroI16::new(code).unwrap_or_else(|| panic!("test code must be nonzero")),
            None,
            false,
        ),
    }
}
