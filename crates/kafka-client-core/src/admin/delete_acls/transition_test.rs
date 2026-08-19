//! `AnyBroker` submission, positional results, storage reuse, and terminal tests.

#![expect(
    clippy::needless_pass_by_value,
    reason = "test helpers preserve exact effect ownership"
)]

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    DELETE_ACLS_DIAGNOSTIC_BYTES, DeleteAclBrokerError, DeleteAclFilterResult,
    DeleteAclMatchResult, DeleteAclMatchingBinding, DeleteAclsEffect, DeleteAclsFailureKind,
    DeleteAclsFilter, DeleteAclsInput, DeleteAclsMachine, DeleteAclsMachineError, DeleteAclsPlan,
    DeleteAclsRoute, DeleteAclsTerminal,
};

#[test]
fn sole_any_broker_attempt_preserves_duplicate_filter_positions_and_reuses_storage() {
    let duplicate = filter(Some("orders"));
    let mut machine = machine(vec![duplicate.clone(), duplicate]);
    let filter_pointer = machine
        .plan()
        .unwrap_or_else(|| panic!("plan"))
        .filters()
        .as_ptr();
    let submit = effect(
        &mut machine,
        DeleteAclsInput::Start {
            now: Moment::from_tick(1),
        },
    );
    let DeleteAclsEffect::Submit {
        operation_id,
        deadline,
        route,
        plan,
    } = submit
    else {
        panic!("expected submit");
    };
    assert_eq!(operation_id, OperationId::from_raw(43));
    assert_eq!(deadline, Deadline::from_tick(100));
    assert_eq!(route, DeleteAclsRoute::AnyBroker);
    assert_eq!(plan.filters()[0], plan.filters()[1]);

    machine
        .apply(DeleteAclsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept: {error}"));
    let mut matches = Vec::new();
    matches
        .try_reserve_exact(2)
        .unwrap_or_else(|error| panic!("reserve matches: {error}"));
    matches.push(matching("second", DeleteAclMatchResult::Deleted));
    matches.push(matching(
        "first",
        DeleteAclMatchResult::BrokerFailed(broker_error(-731, Some("denied"))),
    ));
    let matching_pointer = matches.as_ptr();
    let mut results = Vec::new();
    results
        .try_reserve_exact(2)
        .unwrap_or_else(|error| panic!("reserve results: {error}"));
    results.push(DeleteAclFilterResult::Matched(matches));
    results.push(DeleteAclFilterResult::BrokerFailed(broker_error(17, None)));
    let result_pointer = results.as_ptr();
    let terminal = effect(
        &mut machine,
        DeleteAclsInput::BrokerResponded {
            throttle_time_ms: 9,
            results,
        },
    );
    let DeleteAclsEffect::Complete {
        terminal: DeleteAclsTerminal::Deleted(batch),
        ..
    } = terminal
    else {
        panic!("expected deleted terminal");
    };

    assert_eq!(batch.throttle_time_ms(), 9);
    assert_eq!(batch.filters().as_ptr(), filter_pointer);
    assert_eq!(batch.results().as_ptr(), result_pointer);
    let DeleteAclFilterResult::Matched(matches) = &batch.results()[0] else {
        panic!("expected matches");
    };
    assert_eq!(matches.as_ptr(), matching_pointer);
    assert_eq!(matches[0].resource_name(), "second");
    assert_eq!(matches[1].resource_name(), "first");
}

#[test]
fn duplicate_matching_binding_is_an_invalid_single_terminal() {
    let mut machine = submitted_machine();
    let duplicate = matching("orders", DeleteAclMatchResult::Deleted);
    let terminal = effect(
        &mut machine,
        DeleteAclsInput::BrokerResponded {
            throttle_time_ms: 0,
            results: vec![DeleteAclFilterResult::Matched(vec![
                duplicate.clone(),
                duplicate,
            ])],
        },
    );

    assert_failure(
        terminal,
        DeleteAclsFailureKind::InvalidResponse,
        DeliveryStatus::PossiblySent,
    );
    assert_eq!(
        machine.apply(DeleteAclsInput::InvalidResponse),
        Err(DeleteAclsMachineError::AlreadyCompleted)
    );
}

#[test]
fn oversized_diagnostic_and_result_count_are_invalid() {
    let mut diagnostic = submitted_machine();
    let terminal = effect(
        &mut diagnostic,
        DeleteAclsInput::BrokerResponded {
            throttle_time_ms: 0,
            results: vec![DeleteAclFilterResult::BrokerFailed(broker_error(
                17,
                Some(&"x".repeat(DELETE_ACLS_DIAGNOSTIC_BYTES + 1)),
            ))],
        },
    );
    assert_failure(
        terminal,
        DeleteAclsFailureKind::InvalidResponse,
        DeliveryStatus::PossiblySent,
    );

    let mut count = submitted_machine();
    let terminal = effect(
        &mut count,
        DeleteAclsInput::BrokerResponded {
            throttle_time_ms: 0,
            results: Vec::new(),
        },
    );
    assert_failure(
        terminal,
        DeleteAclsFailureKind::InvalidResponse,
        DeliveryStatus::PossiblySent,
    );
}

#[test]
fn malformed_concrete_matching_binding_is_invalid() {
    let malformed = DeleteAclMatchingBinding::new(
        1,
        "orders".to_owned(),
        3,
        "User:alice".to_owned(),
        "*".to_owned(),
        3,
        3,
        DeleteAclMatchResult::Deleted,
    );
    let mut machine = submitted_machine();
    let terminal = effect(
        &mut machine,
        DeleteAclsInput::BrokerResponded {
            throttle_time_ms: 0,
            results: vec![DeleteAclFilterResult::Matched(vec![malformed])],
        },
    );

    assert_failure(
        terminal,
        DeleteAclsFailureKind::InvalidResponse,
        DeliveryStatus::PossiblySent,
    );
}

#[test]
fn transport_delivery_is_authoritative_and_no_retry_follows() {
    let mut machine = submitted_machine();
    let terminal = effect(
        &mut machine,
        DeleteAclsInput::TransportFailed {
            delivery: DeliveryStatus::PossiblySent,
        },
    );
    assert_failure(
        terminal,
        DeleteAclsFailureKind::Transport,
        DeliveryStatus::PossiblySent,
    );
    assert_eq!(
        machine.apply(DeleteAclsInput::Start {
            now: Moment::from_tick(2)
        }),
        Err(DeleteAclsMachineError::AlreadyCompleted)
    );
}

#[test]
fn pre_driver_expiry_is_definitely_unsent() {
    let mut machine = DeleteAclsMachine::new(
        OperationId::from_raw(43),
        Deadline::from_tick(5),
        plan(vec![filter(None)]),
    );
    let terminal = effect(
        &mut machine,
        DeleteAclsInput::Start {
            now: Moment::from_tick(5),
        },
    );

    assert_failure(
        terminal,
        DeleteAclsFailureKind::DeadlineElapsed,
        DeliveryStatus::NotSent,
    );
}

fn submitted_machine() -> DeleteAclsMachine {
    let mut machine = machine(vec![filter(None)]);
    effect(
        &mut machine,
        DeleteAclsInput::Start {
            now: Moment::from_tick(1),
        },
    );
    machine
        .apply(DeleteAclsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept: {error}"));
    machine
}

fn machine(filters: Vec<DeleteAclsFilter>) -> DeleteAclsMachine {
    DeleteAclsMachine::new(
        OperationId::from_raw(43),
        Deadline::from_tick(100),
        plan(filters),
    )
}

fn plan(filters: Vec<DeleteAclsFilter>) -> DeleteAclsPlan {
    DeleteAclsPlan::new(filters).unwrap_or_else(|error| panic!("valid plan: {error}"))
}

fn filter(resource_name: Option<&str>) -> DeleteAclsFilter {
    DeleteAclsFilter::new(1, resource_name.map(str::to_owned), 1, None, None, 1, 1)
}

fn matching(name: &str, result: DeleteAclMatchResult) -> DeleteAclMatchingBinding {
    DeleteAclMatchingBinding::new(
        2,
        name.to_owned(),
        3,
        "User:alice".to_owned(),
        "*".to_owned(),
        3,
        3,
        result,
    )
}

fn broker_error(code: i16, message: Option<&str>) -> DeleteAclBrokerError {
    DeleteAclBrokerError::new(
        NonZeroI16::new(code).unwrap_or_else(|| panic!("nonzero")),
        message.map(str::to_owned),
        false,
    )
}

fn effect(machine: &mut DeleteAclsMachine, input: DeleteAclsInput) -> DeleteAclsEffect {
    machine
        .apply(input)
        .unwrap_or_else(|error| panic!("transition: {error}"))
        .into_effect()
        .unwrap_or_else(|| panic!("expected effect"))
}

fn assert_failure(
    effect: DeleteAclsEffect,
    expected_kind: DeleteAclsFailureKind,
    expected_delivery: DeliveryStatus,
) {
    let DeleteAclsEffect::Complete {
        terminal: DeleteAclsTerminal::Failed(failure),
        ..
    } = effect
    else {
        panic!("expected failed terminal");
    };
    assert_eq!(failure.kind(), expected_kind);
    assert_eq!(failure.delivery(), expected_delivery);
}
