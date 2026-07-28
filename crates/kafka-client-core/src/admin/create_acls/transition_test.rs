//! AnyBroker submission, allocation reuse, and terminal-assignment tests.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    CREATE_ACLS_DIAGNOSTIC_BYTES, CreateAclBinding, CreateAclBrokerError, CreateAclResult,
    CreateAclsEffect, CreateAclsFailureKind, CreateAclsInput, CreateAclsMachine,
    CreateAclsMachineError, CreateAclsPlan, CreateAclsRoute, CreateAclsTerminal,
};

#[test]
fn sole_any_broker_submission_reuses_original_deadline_and_reserved_terminal_vectors() {
    let mut machine = machine(vec![binding("first"), binding("second")]);
    let binding_pointer = machine
        .plan()
        .unwrap_or_else(|| panic!("plan"))
        .bindings()
        .as_ptr();
    let submit = effect(
        &mut machine,
        CreateAclsInput::Start {
            now: Moment::from_tick(1),
        },
    );
    let CreateAclsEffect::Submit {
        operation_id,
        deadline,
        route,
        plan,
    } = submit
    else {
        panic!("expected submission");
    };
    assert_eq!(operation_id, OperationId::from_raw(41));
    assert_eq!(deadline, Deadline::from_tick(100));
    assert_eq!(route, CreateAclsRoute::AnyBroker);
    assert_eq!(plan.bindings()[0].resource_name(), "first");

    machine
        .apply(CreateAclsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept: {error}"));
    let mut results = Vec::with_capacity(8);
    results.push(CreateAclResult::Created);
    results.push(CreateAclResult::BrokerFailed(broker_error(-731, "denied")));
    let result_pointer = results.as_ptr();
    let terminal = effect(
        &mut machine,
        CreateAclsInput::BrokerResponded {
            throttle_time_ms: 9,
            results,
        },
    );
    let CreateAclsEffect::Complete {
        operation_id,
        terminal: CreateAclsTerminal::Created(batch),
    } = terminal
    else {
        panic!("expected created terminal");
    };
    assert_eq!(operation_id, OperationId::from_raw(41));
    assert_eq!(batch.throttle_time_ms(), 9);
    assert_eq!(batch.bindings().as_ptr(), binding_pointer);
    assert_eq!(batch.results().as_ptr(), result_pointer);
    assert_eq!(batch.bindings()[0].resource_name(), "first");
    assert_eq!(batch.bindings()[1].resource_name(), "second");
    assert!(matches!(
        &batch.results()[1],
        CreateAclResult::BrokerFailed(error)
            if error.code() == -731 && error.message() == Some("denied")
    ));
}

#[test]
fn pre_driver_expiry_is_definitely_unsent_and_emits_no_submission() {
    let mut machine = CreateAclsMachine::new(
        OperationId::from_raw(41),
        Deadline::from_tick(5),
        plan(vec![binding("orders")]),
    );
    let terminal = effect(
        &mut machine,
        CreateAclsInput::Start {
            now: Moment::from_tick(5),
        },
    );

    assert_failure(
        terminal,
        CreateAclsFailureKind::DeadlineElapsed,
        DeliveryStatus::NotSent,
    );
}

#[test]
fn mechanism_failure_preserves_authoritative_delivery_and_completes_once() {
    let mut machine = submitted_machine();
    let terminal = effect(
        &mut machine,
        CreateAclsInput::TransportFailed {
            delivery: DeliveryStatus::PossiblySent,
        },
    );

    assert_failure(
        terminal,
        CreateAclsFailureKind::Transport,
        DeliveryStatus::PossiblySent,
    );
    assert_eq!(
        machine.apply(CreateAclsInput::InvalidResponse),
        Err(CreateAclsMachineError::AlreadyCompleted)
    );
}

#[test]
fn malformed_count_or_diagnostic_settles_invalid_without_retry() {
    let mut count = submitted_machine();
    let terminal = effect(
        &mut count,
        CreateAclsInput::BrokerResponded {
            throttle_time_ms: 0,
            results: Vec::new(),
        },
    );
    assert_failure(
        terminal,
        CreateAclsFailureKind::InvalidResponse,
        DeliveryStatus::PossiblySent,
    );

    let mut diagnostic = submitted_machine();
    let terminal = effect(
        &mut diagnostic,
        CreateAclsInput::BrokerResponded {
            throttle_time_ms: 0,
            results: vec![CreateAclResult::BrokerFailed(CreateAclBrokerError::new(
                NonZeroI16::new(17).unwrap_or_else(|| panic!("nonzero")),
                Some("x".repeat(CREATE_ACLS_DIAGNOSTIC_BYTES + 1)),
                false,
            ))],
        },
    );
    assert_failure(
        terminal,
        CreateAclsFailureKind::InvalidResponse,
        DeliveryStatus::PossiblySent,
    );
}

#[test]
fn driver_rejection_is_unsent_and_never_retries() {
    let mut machine = machine(vec![binding("orders")]);
    effect(
        &mut machine,
        CreateAclsInput::Start {
            now: Moment::from_tick(1),
        },
    );
    let terminal = effect(&mut machine, CreateAclsInput::DriverRejected);

    assert_failure(
        terminal,
        CreateAclsFailureKind::DriverRejected,
        DeliveryStatus::NotSent,
    );
    assert_eq!(
        machine.apply(CreateAclsInput::Start {
            now: Moment::from_tick(2)
        }),
        Err(CreateAclsMachineError::AlreadyCompleted)
    );
}

fn submitted_machine() -> CreateAclsMachine {
    let mut machine = machine(vec![binding("orders")]);
    effect(
        &mut machine,
        CreateAclsInput::Start {
            now: Moment::from_tick(1),
        },
    );
    machine
        .apply(CreateAclsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept: {error}"));
    machine
}

fn machine(bindings: Vec<CreateAclBinding>) -> CreateAclsMachine {
    CreateAclsMachine::new(
        OperationId::from_raw(41),
        Deadline::from_tick(100),
        plan(bindings),
    )
}

fn plan(bindings: Vec<CreateAclBinding>) -> CreateAclsPlan {
    CreateAclsPlan::new(bindings).unwrap_or_else(|error| panic!("valid plan: {error}"))
}

fn binding(name: &str) -> CreateAclBinding {
    CreateAclBinding::new(
        2,
        name.to_owned(),
        3,
        "User:alice".to_owned(),
        "*".to_owned(),
        3,
        3,
    )
}

fn broker_error(code: i16, message: &str) -> CreateAclBrokerError {
    CreateAclBrokerError::new(
        NonZeroI16::new(code).unwrap_or_else(|| panic!("nonzero")),
        Some(message.to_owned()),
        false,
    )
}

fn effect(machine: &mut CreateAclsMachine, input: CreateAclsInput) -> CreateAclsEffect {
    machine
        .apply(input)
        .unwrap_or_else(|error| panic!("transition: {error}"))
        .into_effect()
        .unwrap_or_else(|| panic!("expected effect"))
}

fn assert_failure(
    effect: CreateAclsEffect,
    expected_kind: CreateAclsFailureKind,
    expected_delivery: DeliveryStatus,
) {
    let CreateAclsEffect::Complete {
        terminal: CreateAclsTerminal::Failed(failure),
        ..
    } = effect
    else {
        panic!("expected failed terminal");
    };
    assert_eq!(failure.kind(), expected_kind);
    assert_eq!(failure.delivery(), expected_delivery);
}
