//! Sequential exact-broker and partial-result transition scenarios.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    AdminDescribeLogDirsBrokerError, AdminDescribeLogDirsBrokerOutcome,
    AdminDescribeLogDirsBrokerResult, AdminDescribeLogDirsEffect, AdminDescribeLogDirsFailureKind,
    AdminDescribeLogDirsInput, AdminDescribeLogDirsMachine, AdminDescribeLogDirsMachineError,
    AdminDescribeLogDirsPlan, AdminDescribeLogDirsTerminal,
};

#[test]
fn exact_broker_calls_reuse_original_identity_deadline_and_preserve_caller_order() {
    let mut machine = machine(vec![8, 3]);
    let first = effect(
        &mut machine,
        AdminDescribeLogDirsInput::Start {
            now: Moment::from_tick(1),
        },
    );
    assert_submit(first, 8);
    machine
        .apply(AdminDescribeLogDirsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept first: {error}"));
    let second = effect(
        &mut machine,
        AdminDescribeLogDirsInput::BrokerResponded {
            throttle_time_ms: 4,
            outcome: AdminDescribeLogDirsBrokerOutcome::described(8, Vec::new()),
        },
    );
    assert_submit(second, 3);
    machine
        .apply(AdminDescribeLogDirsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept second: {error}"));
    let error = AdminDescribeLogDirsBrokerError::new(
        NonZeroI16::new(56).unwrap_or_else(|| panic!("nonzero")),
    );
    let terminal = effect(
        &mut machine,
        AdminDescribeLogDirsInput::BrokerResponded {
            throttle_time_ms: 11,
            outcome: AdminDescribeLogDirsBrokerOutcome::broker_failed(3, error),
        },
    );
    let AdminDescribeLogDirsEffect::Complete {
        operation_id,
        terminal: AdminDescribeLogDirsTerminal::Described(batch),
    } = terminal
    else {
        panic!("expected described terminal");
    };
    assert_eq!(operation_id, OperationId::from_raw(23));
    assert_eq!(batch.throttle_time_ms(), 11);
    assert_eq!(
        batch
            .outcomes()
            .iter()
            .map(AdminDescribeLogDirsBrokerOutcome::broker_id)
            .collect::<Vec<_>>(),
        vec![8, 3]
    );
    assert!(matches!(
        batch.outcomes()[1].result(),
        AdminDescribeLogDirsBrokerResult::BrokerFailed(value) if value.code() == 56
    ));
}

#[test]
fn later_transport_failure_preserves_completed_current_and_unattempted_brokers() {
    let mut machine = machine(vec![4, 9, 2]);
    effect(
        &mut machine,
        AdminDescribeLogDirsInput::Start {
            now: Moment::from_tick(1),
        },
    );
    machine
        .apply(AdminDescribeLogDirsInput::DriverAccepted)
        .and_then(|_| {
            machine.apply(AdminDescribeLogDirsInput::BrokerResponded {
                throttle_time_ms: 7,
                outcome: AdminDescribeLogDirsBrokerOutcome::described(4, Vec::new()),
            })
        })
        .and_then(|_| machine.apply(AdminDescribeLogDirsInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("complete first and accept second: {error}"));

    let terminal = effect(
        &mut machine,
        AdminDescribeLogDirsInput::TransportFailed {
            delivery: DeliveryStatus::PossiblySent,
        },
    );
    let AdminDescribeLogDirsEffect::Complete {
        terminal: AdminDescribeLogDirsTerminal::Described(batch),
        ..
    } = terminal
    else {
        panic!("expected partial described terminal");
    };
    assert_eq!(batch.throttle_time_ms(), 7);
    assert!(matches!(
        batch.outcomes()[0].result(),
        AdminDescribeLogDirsBrokerResult::Described(_)
    ));
    let AdminDescribeLogDirsBrokerResult::OperationFailed(current) = batch.outcomes()[1].result()
    else {
        panic!("current broker failure missing");
    };
    assert_eq!(current.kind(), AdminDescribeLogDirsFailureKind::Transport);
    assert_eq!(current.delivery(), DeliveryStatus::PossiblySent);
    let AdminDescribeLogDirsBrokerResult::OperationFailed(unattempted) =
        batch.outcomes()[2].result()
    else {
        panic!("unattempted broker failure missing");
    };
    assert_eq!(
        unattempted.kind(),
        AdminDescribeLogDirsFailureKind::NotAttempted
    );
    assert_eq!(unattempted.delivery(), DeliveryStatus::NotSent);
}

#[test]
fn mismatched_broker_settles_invalid_once() {
    let mut machine = machine(vec![6]);
    effect(
        &mut machine,
        AdminDescribeLogDirsInput::Start {
            now: Moment::from_tick(1),
        },
    );
    machine
        .apply(AdminDescribeLogDirsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept: {error}"));
    let terminal = effect(
        &mut machine,
        AdminDescribeLogDirsInput::BrokerResponded {
            throttle_time_ms: 0,
            outcome: AdminDescribeLogDirsBrokerOutcome::described(7, Vec::new()),
        },
    );
    let AdminDescribeLogDirsEffect::Complete {
        terminal: AdminDescribeLogDirsTerminal::Described(batch),
        ..
    } = terminal
    else {
        panic!("expected invalid terminal");
    };
    let AdminDescribeLogDirsBrokerResult::OperationFailed(failure) = batch.outcomes()[0].result()
    else {
        panic!("expected operation failure");
    };
    assert_eq!(
        failure.kind(),
        AdminDescribeLogDirsFailureKind::InvalidResponse
    );
    assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
    assert_eq!(
        machine.apply(AdminDescribeLogDirsInput::InvalidResponse),
        Err(AdminDescribeLogDirsMachineError::AlreadyCompleted)
    );
}

fn machine(broker_ids: Vec<i32>) -> AdminDescribeLogDirsMachine {
    AdminDescribeLogDirsMachine::new(
        OperationId::from_raw(23),
        Deadline::from_tick(100),
        AdminDescribeLogDirsPlan::new(broker_ids)
            .unwrap_or_else(|error| panic!("valid plan: {error}")),
    )
}

fn effect(
    machine: &mut AdminDescribeLogDirsMachine,
    input: AdminDescribeLogDirsInput,
) -> AdminDescribeLogDirsEffect {
    machine
        .apply(input)
        .unwrap_or_else(|error| panic!("transition: {error}"))
        .into_effect()
        .unwrap_or_else(|| panic!("expected effect"))
}

fn assert_submit(effect: AdminDescribeLogDirsEffect, broker_id: i32) {
    let AdminDescribeLogDirsEffect::Submit {
        operation_id,
        deadline,
        broker_id: actual,
    } = effect
    else {
        panic!("expected submit");
    };
    assert_eq!(operation_id, OperationId::from_raw(23));
    assert_eq!(deadline, Deadline::from_tick(100));
    assert_eq!(actual, broker_id);
}
