//! Scenarios for API-91 lifecycle, group rejection, and terminal assignment.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    ALTER_SHARE_GROUP_OFFSETS_DIAGNOSTIC_BYTES, AlterShareGroupOffset,
    AlterShareGroupOffsetsBrokerError, AlterShareGroupOffsetsEffect,
    AlterShareGroupOffsetsFailureKind, AlterShareGroupOffsetsInput, AlterShareGroupOffsetsMachine,
    AlterShareGroupOffsetsMachineError, AlterShareGroupOffsetsPlan, AlterShareGroupOffsetsState,
    AlterShareGroupOffsetsTerminal, AlterShareGroupOffsetsTransition,
};

#[test]
fn original_deadline_group_and_changes_cross_the_only_submit_effect() {
    let mut machine = machine(20);
    let transition = machine
        .apply(AlterShareGroupOffsetsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start should succeed: {error}"));
    let Some(AlterShareGroupOffsetsEffect::Submit {
        operation_id,
        deadline,
        plan,
    }) = transition.into_effect()
    else {
        panic!("start must submit");
    };

    assert_eq!(operation_id, OperationId::from_raw(91));
    assert_eq!(deadline, Deadline::from_tick(20));
    assert_eq!(plan.group_id(), "payments-share");
    assert_eq!(plan.changes()[0].start_offset(), 42);
    assert_eq!(machine.state(), AlterShareGroupOffsetsState::AwaitingDriver);
}

#[test]
fn exact_group_rejection_is_distinct_and_bounded() {
    let mut machine = submitted_machine();
    let transition = machine
        .apply(AlterShareGroupOffsetsInput::BrokerRejected {
            error: AlterShareGroupOffsetsBrokerError::new(
                19,
                nonzero(-31_777),
                Some("group rejected".to_owned()),
                false,
            ),
        })
        .unwrap_or_else(|error| panic!("broker rejection should settle: {error}"));
    let Some(AlterShareGroupOffsetsEffect::Complete {
        terminal: AlterShareGroupOffsetsTerminal::BrokerRejected(error),
        ..
    }) = transition.into_effect()
    else {
        panic!("exact group rejection must remain distinct");
    };

    assert_eq!(error.throttle_time_ms(), 19);
    assert_eq!(error.code(), -31_777);
    assert_eq!(error.message(), Some("group rejected"));
    assert_eq!(machine.state(), AlterShareGroupOffsetsState::Completed);
    assert_eq!(
        machine.apply(AlterShareGroupOffsetsInput::InvalidResponse),
        Err(AlterShareGroupOffsetsMachineError::AlreadyCompleted)
    );
}

#[test]
fn invalid_group_diagnostic_becomes_invalid_response() {
    for error in [
        AlterShareGroupOffsetsBrokerError::new(0, nonzero(1), None, true),
        AlterShareGroupOffsetsBrokerError::new(
            0,
            nonzero(1),
            Some("x".repeat(ALTER_SHARE_GROUP_OFFSETS_DIAGNOSTIC_BYTES + 1)),
            false,
        ),
    ] {
        let mut machine = submitted_machine();
        assert_failure(
            machine
                .apply(AlterShareGroupOffsetsInput::BrokerRejected { error })
                .unwrap_or_else(|error| panic!("invalid response should settle: {error}")),
            AlterShareGroupOffsetsFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        );
    }
}

#[test]
fn deadlines_and_driver_owned_failures_preserve_certainty_without_retry() {
    let mut elapsed = machine(4);
    assert_failure(
        elapsed
            .apply(AlterShareGroupOffsetsInput::Start {
                now: Moment::from_tick(4),
            })
            .unwrap_or_else(|error| panic!("elapsed start should settle: {error}")),
        AlterShareGroupOffsetsFailureKind::DeadlineElapsed,
        DeliveryStatus::NotSent,
    );

    let mut rejected = machine(20);
    rejected
        .apply(AlterShareGroupOffsetsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start should succeed: {error}"));
    assert_failure(
        rejected
            .apply(AlterShareGroupOffsetsInput::DriverRejected)
            .unwrap_or_else(|error| panic!("driver rejection should settle: {error}")),
        AlterShareGroupOffsetsFailureKind::DriverRejected,
        DeliveryStatus::NotSent,
    );

    for (input, kind, delivery) in [
        (
            AlterShareGroupOffsetsInput::DriverDeadlineElapsed {
                delivery: DeliveryStatus::PossiblySent,
            },
            AlterShareGroupOffsetsFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            AlterShareGroupOffsetsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::NotSent,
            },
            AlterShareGroupOffsetsFailureKind::Compatibility,
            DeliveryStatus::NotSent,
        ),
        (
            AlterShareGroupOffsetsInput::TransportFailed {
                delivery: DeliveryStatus::PossiblySent,
            },
            AlterShareGroupOffsetsFailureKind::Transport,
            DeliveryStatus::PossiblySent,
        ),
        (
            AlterShareGroupOffsetsInput::ResponseTooLarge,
            AlterShareGroupOffsetsFailureKind::ResponseTooLarge,
            DeliveryStatus::PossiblySent,
        ),
    ] {
        let mut machine = submitted_machine();
        assert_failure(
            machine
                .apply(input)
                .unwrap_or_else(|error| panic!("submitted failure should settle: {error}")),
            kind,
            delivery,
        );
    }
}

#[test]
fn lifecycle_rejects_duplicate_start_and_pre_submission_response() {
    let mut machine = machine(20);
    machine
        .apply(AlterShareGroupOffsetsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start should succeed: {error}"));
    assert_eq!(
        machine.apply(AlterShareGroupOffsetsInput::Start {
            now: Moment::from_tick(2),
        }),
        Err(AlterShareGroupOffsetsMachineError::InvalidState)
    );
    assert_eq!(
        machine.apply(AlterShareGroupOffsetsInput::InvalidResponse),
        Err(AlterShareGroupOffsetsMachineError::InvalidState)
    );
}

fn machine(deadline: u64) -> AlterShareGroupOffsetsMachine {
    let plan = AlterShareGroupOffsetsPlan::new(
        "payments-share".to_owned(),
        vec![
            AlterShareGroupOffset::new("orders".to_owned(), 1, 42),
            AlterShareGroupOffset::new("audit".to_owned(), 0, 7),
        ],
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"));
    AlterShareGroupOffsetsMachine::new(
        OperationId::from_raw(91),
        Deadline::from_tick(deadline),
        plan,
    )
}

fn submitted_machine() -> AlterShareGroupOffsetsMachine {
    let mut machine = machine(20);
    machine
        .apply(AlterShareGroupOffsetsInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(AlterShareGroupOffsetsInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("submit machine: {error}"));
    machine
}

fn assert_failure(
    transition: AlterShareGroupOffsetsTransition,
    kind: AlterShareGroupOffsetsFailureKind,
    delivery: DeliveryStatus,
) {
    let Some(AlterShareGroupOffsetsEffect::Complete {
        terminal: AlterShareGroupOffsetsTerminal::Failed(failure),
        ..
    }) = transition.into_effect()
    else {
        panic!("expected failed terminal");
    };
    assert_eq!(failure.kind(), kind);
    assert_eq!(failure.delivery(), delivery);
}

fn nonzero(code: i16) -> NonZeroI16 {
    NonZeroI16::new(code).unwrap_or_else(|| panic!("test code must be nonzero"))
}
