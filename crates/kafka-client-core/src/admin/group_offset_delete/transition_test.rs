//! Scenarios for offset deletion lifecycle and terminal single assignment.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    DeleteConsumerGroupOffsetBrokerError, DeleteConsumerGroupOffsetOutcome,
    DeleteConsumerGroupOffsetTarget, DeleteConsumerGroupOffsetsBatch,
    DeleteConsumerGroupOffsetsEffect, DeleteConsumerGroupOffsetsFailureKind,
    DeleteConsumerGroupOffsetsInput, DeleteConsumerGroupOffsetsMachine,
    DeleteConsumerGroupOffsetsMachineError, DeleteConsumerGroupOffsetsPlan,
    DeleteConsumerGroupOffsetsState, DeleteConsumerGroupOffsetsTerminal,
    DeleteConsumerGroupOffsetsTransition,
};

#[test]
fn original_deadline_group_and_targets_cross_the_only_submit_effect() {
    let mut machine = machine(20);
    let transition = machine
        .apply(DeleteConsumerGroupOffsetsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start should succeed: {error}"));
    let Some(DeleteConsumerGroupOffsetsEffect::Submit {
        operation_id,
        deadline,
        plan,
    }) = transition.into_effect()
    else {
        panic!("start must submit");
    };

    assert_eq!(operation_id, OperationId::from_raw(23));
    assert_eq!(deadline, Deadline::from_tick(20));
    assert_eq!(plan.group_id(), "payments");
    assert_eq!(plan.targets()[0], target("orders", 2));
    assert_eq!(plan.targets()[1], target("audit", 0));
}

#[test]
fn ordered_mixed_terminal_is_single_assignment_and_lossless() {
    let mut machine = submitted_machine();
    let code = nonzero(-32_123);
    let batch = DeleteConsumerGroupOffsetsBatch::new(
        73,
        vec![
            DeleteConsumerGroupOffsetOutcome::deleted("orders".to_owned(), 2),
            DeleteConsumerGroupOffsetOutcome::failed(
                "audit".to_owned(),
                0,
                DeleteConsumerGroupOffsetBrokerError::new(code),
            ),
        ],
    );
    let transition = machine
        .apply(DeleteConsumerGroupOffsetsInput::BrokerResponded { batch })
        .unwrap_or_else(|error| panic!("response should settle: {error}"));
    let Some(DeleteConsumerGroupOffsetsEffect::Complete {
        terminal: DeleteConsumerGroupOffsetsTerminal::Deleted(batch),
        ..
    }) = transition.into_effect()
    else {
        panic!("response must complete with ordered outcomes");
    };

    assert_eq!(batch.throttle_time_ms(), 73);
    assert_eq!(batch.outcomes()[0].topic(), "orders");
    assert_eq!(batch.outcomes()[1].topic(), "audit");
    assert_eq!(machine.state(), DeleteConsumerGroupOffsetsState::Completed);
    assert_eq!(
        machine.apply(DeleteConsumerGroupOffsetsInput::InvalidResponse),
        Err(DeleteConsumerGroupOffsetsMachineError::AlreadyCompleted)
    );
}

#[test]
fn top_level_group_error_is_separate_exact_and_terminal() {
    let mut machine = submitted_machine();
    let code = nonzero(-31_777);
    let transition = machine
        .apply(DeleteConsumerGroupOffsetsInput::BrokerRejected { code })
        .unwrap_or_else(|error| panic!("group error should settle: {error}"));

    assert_failure(
        transition,
        DeleteConsumerGroupOffsetsFailureKind::Broker(code),
        DeliveryStatus::PossiblySent,
    );
}

#[test]
fn count_or_order_mismatch_settles_invalid_once() {
    for outcomes in [
        vec![DeleteConsumerGroupOffsetOutcome::deleted(
            "orders".to_owned(),
            2,
        )],
        vec![
            DeleteConsumerGroupOffsetOutcome::deleted("audit".to_owned(), 0),
            DeleteConsumerGroupOffsetOutcome::deleted("orders".to_owned(), 2),
        ],
        vec![
            DeleteConsumerGroupOffsetOutcome::deleted("orders".to_owned(), 1),
            DeleteConsumerGroupOffsetOutcome::deleted("audit".to_owned(), 0),
        ],
    ] {
        let mut machine = submitted_machine();
        let transition = machine
            .apply(DeleteConsumerGroupOffsetsInput::BrokerResponded {
                batch: DeleteConsumerGroupOffsetsBatch::new(0, outcomes),
            })
            .unwrap_or_else(|error| panic!("malformed response should settle: {error}"));
        assert_failure(
            transition,
            DeleteConsumerGroupOffsetsFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        );
        assert_eq!(machine.state(), DeleteConsumerGroupOffsetsState::Completed);
    }
}

#[test]
fn deadlines_and_driver_ownership_preserve_certainty_without_retry() {
    let mut elapsed = machine(4);
    assert_failure(
        elapsed
            .apply(DeleteConsumerGroupOffsetsInput::Start {
                now: Moment::from_tick(4),
            })
            .unwrap_or_else(|error| panic!("elapsed start should settle: {error}")),
        DeleteConsumerGroupOffsetsFailureKind::DeadlineElapsed,
        DeliveryStatus::NotSent,
    );

    let mut rejected = machine(20);
    rejected
        .apply(DeleteConsumerGroupOffsetsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start should succeed: {error}"));
    assert_failure(
        rejected
            .apply(DeleteConsumerGroupOffsetsInput::DriverRejected)
            .unwrap_or_else(|error| panic!("driver rejection should settle: {error}")),
        DeleteConsumerGroupOffsetsFailureKind::DriverRejected,
        DeliveryStatus::NotSent,
    );

    let mut waiting = machine(20);
    waiting
        .apply(DeleteConsumerGroupOffsetsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start should succeed: {error}"));
    assert_failure(
        waiting
            .apply(DeleteConsumerGroupOffsetsInput::DeadlineElapsed)
            .unwrap_or_else(|error| panic!("queued deadline should settle: {error}")),
        DeleteConsumerGroupOffsetsFailureKind::DeadlineElapsed,
        DeliveryStatus::NotSent,
    );

    for (input, kind, delivery) in [
        (
            DeleteConsumerGroupOffsetsInput::DriverDeadlineElapsed {
                delivery: DeliveryStatus::PossiblySent,
            },
            DeleteConsumerGroupOffsetsFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            DeleteConsumerGroupOffsetsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::NotSent,
            },
            DeleteConsumerGroupOffsetsFailureKind::Compatibility,
            DeliveryStatus::NotSent,
        ),
        (
            DeleteConsumerGroupOffsetsInput::TransportFailed {
                delivery: DeliveryStatus::PossiblySent,
            },
            DeleteConsumerGroupOffsetsFailureKind::Transport,
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

fn machine(deadline: u64) -> DeleteConsumerGroupOffsetsMachine {
    let plan = DeleteConsumerGroupOffsetsPlan::new(
        "payments".to_owned(),
        vec![target("orders", 2), target("audit", 0)],
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"));
    DeleteConsumerGroupOffsetsMachine::new(
        OperationId::from_raw(23),
        Deadline::from_tick(deadline),
        plan,
    )
}

fn submitted_machine() -> DeleteConsumerGroupOffsetsMachine {
    let mut machine = machine(20);
    machine
        .apply(DeleteConsumerGroupOffsetsInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(DeleteConsumerGroupOffsetsInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("submit machine: {error}"));
    machine
}

fn target(topic: &str, partition: i32) -> DeleteConsumerGroupOffsetTarget {
    DeleteConsumerGroupOffsetTarget::new(topic.to_owned(), partition)
}

fn nonzero(code: i16) -> NonZeroI16 {
    NonZeroI16::new(code).unwrap_or_else(|| panic!("test code must be nonzero"))
}

fn assert_failure(
    transition: DeleteConsumerGroupOffsetsTransition,
    kind: DeleteConsumerGroupOffsetsFailureKind,
    delivery: DeliveryStatus,
) {
    let Some(DeleteConsumerGroupOffsetsEffect::Complete {
        terminal: DeleteConsumerGroupOffsetsTerminal::Failed(failure),
        ..
    }) = transition.into_effect()
    else {
        panic!("expected failed terminal");
    };
    assert_eq!(failure.kind(), kind);
    assert_eq!(failure.delivery(), delivery);
}
