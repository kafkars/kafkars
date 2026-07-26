//! Scenarios for offset alteration lifecycle and terminal single assignment.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    AlterConsumerGroupOffsetBrokerError, AlterConsumerGroupOffsetOutcome,
    AlterConsumerGroupOffsetTarget, AlterConsumerGroupOffsetsBatch,
    AlterConsumerGroupOffsetsEffect, AlterConsumerGroupOffsetsFailureKind,
    AlterConsumerGroupOffsetsInput, AlterConsumerGroupOffsetsMachine,
    AlterConsumerGroupOffsetsMachineError, AlterConsumerGroupOffsetsPlan,
    AlterConsumerGroupOffsetsState, AlterConsumerGroupOffsetsTerminal,
    AlterConsumerGroupOffsetsTransition,
};

#[test]
fn original_deadline_and_exact_plan_cross_the_only_submit_effect() {
    let mut machine = machine(20);
    let transition = machine
        .apply(AlterConsumerGroupOffsetsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start should succeed: {error}"));
    let Some(AlterConsumerGroupOffsetsEffect::Submit {
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
    assert_eq!(plan.targets()[0].next_offset(), 91);
    assert_eq!(plan.targets()[0].leader_epoch(), Some(7));
    assert_eq!(plan.targets()[0].metadata(), Some(""));
    assert_eq!(plan.targets()[1].metadata(), None);
}

#[test]
fn ordered_mixed_terminal_is_single_assignment_and_lossless() {
    let mut machine = submitted_machine();
    let code = nonzero(-32_123);
    let batch = AlterConsumerGroupOffsetsBatch::new(
        73,
        vec![
            AlterConsumerGroupOffsetOutcome::altered("orders".to_owned(), 2),
            AlterConsumerGroupOffsetOutcome::failed(
                "audit".to_owned(),
                0,
                AlterConsumerGroupOffsetBrokerError::new(code),
            ),
        ],
    );
    let transition = machine
        .apply(AlterConsumerGroupOffsetsInput::BrokerResponded { batch })
        .unwrap_or_else(|error| panic!("response should settle: {error}"));
    let Some(AlterConsumerGroupOffsetsEffect::Complete {
        terminal: AlterConsumerGroupOffsetsTerminal::Altered(batch),
        ..
    }) = transition.into_effect()
    else {
        panic!("response must complete with ordered outcomes");
    };

    assert_eq!(batch.throttle_time_ms(), 73);
    assert_eq!(batch.outcomes()[0].topic(), "orders");
    assert_eq!(batch.outcomes()[1].topic(), "audit");
    assert_eq!(machine.state(), AlterConsumerGroupOffsetsState::Completed);
    assert_eq!(
        machine.apply(AlterConsumerGroupOffsetsInput::InvalidResponse),
        Err(AlterConsumerGroupOffsetsMachineError::AlreadyCompleted)
    );
}

#[test]
fn count_or_order_mismatch_settles_invalid_once() {
    for outcomes in [
        vec![AlterConsumerGroupOffsetOutcome::altered(
            "orders".to_owned(),
            2,
        )],
        vec![
            AlterConsumerGroupOffsetOutcome::altered("audit".to_owned(), 0),
            AlterConsumerGroupOffsetOutcome::altered("orders".to_owned(), 2),
        ],
        vec![
            AlterConsumerGroupOffsetOutcome::altered("orders".to_owned(), 1),
            AlterConsumerGroupOffsetOutcome::altered("audit".to_owned(), 0),
        ],
    ] {
        let mut machine = submitted_machine();
        let transition = machine
            .apply(AlterConsumerGroupOffsetsInput::BrokerResponded {
                batch: AlterConsumerGroupOffsetsBatch::new(0, outcomes),
            })
            .unwrap_or_else(|error| panic!("malformed response should settle: {error}"));
        assert_failure(
            transition,
            AlterConsumerGroupOffsetsFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        );
        assert_eq!(machine.state(), AlterConsumerGroupOffsetsState::Completed);
    }
}

#[test]
fn deadlines_and_driver_ownership_preserve_certainty_without_retry() {
    let mut elapsed = machine(4);
    assert_failure(
        elapsed
            .apply(AlterConsumerGroupOffsetsInput::Start {
                now: Moment::from_tick(4),
            })
            .unwrap_or_else(|error| panic!("elapsed start should settle: {error}")),
        AlterConsumerGroupOffsetsFailureKind::DeadlineElapsed,
        DeliveryStatus::NotSent,
    );

    let mut rejected = awaiting_machine();
    assert_failure(
        rejected
            .apply(AlterConsumerGroupOffsetsInput::DriverRejected)
            .unwrap_or_else(|error| panic!("driver rejection should settle: {error}")),
        AlterConsumerGroupOffsetsFailureKind::DriverRejected,
        DeliveryStatus::NotSent,
    );

    let mut waiting = awaiting_machine();
    assert_failure(
        waiting
            .apply(AlterConsumerGroupOffsetsInput::DeadlineElapsed)
            .unwrap_or_else(|error| panic!("queued deadline should settle: {error}")),
        AlterConsumerGroupOffsetsFailureKind::DeadlineElapsed,
        DeliveryStatus::NotSent,
    );

    for (input, kind, delivery) in [
        (
            AlterConsumerGroupOffsetsInput::DriverDeadlineElapsed {
                delivery: DeliveryStatus::PossiblySent,
            },
            AlterConsumerGroupOffsetsFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            AlterConsumerGroupOffsetsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::NotSent,
            },
            AlterConsumerGroupOffsetsFailureKind::Compatibility,
            DeliveryStatus::NotSent,
        ),
        (
            AlterConsumerGroupOffsetsInput::TransportFailed {
                delivery: DeliveryStatus::PossiblySent,
            },
            AlterConsumerGroupOffsetsFailureKind::Transport,
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

fn machine(deadline: u64) -> AlterConsumerGroupOffsetsMachine {
    let plan = AlterConsumerGroupOffsetsPlan::new(
        "payments".to_owned(),
        vec![
            target("orders", 2, 91, Some(7), Some(String::new())),
            target("audit", 0, 13, None, None),
        ],
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"));
    AlterConsumerGroupOffsetsMachine::new(
        OperationId::from_raw(23),
        Deadline::from_tick(deadline),
        plan,
    )
}

fn awaiting_machine() -> AlterConsumerGroupOffsetsMachine {
    let mut machine = machine(20);
    machine
        .apply(AlterConsumerGroupOffsetsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start machine: {error}"));
    machine
}

fn submitted_machine() -> AlterConsumerGroupOffsetsMachine {
    let mut machine = awaiting_machine();
    machine
        .apply(AlterConsumerGroupOffsetsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("submit machine: {error}"));
    machine
}

fn target(
    topic: &str,
    partition: i32,
    next_offset: i64,
    leader_epoch: Option<i32>,
    metadata: Option<String>,
) -> AlterConsumerGroupOffsetTarget {
    AlterConsumerGroupOffsetTarget::new(
        topic.to_owned(),
        partition,
        next_offset,
        leader_epoch,
        metadata,
    )
}

fn nonzero(code: i16) -> NonZeroI16 {
    NonZeroI16::new(code).unwrap_or_else(|| panic!("test code must be nonzero"))
}

fn assert_failure(
    transition: AlterConsumerGroupOffsetsTransition,
    kind: AlterConsumerGroupOffsetsFailureKind,
    delivery: DeliveryStatus,
) {
    let Some(AlterConsumerGroupOffsetsEffect::Complete {
        terminal: AlterConsumerGroupOffsetsTerminal::Failed(failure),
        ..
    }) = transition.into_effect()
    else {
        panic!("expected failed terminal");
    };
    assert_eq!(failure.kind(), kind);
    assert_eq!(failure.delivery(), delivery);
}
