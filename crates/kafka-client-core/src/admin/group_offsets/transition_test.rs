//! Scenarios for group-offset lifecycle and terminal single assignment.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    GroupOffsetBrokerError, GroupOffsetDescription, GroupOffsetOutcome,
    ListConsumerGroupOffsetsBatch, ListConsumerGroupOffsetsEffect,
    ListConsumerGroupOffsetsFailureKind, ListConsumerGroupOffsetsInput,
    ListConsumerGroupOffsetsMachine, ListConsumerGroupOffsetsMachineError,
    ListConsumerGroupOffsetsPlan, ListConsumerGroupOffsetsState, ListConsumerGroupOffsetsTerminal,
    ListConsumerGroupOffsetsTransition,
};

#[test]
fn original_deadline_group_and_stability_cross_the_only_submit_effect() {
    let mut machine = machine(20);
    let transition = machine
        .apply(ListConsumerGroupOffsetsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start should succeed: {error}"));
    let Some(ListConsumerGroupOffsetsEffect::Submit {
        operation_id,
        deadline,
        plan,
    }) = transition.into_effect()
    else {
        panic!("start must submit");
    };

    assert_eq!(operation_id, OperationId::from_raw(19));
    assert_eq!(deadline, Deadline::from_tick(20));
    assert_eq!(plan.group_id(), "payments");
    assert!(plan.require_stable());
    assert_eq!(
        machine.state(),
        ListConsumerGroupOffsetsState::AwaitingDriver
    );
}

#[test]
fn ordered_mixed_terminal_is_single_assignment_and_lossless() {
    let mut machine = submitted_machine();
    let code = NonZeroI16::new(-32_123).unwrap_or_else(|| panic!("code is nonzero"));
    let outcomes = vec![
        described("audit", 0, None, None, None),
        GroupOffsetOutcome::failed("audit".to_owned(), 2, GroupOffsetBrokerError::new(code)),
        described("orders", 1, Some(42), Some(7), Some(String::new())),
    ];
    let transition = machine
        .apply(ListConsumerGroupOffsetsInput::BrokerResponded {
            batch: ListConsumerGroupOffsetsBatch::new(73, outcomes),
        })
        .unwrap_or_else(|error| panic!("response should settle: {error}"));
    let Some(ListConsumerGroupOffsetsEffect::Complete {
        terminal: ListConsumerGroupOffsetsTerminal::Offsets(batch),
        ..
    }) = transition.into_effect()
    else {
        panic!("response must complete with offsets");
    };

    assert_eq!(batch.throttle_time_ms(), 73);
    assert_eq!(batch.outcomes().len(), 3);
    assert_eq!(batch.outcomes()[2].topic(), "orders");
    assert_eq!(batch.outcomes()[2].partition(), 1);
    assert_eq!(machine.state(), ListConsumerGroupOffsetsState::Completed);
    assert_eq!(
        machine.apply(ListConsumerGroupOffsetsInput::InvalidResponse),
        Err(ListConsumerGroupOffsetsMachineError::AlreadyCompleted)
    );
}

#[test]
fn malformed_or_duplicate_partition_facts_settle_invalid_once() {
    let malformed = [
        vec![described("", 0, None, None, None)],
        vec![described("audit", -1, None, None, None)],
        vec![described("audit", 0, Some(-1), None, None)],
        vec![described("audit", 0, None, Some(-1), None)],
        vec![
            described("orders", 0, None, None, None),
            described("audit", 0, None, None, None),
        ],
        vec![
            described("évents", 0, None, None, None),
            described("z-events", 0, None, None, None),
        ],
        vec![
            described("audit", 2, None, None, None),
            described("audit", 1, None, None, None),
        ],
        vec![
            described("audit", 1, None, None, None),
            GroupOffsetOutcome::failed(
                "audit".to_owned(),
                1,
                GroupOffsetBrokerError::new(nonzero(7)),
            ),
        ],
    ];

    for outcomes in malformed {
        let mut machine = submitted_machine();
        let terminal = machine
            .apply(ListConsumerGroupOffsetsInput::BrokerResponded {
                batch: ListConsumerGroupOffsetsBatch::new(0, outcomes),
            })
            .unwrap_or_else(|error| panic!("malformed response should settle: {error}"));
        assert_failure(
            terminal,
            ListConsumerGroupOffsetsFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        );
        assert_eq!(machine.state(), ListConsumerGroupOffsetsState::Completed);
    }
}

#[test]
fn empty_result_is_a_valid_completed_group_query() {
    let mut machine = submitted_machine();
    let transition = machine
        .apply(ListConsumerGroupOffsetsInput::BrokerResponded {
            batch: ListConsumerGroupOffsetsBatch::new(0, Vec::new()),
        })
        .unwrap_or_else(|error| panic!("empty group result should settle: {error}"));
    assert!(matches!(
        transition.into_effect(),
        Some(ListConsumerGroupOffsetsEffect::Complete {
            terminal: ListConsumerGroupOffsetsTerminal::Offsets(batch),
            ..
        }) if batch.outcomes().is_empty()
    ));
}

#[test]
fn top_level_group_error_is_exact_and_terminal() {
    let mut machine = submitted_machine();
    let code = nonzero(-31_777);
    let transition = machine
        .apply(ListConsumerGroupOffsetsInput::BrokerRejected { code })
        .unwrap_or_else(|error| panic!("group error should settle: {error}"));

    assert_failure(
        transition,
        ListConsumerGroupOffsetsFailureKind::Broker(code),
        DeliveryStatus::PossiblySent,
    );
}

#[test]
fn deadlines_and_driver_ownership_preserve_certainty_without_retry() {
    let mut elapsed = machine(4);
    assert_failure(
        elapsed
            .apply(ListConsumerGroupOffsetsInput::Start {
                now: Moment::from_tick(4),
            })
            .unwrap_or_else(|error| panic!("elapsed start should settle: {error}")),
        ListConsumerGroupOffsetsFailureKind::DeadlineElapsed,
        DeliveryStatus::NotSent,
    );

    let mut rejected = machine(20);
    rejected
        .apply(ListConsumerGroupOffsetsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start should succeed: {error}"));
    assert_failure(
        rejected
            .apply(ListConsumerGroupOffsetsInput::DriverRejected)
            .unwrap_or_else(|error| panic!("driver rejection should settle: {error}")),
        ListConsumerGroupOffsetsFailureKind::DriverRejected,
        DeliveryStatus::NotSent,
    );

    for (input, kind, delivery) in [
        (
            ListConsumerGroupOffsetsInput::DriverDeadlineElapsed {
                delivery: DeliveryStatus::PossiblySent,
            },
            ListConsumerGroupOffsetsFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            ListConsumerGroupOffsetsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::NotSent,
            },
            ListConsumerGroupOffsetsFailureKind::Compatibility,
            DeliveryStatus::NotSent,
        ),
        (
            ListConsumerGroupOffsetsInput::TransportFailed {
                delivery: DeliveryStatus::PossiblySent,
            },
            ListConsumerGroupOffsetsFailureKind::Transport,
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

fn machine(deadline: u64) -> ListConsumerGroupOffsetsMachine {
    let plan = ListConsumerGroupOffsetsPlan::new("payments".to_owned(), true)
        .unwrap_or_else(|error| panic!("valid plan: {error}"));
    ListConsumerGroupOffsetsMachine::new(
        OperationId::from_raw(19),
        Deadline::from_tick(deadline),
        plan,
    )
}

fn submitted_machine() -> ListConsumerGroupOffsetsMachine {
    let mut machine = machine(20);
    machine
        .apply(ListConsumerGroupOffsetsInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(ListConsumerGroupOffsetsInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("submit machine: {error}"));
    machine
}

fn described(
    topic: &str,
    partition: i32,
    offset: Option<i64>,
    leader_epoch: Option<i32>,
    metadata: Option<String>,
) -> GroupOffsetOutcome {
    GroupOffsetOutcome::described(
        topic.to_owned(),
        partition,
        GroupOffsetDescription::new(offset, leader_epoch, metadata),
    )
}

fn nonzero(code: i16) -> NonZeroI16 {
    NonZeroI16::new(code).unwrap_or_else(|| panic!("test code must be nonzero"))
}

fn assert_failure(
    transition: ListConsumerGroupOffsetsTransition,
    kind: ListConsumerGroupOffsetsFailureKind,
    delivery: DeliveryStatus,
) {
    let Some(ListConsumerGroupOffsetsEffect::Complete {
        terminal: ListConsumerGroupOffsetsTerminal::Failed(failure),
        ..
    }) = transition.into_effect()
    else {
        panic!("expected failed terminal");
    };
    assert_eq!(failure.kind(), kind);
    assert_eq!(failure.delivery(), delivery);
}
