//! Scenarios for member-removal lifecycle and terminal single assignment.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    ConsumerGroupMemberRemoval, ConsumerGroupMemberRemovalBrokerError,
    ConsumerGroupMemberRemovalOutcome, RemoveConsumerGroupMembersBatch,
    RemoveConsumerGroupMembersEffect, RemoveConsumerGroupMembersFailureKind,
    RemoveConsumerGroupMembersInput, RemoveConsumerGroupMembersMachine,
    RemoveConsumerGroupMembersMachineError, RemoveConsumerGroupMembersPlan,
    RemoveConsumerGroupMembersState, RemoveConsumerGroupMembersTerminal,
    RemoveConsumerGroupMembersTransition,
};

#[test]
fn original_deadline_group_members_and_reason_cross_the_only_submit_effect() {
    let mut machine = machine(20);
    let transition = machine
        .apply(RemoveConsumerGroupMembersInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start should succeed: {error}"));
    let Some(RemoveConsumerGroupMembersEffect::Submit {
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
    assert_eq!(plan.members()[0].group_instance_id(), "instance-b");
    assert_eq!(plan.reason(), Some("maintenance"));
}

#[test]
fn ordered_mixed_terminal_is_single_assignment_and_lossless() {
    let mut machine = submitted_machine();
    let code = nonzero(-32_123);
    let batch = RemoveConsumerGroupMembersBatch::new(
        73,
        vec![
            ConsumerGroupMemberRemovalOutcome::removed("instance-b".to_owned()),
            ConsumerGroupMemberRemovalOutcome::failed(
                "instance-a".to_owned(),
                ConsumerGroupMemberRemovalBrokerError::new(code),
            ),
        ],
    );
    let transition = machine
        .apply(RemoveConsumerGroupMembersInput::BrokerResponded { batch })
        .unwrap_or_else(|error| panic!("response should settle: {error}"));
    let Some(RemoveConsumerGroupMembersEffect::Complete {
        terminal: RemoveConsumerGroupMembersTerminal::Removed(batch),
        ..
    }) = transition.into_effect()
    else {
        panic!("response must complete with ordered outcomes");
    };

    assert_eq!(batch.throttle_time_ms(), 73);
    assert_eq!(machine.state(), RemoveConsumerGroupMembersState::Completed);
    assert_eq!(
        machine.apply(RemoveConsumerGroupMembersInput::InvalidResponse),
        Err(RemoveConsumerGroupMembersMachineError::AlreadyCompleted)
    );
}

#[test]
fn response_identity_mismatch_and_top_level_error_are_exact_terminals() {
    let mut mismatched = submitted_machine();
    assert_failure(
        mismatched
            .apply(RemoveConsumerGroupMembersInput::BrokerResponded {
                batch: RemoveConsumerGroupMembersBatch::new(
                    0,
                    vec![
                        ConsumerGroupMemberRemovalOutcome::removed("instance-a".to_owned()),
                        ConsumerGroupMemberRemovalOutcome::removed("instance-b".to_owned()),
                    ],
                ),
            })
            .unwrap_or_else(|error| panic!("malformed response should settle: {error}")),
        RemoveConsumerGroupMembersFailureKind::InvalidResponse,
        DeliveryStatus::PossiblySent,
    );

    let mut rejected = submitted_machine();
    let code = nonzero(-31_777);
    assert_failure(
        rejected
            .apply(RemoveConsumerGroupMembersInput::BrokerRejected { code })
            .unwrap_or_else(|error| panic!("group error should settle: {error}")),
        RemoveConsumerGroupMembersFailureKind::Broker(code),
        DeliveryStatus::PossiblySent,
    );
}

#[test]
fn deadlines_and_driver_ownership_preserve_certainty_without_retry() {
    let mut elapsed = machine(4);
    assert_failure(
        elapsed
            .apply(RemoveConsumerGroupMembersInput::Start {
                now: Moment::from_tick(4),
            })
            .unwrap_or_else(|error| panic!("elapsed start should settle: {error}")),
        RemoveConsumerGroupMembersFailureKind::DeadlineElapsed,
        DeliveryStatus::NotSent,
    );

    let mut waiting = machine(20);
    waiting
        .apply(RemoveConsumerGroupMembersInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start should succeed: {error}"));
    assert_failure(
        waiting
            .apply(RemoveConsumerGroupMembersInput::DriverRejected)
            .unwrap_or_else(|error| panic!("driver rejection should settle: {error}")),
        RemoveConsumerGroupMembersFailureKind::DriverRejected,
        DeliveryStatus::NotSent,
    );

    let mut submitted = submitted_machine();
    assert_failure(
        submitted
            .apply(RemoveConsumerGroupMembersInput::TransportFailed {
                delivery: DeliveryStatus::PossiblySent,
            })
            .unwrap_or_else(|error| panic!("transport should settle: {error}")),
        RemoveConsumerGroupMembersFailureKind::Transport,
        DeliveryStatus::PossiblySent,
    );
}

fn machine(deadline: u64) -> RemoveConsumerGroupMembersMachine {
    let plan = RemoveConsumerGroupMembersPlan::new(
        "payments".to_owned(),
        vec![member("instance-b"), member("instance-a")],
        Some("maintenance".to_owned()),
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"));
    RemoveConsumerGroupMembersMachine::new(
        OperationId::from_raw(23),
        Deadline::from_tick(deadline),
        plan,
    )
}

fn submitted_machine() -> RemoveConsumerGroupMembersMachine {
    let mut machine = machine(20);
    machine
        .apply(RemoveConsumerGroupMembersInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(RemoveConsumerGroupMembersInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("submit machine: {error}"));
    machine
}

fn member(group_instance_id: &str) -> ConsumerGroupMemberRemoval {
    ConsumerGroupMemberRemoval::new(group_instance_id.to_owned())
}

fn nonzero(code: i16) -> NonZeroI16 {
    NonZeroI16::new(code).unwrap_or_else(|| panic!("test code must be nonzero"))
}

fn assert_failure(
    transition: RemoveConsumerGroupMembersTransition,
    kind: RemoveConsumerGroupMembersFailureKind,
    delivery: DeliveryStatus,
) {
    let Some(RemoveConsumerGroupMembersEffect::Complete {
        terminal: RemoveConsumerGroupMembersTerminal::Failed(failure),
        ..
    }) = transition.into_effect()
    else {
        panic!("expected failed terminal");
    };
    assert_eq!(failure.kind(), kind);
    assert_eq!(failure.delivery(), delivery);
}
