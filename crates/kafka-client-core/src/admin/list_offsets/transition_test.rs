//! Scenarios for Admin `ListOffsets` lifecycle and terminal assignment.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    AdminListOffset, AdminListOffsetBrokerError, AdminListOffsetOutcome, AdminListOffsetSpec,
    AdminListOffsetTarget, AdminListOffsetsEffect, AdminListOffsetsFailureKind,
    AdminListOffsetsInput, AdminListOffsetsMachine, AdminListOffsetsMachineError,
    AdminListOffsetsPlan, AdminListOffsetsState, AdminListOffsetsTerminal,
    AdminListOffsetsTransition,
};

#[test]
fn original_deadline_and_each_target_cross_sequential_submit_effects() {
    let mut machine = machine(20);
    let first = start(&mut machine);
    assert_submit(
        first,
        "orders",
        2,
        AdminListOffsetSpec::Latest,
        Deadline::from_tick(20),
    );
    machine
        .apply(AdminListOffsetsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept first: {error}"));
    let second = machine
        .apply(AdminListOffsetsInput::BrokerResponded {
            throttle_time_ms: 17,
            outcome: AdminListOffsetOutcome::listed(
                "orders".to_owned(),
                2,
                AdminListOffset::new(Some(91), Some(5), Some(7)),
            ),
        })
        .unwrap_or_else(|error| panic!("first response: {error}"));
    assert_submit(
        second,
        "audit",
        0,
        AdminListOffsetSpec::Timestamp(1_700_000_000_000),
        Deadline::from_tick(20),
    );
}

#[test]
fn ordered_mixed_results_complete_once_with_maximum_throttle() {
    let mut machine = machine(20);
    start(&mut machine);
    machine
        .apply(AdminListOffsetsInput::DriverAccepted)
        .and_then(|_| {
            machine.apply(AdminListOffsetsInput::BrokerResponded {
                throttle_time_ms: 17,
                outcome: AdminListOffsetOutcome::listed(
                    "orders".to_owned(),
                    2,
                    AdminListOffset::new(Some(91), Some(5), Some(7)),
                ),
            })
        })
        .and_then(|_| machine.apply(AdminListOffsetsInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("submit both calls: {error}"));
    let code = NonZeroI16::new(-31_777).unwrap_or_else(|| panic!("nonzero code"));
    let transition = machine
        .apply(AdminListOffsetsInput::BrokerResponded {
            throttle_time_ms: 73,
            outcome: AdminListOffsetOutcome::failed(
                "audit".to_owned(),
                0,
                AdminListOffsetBrokerError::new(code),
            ),
        })
        .unwrap_or_else(|error| panic!("second response: {error}"));
    let Some(AdminListOffsetsEffect::Complete {
        terminal: AdminListOffsetsTerminal::Listed(batch),
        ..
    }) = transition.into_effect()
    else {
        panic!("second response must complete");
    };

    assert_eq!(batch.throttle_time_ms(), 73);
    assert_eq!(batch.outcomes()[0].topic(), "orders");
    assert_eq!(batch.outcomes()[1].topic(), "audit");
    assert_eq!(machine.state(), AdminListOffsetsState::Completed);
    assert_eq!(
        machine.apply(AdminListOffsetsInput::InvalidResponse),
        Err(AdminListOffsetsMachineError::AlreadyCompleted)
    );
}

#[test]
fn identity_mismatch_is_terminal_invalid_response() {
    let mut machine = machine(20);
    start(&mut machine);
    machine
        .apply(AdminListOffsetsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept: {error}"));
    let transition = machine
        .apply(AdminListOffsetsInput::BrokerResponded {
            throttle_time_ms: 0,
            outcome: AdminListOffsetOutcome::listed(
                "other".to_owned(),
                2,
                AdminListOffset::new(Some(1), None, None),
            ),
        })
        .unwrap_or_else(|error| panic!("mismatch settles: {error}"));
    assert_failure(
        transition,
        AdminListOffsetsFailureKind::InvalidResponse,
        DeliveryStatus::PossiblySent,
    );
}

#[test]
fn original_deadline_and_driver_certainty_remain_terminal_without_retry() {
    let mut elapsed = machine(4);
    assert_failure(
        elapsed
            .apply(AdminListOffsetsInput::Start {
                now: Moment::from_tick(4),
            })
            .unwrap_or_else(|error| panic!("elapsed start: {error}")),
        AdminListOffsetsFailureKind::DeadlineElapsed,
        DeliveryStatus::NotSent,
    );

    let mut rejected = machine(20);
    start(&mut rejected);
    assert_failure(
        rejected
            .apply(AdminListOffsetsInput::DriverRejected)
            .unwrap_or_else(|error| panic!("driver rejection: {error}")),
        AdminListOffsetsFailureKind::DriverRejected,
        DeliveryStatus::NotSent,
    );

    let mut submitted = machine(20);
    start(&mut submitted);
    submitted
        .apply(AdminListOffsetsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("driver acceptance: {error}"));
    assert_failure(
        submitted
            .apply(AdminListOffsetsInput::TransportFailed {
                delivery: DeliveryStatus::PossiblySent,
            })
            .unwrap_or_else(|error| panic!("transport terminal: {error}")),
        AdminListOffsetsFailureKind::Transport,
        DeliveryStatus::PossiblySent,
    );
}

fn machine(deadline: u64) -> AdminListOffsetsMachine {
    AdminListOffsetsMachine::new(
        OperationId::from_raw(23),
        Deadline::from_tick(deadline),
        AdminListOffsetsPlan::new(vec![
            target("orders", 2, AdminListOffsetSpec::Latest),
            target(
                "audit",
                0,
                AdminListOffsetSpec::Timestamp(1_700_000_000_000),
            ),
        ])
        .unwrap_or_else(|error| panic!("valid plan: {error}")),
    )
}

fn target(topic: &str, partition: i32, spec: AdminListOffsetSpec) -> AdminListOffsetTarget {
    AdminListOffsetTarget::new(topic.to_owned(), partition, spec)
}

fn start(machine: &mut AdminListOffsetsMachine) -> AdminListOffsetsTransition {
    machine
        .apply(AdminListOffsetsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start: {error}"))
}

fn assert_submit(
    transition: AdminListOffsetsTransition,
    topic: &str,
    partition: i32,
    spec: AdminListOffsetSpec,
    expected_deadline: Deadline,
) {
    let Some(AdminListOffsetsEffect::Submit {
        operation_id,
        deadline,
        target,
        read_isolation,
    }) = transition.into_effect()
    else {
        panic!("expected submit effect");
    };
    assert_eq!(operation_id, OperationId::from_raw(23));
    assert_eq!(deadline, expected_deadline);
    assert_eq!(read_isolation, crate::ReadIsolation::ReadUncommitted);
    assert_eq!(target.topic(), topic);
    assert_eq!(target.partition(), partition);
    assert_eq!(target.spec(), spec);
}

fn assert_failure(
    transition: AdminListOffsetsTransition,
    kind: AdminListOffsetsFailureKind,
    delivery: DeliveryStatus,
) {
    let Some(AdminListOffsetsEffect::Complete {
        terminal: AdminListOffsetsTerminal::Failed(failure),
        ..
    }) = transition.into_effect()
    else {
        panic!("expected failed terminal");
    };
    assert_eq!(failure.kind(), kind);
    assert_eq!(failure.delivery(), delivery);
}
