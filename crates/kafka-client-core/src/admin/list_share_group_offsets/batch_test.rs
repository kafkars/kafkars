//! Caller-ordered multi-group re-arming, aggregation, and delivery evidence.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    ListShareGroupOffsetDescription, ListShareGroupOffsetOutcome, ListShareGroupOffsetTarget,
    ListShareGroupOffsetsBatch, ListShareGroupOffsetsBatchOutcome,
    ListShareGroupOffsetsBrokerError, ListShareGroupOffsetsEffect,
    ListShareGroupOffsetsFailureKind, ListShareGroupOffsetsInput, ListShareGroupOffsetsMachine,
    ListShareGroupOffsetsPlan, ListShareGroupOffsetsQuery, ListShareGroupOffsetsState,
    ListShareGroupOffsetsTerminal,
};

#[test]
fn batch_rearms_singleton_calls_and_preserves_per_group_rejections() {
    let mut machine = machine();
    let first = effect(
        &mut machine,
        ListShareGroupOffsetsInput::Start {
            now: Moment::from_tick(1),
        },
    );
    let ListShareGroupOffsetsEffect::Submit {
        operation_id,
        deadline,
        plan,
    } = first
    else {
        panic!("first submission expected");
    };
    assert_eq!(operation_id, OperationId::from_raw(90));
    assert_eq!(deadline, Deadline::from_tick(20));
    assert_eq!(plan.group_id(), "share-a");
    accept(&mut machine);

    let first_error = ListShareGroupOffsetsBrokerError::new(
        41,
        nonzero(15),
        Some("not coordinator".to_owned()),
        false,
    );
    let next = effect(
        &mut machine,
        ListShareGroupOffsetsInput::BrokerRejected {
            error: first_error.clone(),
        },
    );
    let ListShareGroupOffsetsEffect::Submit { deadline, plan, .. } = next else {
        panic!("second submission expected");
    };
    assert_eq!(deadline, Deadline::from_tick(20));
    assert_eq!(plan.group_id(), "share-b");
    assert_eq!(machine.state(), ListShareGroupOffsetsState::AwaitingDriver);
    accept(&mut machine);

    let terminal = terminal(effect(
        &mut machine,
        ListShareGroupOffsetsInput::BrokerResponded {
            batch: ListShareGroupOffsetsBatch::new(
                17,
                vec![ListShareGroupOffsetOutcome::described(
                    "audit".to_owned(),
                    [7; 16],
                    0,
                    ListShareGroupOffsetDescription::new(Some(9), Some(3), Some(1)),
                )],
            ),
        },
    ));
    let ListShareGroupOffsetsTerminal::Batch(batch) = terminal else {
        panic!("batch terminal expected");
    };
    assert_eq!(batch.throttle_time_ms(), 41);
    assert_eq!(batch.outcomes().len(), 2);
    assert!(matches!(
        &batch.outcomes()[0],
        ListShareGroupOffsetsBatchOutcome::BrokerRejected { group_id, error }
            if group_id == "share-a" && error == &first_error
    ));
    assert!(matches!(
        &batch.outcomes()[1],
        ListShareGroupOffsetsBatchOutcome::Offsets { group_id, offsets }
            if group_id == "share-b"
                && offsets.throttle_time_ms() == 17
                && offsets.outcomes()[0].topic() == "audit"
    ));
}

#[test]
fn later_unsent_batch_failure_retains_prior_delivery_evidence() {
    let mut machine = machine();
    let _first = effect(
        &mut machine,
        ListShareGroupOffsetsInput::Start {
            now: Moment::from_tick(1),
        },
    );
    accept(&mut machine);
    let _second = effect(
        &mut machine,
        ListShareGroupOffsetsInput::BrokerRejected {
            error: ListShareGroupOffsetsBrokerError::new(0, nonzero(15), None, false),
        },
    );

    let terminal = terminal(effect(
        &mut machine,
        ListShareGroupOffsetsInput::DeadlineElapsed,
    ));
    let ListShareGroupOffsetsTerminal::Failed(failure) = terminal else {
        panic!("deadline failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            ListShareGroupOffsetsFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        )
    );
}

fn machine() -> ListShareGroupOffsetsMachine {
    ListShareGroupOffsetsMachine::new(
        OperationId::from_raw(90),
        Deadline::from_tick(20),
        ListShareGroupOffsetsPlan::batch(vec![
            ListShareGroupOffsetsQuery::selected(
                "share-a".to_owned(),
                vec![ListShareGroupOffsetTarget::new("orders".to_owned(), 0)],
            )
            .unwrap_or_else(|error| panic!("selected query: {error}")),
            ListShareGroupOffsetsQuery::all("share-b".to_owned())
                .unwrap_or_else(|error| panic!("all query: {error}")),
        ])
        .unwrap_or_else(|error| panic!("batch plan: {error}")),
    )
}

fn accept(machine: &mut ListShareGroupOffsetsMachine) {
    machine
        .apply(ListShareGroupOffsetsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept driver: {error}"));
}

fn effect(
    machine: &mut ListShareGroupOffsetsMachine,
    input: ListShareGroupOffsetsInput,
) -> ListShareGroupOffsetsEffect {
    machine
        .apply(input)
        .unwrap_or_else(|error| panic!("apply input: {error}"))
        .into_effect()
        .unwrap_or_else(|| panic!("effect expected"))
}

fn terminal(effect: ListShareGroupOffsetsEffect) -> ListShareGroupOffsetsTerminal {
    let ListShareGroupOffsetsEffect::Complete { terminal, .. } = effect else {
        panic!("terminal effect expected");
    };
    terminal
}

fn nonzero(code: i16) -> NonZeroI16 {
    NonZeroI16::new(code).unwrap_or_else(|| panic!("test code must be nonzero"))
}
