//! Lossless core-to-engine API-77 terminal translation scenarios.

use core::num::NonZeroI16;

use kafka_client_core::{
    Deadline, DeliveryStatus, DescribeShareGroupAssignment as CoreAssignment,
    DescribeShareGroupBrokerError as CoreError, DescribeShareGroupDescription as CoreDescription,
    DescribeShareGroupInput as CoreInput, DescribeShareGroupMachine as CoreMachine,
    DescribeShareGroupMember as CoreMember, DescribeShareGroupOutcome as CoreOutcome,
    DescribeShareGroupPlan as CorePlan, DescribeShareGroupResult as CoreResult,
    DescribeShareGroupTerminal as CoreTerminal,
    DescribeShareGroupTopicAssignment as CoreTopicAssignment,
    DescribeShareGroupsBatch as CoreBatch, Moment, OperationId,
};

use super::{
    DescribeShareGroupBatchOutcome, DescribeShareGroupDeliveryStatus,
    DescribeShareGroupFailureKind, DescribeShareGroupOutcome, outcome::translate_terminal,
};

#[test]
fn exact_description_translates_without_generated_values() {
    let terminal = CoreTerminal::Described(CoreResult::new(
        73,
        CoreDescription::new(
            "share".to_owned(),
            "Stable".to_owned(),
            1,
            2,
            "uniform".to_owned(),
            vec![CoreMember::new(
                "member-a".to_owned(),
                None,
                3,
                "client".to_owned(),
                "host".to_owned(),
                vec!["orders".to_owned()],
                CoreAssignment::new(vec![CoreTopicAssignment::new(
                    [7; 16],
                    "orders".to_owned(),
                    vec![0, 1],
                )]),
            )],
            Some(5),
        ),
    ));
    let DescribeShareGroupOutcome::Described(result) = translate_terminal(terminal) else {
        panic!("description expected");
    };

    assert_eq!(result.throttle_time_ms(), 73);
    let (group, state, epoch, assignment_epoch, assignor, members, operations) =
        result.description().clone().into_parts();
    assert_eq!((group.as_str(), state.as_str()), ("share", "Stable"));
    assert_eq!(
        (epoch, assignment_epoch, assignor.as_str()),
        (1, 2, "uniform")
    );
    assert_eq!(members.len(), 1);
    assert_eq!(operations, Some(5));
}

#[test]
fn top_level_rejection_preserves_throttle_code_and_diagnostic() {
    let terminal = CoreTerminal::BrokerRejected(CoreError::new(
        19,
        nonzero(-32_000),
        Some("share group rejected".to_owned()),
        false,
    ));
    let DescribeShareGroupOutcome::BrokerRejected(error) = translate_terminal(terminal) else {
        panic!("broker rejection expected");
    };
    assert_eq!(
        error.into_parts(),
        (19, -32_000, Some("share group rejected".to_owned()), false)
    );
}

#[test]
fn mechanism_failure_preserves_authoritative_delivery_certainty() {
    let mut machine = CoreMachine::new(
        OperationId::from_raw(77),
        Deadline::from_tick(20),
        CorePlan::new("share".to_owned(), false).unwrap_or_else(|error| panic!("plan: {error}")),
    );
    machine
        .apply(CoreInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(CoreInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("submit: {error}"));
    let effect = machine
        .apply(CoreInput::TransportFailed {
            delivery: DeliveryStatus::PossiblySent,
        })
        .unwrap_or_else(|error| panic!("settle: {error}"))
        .into_effect();
    let Some(kafka_client_core::DescribeShareGroupEffect::Complete { terminal, .. }) = effect
    else {
        panic!("terminal expected");
    };
    let DescribeShareGroupOutcome::Failed(failure) = translate_terminal(terminal) else {
        panic!("failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            DescribeShareGroupFailureKind::Transport,
            DescribeShareGroupDeliveryStatus::PossiblySent,
        )
    );
}

#[test]
fn batch_translation_preserves_order_partial_rejection_and_maximum_throttle() {
    let terminal = CoreTerminal::Batch(CoreBatch::new(
        23,
        vec![
            CoreOutcome::broker_rejected(
                "payments-share".to_owned(),
                CoreError::new(
                    23,
                    nonzero(15),
                    Some("coordinator moving".to_owned()),
                    false,
                ),
            ),
            CoreOutcome::described(CoreResult::new(
                3,
                CoreDescription::new(
                    "orders-share".to_owned(),
                    "Stable".to_owned(),
                    4,
                    5,
                    "uniform".to_owned(),
                    Vec::new(),
                    None,
                ),
            )),
        ],
    ));

    let DescribeShareGroupOutcome::Batch(batch) = translate_terminal(terminal) else {
        panic!("batch expected");
    };
    let (throttle_time_ms, outcomes) = batch.into_parts();
    assert_eq!(throttle_time_ms, 23);
    assert_eq!(outcomes.len(), 2);
    let DescribeShareGroupBatchOutcome::BrokerRejected { group_id, error } = &outcomes[0] else {
        panic!("first group rejection expected");
    };
    assert_eq!(group_id, "payments-share");
    assert_eq!(
        error.clone().into_parts(),
        (23, 15, Some("coordinator moving".to_owned()), false)
    );
    let DescribeShareGroupBatchOutcome::Described(result) = &outcomes[1] else {
        panic!("second group description expected");
    };
    assert_eq!(result.throttle_time_ms(), 3);
    assert_eq!(result.description().group_id, "orders-share");
}

fn nonzero(code: i16) -> NonZeroI16 {
    NonZeroI16::new(code).unwrap_or_else(|| panic!("nonzero"))
}
