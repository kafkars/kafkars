//! Lossless core-to-engine API-89 terminal translation scenarios.

use core::num::NonZeroI16;

use kafka_client_core::{
    Deadline, DeliveryStatus, DescribeStreamsGroupBrokerError as CoreError,
    DescribeStreamsGroupDescription as CoreDescription, DescribeStreamsGroupInput as CoreInput,
    DescribeStreamsGroupMachine as CoreMachine, DescribeStreamsGroupPlan as CorePlan,
    DescribeStreamsGroupResult as CoreResult, DescribeStreamsGroupTerminal as CoreTerminal, Moment,
    OperationId,
};

use super::{
    DescribeStreamsGroupBatchOutcome, DescribeStreamsGroupDeliveryStatus,
    DescribeStreamsGroupFailureKind, DescribeStreamsGroupOutcome, outcome::translate_terminal,
};

#[test]
fn exact_description_translates_without_generated_values() {
    let terminal = CoreTerminal::Described(CoreResult::new(
        89,
        CoreDescription::new(
            "streams".to_owned(),
            "Stable".to_owned(),
            1,
            2,
            None,
            Vec::new(),
            Some(5),
            None,
            Some(kafka_client_core::DescribeStreamsGroupTopologyDescriptionStatus::new(9)),
        ),
    ));
    let DescribeStreamsGroupOutcome::Described(result) = translate_terminal(terminal) else {
        panic!("description expected");
    };

    assert_eq!(result.throttle_time_ms(), 89);
    let (group, state, epoch, assignment_epoch, _, members, operations, _, status) =
        result.description().clone().into_parts();
    assert_eq!((group.as_str(), state.as_str()), ("streams", "Stable"));
    assert_eq!((epoch, assignment_epoch), (1, 2));
    assert!(members.is_empty());
    assert_eq!(operations, Some(5));
    assert_eq!(status.map(|value| value.raw()), Some(9));
}

#[test]
fn top_level_rejection_preserves_throttle_code_and_diagnostic() {
    let terminal = CoreTerminal::BrokerRejected(CoreError::new(
        19,
        nonzero(-32_000),
        Some("streams group rejected".to_owned()),
        false,
    ));
    let DescribeStreamsGroupOutcome::BrokerRejected(error) = translate_terminal(terminal) else {
        panic!("broker rejection expected");
    };
    assert_eq!(
        error.into_parts(),
        (
            19,
            -32_000,
            Some("streams group rejected".to_owned()),
            false
        )
    );
}

#[test]
fn mechanism_failure_preserves_authoritative_delivery_certainty() {
    let mut machine = CoreMachine::new(
        OperationId::from_raw(89),
        Deadline::from_tick(20),
        CorePlan::new("streams".to_owned(), false, false)
            .unwrap_or_else(|error| panic!("plan: {error}")),
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
    let Some(kafka_client_core::DescribeStreamsGroupEffect::Complete { terminal, .. }) = effect
    else {
        panic!("terminal expected");
    };
    let DescribeStreamsGroupOutcome::Failed(failure) = translate_terminal(terminal) else {
        panic!("failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            DescribeStreamsGroupFailureKind::Transport,
            DescribeStreamsGroupDeliveryStatus::PossiblySent,
        )
    );
}

#[test]
fn batch_translation_preserves_order_rejections_and_maximum_throttle() {
    let terminal = CoreTerminal::Batch(kafka_client_core::DescribeStreamsGroupsBatch::new(
        31,
        vec![
            kafka_client_core::DescribeStreamsGroupOutcome::broker_rejected(
                "orders".to_owned(),
                CoreError::new(31, nonzero(15), Some("rejected".to_owned()), false),
            ),
            kafka_client_core::DescribeStreamsGroupOutcome::described(CoreResult::new(
                7,
                CoreDescription::new(
                    "audit".to_owned(),
                    "Stable".to_owned(),
                    1,
                    1,
                    None,
                    Vec::new(),
                    None,
                    None,
                    None,
                ),
            )),
        ],
    ));
    let DescribeStreamsGroupOutcome::Batch(batch) = translate_terminal(terminal) else {
        panic!("batch expected");
    };

    let (throttle_time_ms, outcomes) = batch.into_parts();
    assert_eq!(throttle_time_ms, 31);
    assert_eq!(
        outcomes
            .iter()
            .map(DescribeStreamsGroupBatchOutcome::group_id)
            .collect::<Vec<_>>(),
        vec!["orders", "audit"]
    );
    let DescribeStreamsGroupBatchOutcome::BrokerRejected { error, .. } = &outcomes[0] else {
        panic!("broker rejection expected");
    };
    assert_eq!((error.throttle_time_ms(), error.code()), (31, 15));
    assert!(matches!(
        &outcomes[1],
        DescribeStreamsGroupBatchOutcome::Described(_)
    ));
}

fn nonzero(code: i16) -> NonZeroI16 {
    NonZeroI16::new(code).unwrap_or_else(|| panic!("nonzero"))
}
