//! Scenarios for API-92 lifecycle, correlation, and terminal single assignment.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    DELETE_SHARE_GROUP_OFFSETS_DIAGNOSTIC_BYTES, DELETE_SHARE_GROUP_OFFSETS_MAX_TOPICS,
    DeleteShareGroupOffsetsBatch, DeleteShareGroupOffsetsBrokerError,
    DeleteShareGroupOffsetsEffect, DeleteShareGroupOffsetsFailureKind,
    DeleteShareGroupOffsetsInput, DeleteShareGroupOffsetsMachine,
    DeleteShareGroupOffsetsMachineError, DeleteShareGroupOffsetsPlan, DeleteShareGroupOffsetsState,
    DeleteShareGroupOffsetsTerminal, DeleteShareGroupOffsetsTopicBrokerError,
    DeleteShareGroupOffsetsTopicOutcome, DeleteShareGroupOffsetsTopicResult,
    DeleteShareGroupOffsetsTransition,
};

#[test]
fn original_deadline_group_and_topics_cross_the_only_submit_effect() {
    let mut machine = machine(20);
    let transition = machine
        .apply(DeleteShareGroupOffsetsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start should succeed: {error}"));
    let Some(DeleteShareGroupOffsetsEffect::Submit {
        operation_id,
        deadline,
        plan,
    }) = transition.into_effect()
    else {
        panic!("start must submit");
    };

    assert_eq!(operation_id, OperationId::from_raw(92));
    assert_eq!(deadline, Deadline::from_tick(20));
    assert_eq!(plan.group_id(), "payments-share");
    assert_eq!(plan.topics(), ["orders", "audit"]);
}

#[test]
fn mixed_response_is_correlated_to_caller_order_and_lossless() {
    let mut machine = submitted_machine();
    let error = DeleteShareGroupOffsetsTopicBrokerError::new(
        nonzero(-32_123),
        Some("not empty".to_owned()),
        false,
    );
    let transition = machine
        .apply(DeleteShareGroupOffsetsInput::BrokerResponded {
            batch: DeleteShareGroupOffsetsBatch::new(
                73,
                vec![
                    DeleteShareGroupOffsetsTopicOutcome::failed("audit".to_owned(), error),
                    DeleteShareGroupOffsetsTopicOutcome::deleted("orders".to_owned(), [7; 16]),
                ],
            ),
        })
        .unwrap_or_else(|error| panic!("response should settle: {error}"));
    let Some(DeleteShareGroupOffsetsEffect::Complete {
        terminal: DeleteShareGroupOffsetsTerminal::Deleted(batch),
        ..
    }) = transition.into_effect()
    else {
        panic!("response must complete with correlated outcomes");
    };

    assert_eq!(batch.throttle_time_ms(), 73);
    assert_eq!(batch.outcomes()[0].topic(), "orders");
    assert_eq!(
        batch.outcomes()[0].result(),
        &DeleteShareGroupOffsetsTopicResult::Deleted([7; 16])
    );
    assert_eq!(batch.outcomes()[1].topic(), "audit");
    assert_eq!(machine.state(), DeleteShareGroupOffsetsState::Completed);
    assert_eq!(
        machine.apply(DeleteShareGroupOffsetsInput::InvalidResponse),
        Err(DeleteShareGroupOffsetsMachineError::AlreadyCompleted)
    );
}

#[test]
fn top_level_broker_rejection_preserves_exact_bounded_fact() {
    let mut machine = submitted_machine();
    let transition = machine
        .apply(DeleteShareGroupOffsetsInput::BrokerRejected {
            error: DeleteShareGroupOffsetsBrokerError::new(
                19,
                nonzero(-31_777),
                Some("group rejected".to_owned()),
                false,
            ),
        })
        .unwrap_or_else(|error| panic!("broker rejection should settle: {error}"));
    let Some(DeleteShareGroupOffsetsEffect::Complete {
        terminal: DeleteShareGroupOffsetsTerminal::BrokerRejected(error),
        ..
    }) = transition.into_effect()
    else {
        panic!("exact top-level rejection must remain distinct");
    };

    assert_eq!(error.throttle_time_ms(), 19);
    assert_eq!(error.code(), -31_777);
    assert_eq!(error.message(), Some("group rejected"));
}

#[test]
fn missing_duplicate_unexpected_or_zero_id_response_is_invalid() {
    for outcomes in [
        vec![DeleteShareGroupOffsetsTopicOutcome::deleted(
            "orders".to_owned(),
            [1; 16],
        )],
        vec![
            DeleteShareGroupOffsetsTopicOutcome::deleted("orders".to_owned(), [1; 16]),
            DeleteShareGroupOffsetsTopicOutcome::deleted("orders".to_owned(), [2; 16]),
        ],
        vec![
            DeleteShareGroupOffsetsTopicOutcome::deleted("orders".to_owned(), [1; 16]),
            DeleteShareGroupOffsetsTopicOutcome::deleted("unknown".to_owned(), [2; 16]),
        ],
        vec![
            DeleteShareGroupOffsetsTopicOutcome::deleted("orders".to_owned(), [0; 16]),
            DeleteShareGroupOffsetsTopicOutcome::deleted("audit".to_owned(), [2; 16]),
        ],
    ] {
        let mut machine = submitted_machine();
        let transition = machine
            .apply(DeleteShareGroupOffsetsInput::BrokerResponded {
                batch: DeleteShareGroupOffsetsBatch::new(0, outcomes),
            })
            .unwrap_or_else(|error| panic!("malformed response should settle: {error}"));
        assert_failure(
            transition,
            DeleteShareGroupOffsetsFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        );
    }
}

#[test]
fn invalid_diagnostic_shape_cannot_enter_a_terminal_broker_fact() {
    let invalid_topic_errors = [
        DeleteShareGroupOffsetsTopicBrokerError::new(nonzero(1), None, true),
        DeleteShareGroupOffsetsTopicBrokerError::new(
            nonzero(1),
            Some("x".repeat(DELETE_SHARE_GROUP_OFFSETS_DIAGNOSTIC_BYTES + 1)),
            false,
        ),
    ];
    for error in invalid_topic_errors {
        let mut machine = submitted_machine();
        let transition = machine
            .apply(DeleteShareGroupOffsetsInput::BrokerResponded {
                batch: DeleteShareGroupOffsetsBatch::new(
                    0,
                    vec![
                        DeleteShareGroupOffsetsTopicOutcome::deleted("orders".to_owned(), [1; 16]),
                        DeleteShareGroupOffsetsTopicOutcome::failed("audit".to_owned(), error),
                    ],
                ),
            })
            .unwrap_or_else(|error| panic!("invalid diagnostic should settle: {error}"));
        assert_failure(
            transition,
            DeleteShareGroupOffsetsFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        );
    }

    let mut machine = submitted_machine();
    let transition = machine
        .apply(DeleteShareGroupOffsetsInput::BrokerRejected {
            error: DeleteShareGroupOffsetsBrokerError::new(0, nonzero(1), None, true),
        })
        .unwrap_or_else(|error| panic!("invalid top-level diagnostic should settle: {error}"));
    assert_failure(
        transition,
        DeleteShareGroupOffsetsFailureKind::InvalidResponse,
        DeliveryStatus::PossiblySent,
    );
}

#[test]
fn excessive_response_count_is_response_too_large() {
    let mut machine = submitted_machine();
    let outcomes = (0..=DELETE_SHARE_GROUP_OFFSETS_MAX_TOPICS)
        .map(|index| {
            DeleteShareGroupOffsetsTopicOutcome::deleted(format!("topic-{index}"), [1; 16])
        })
        .collect();
    let transition = machine
        .apply(DeleteShareGroupOffsetsInput::BrokerResponded {
            batch: DeleteShareGroupOffsetsBatch::new(0, outcomes),
        })
        .unwrap_or_else(|error| panic!("oversized response should settle: {error}"));

    assert_failure(
        transition,
        DeleteShareGroupOffsetsFailureKind::ResponseTooLarge,
        DeliveryStatus::PossiblySent,
    );
}

#[test]
fn excessive_aggregate_response_text_is_response_too_large() {
    let topics: Vec<_> = (0..1024).map(|index| format!("topic-{index}")).collect();
    let plan = DeleteShareGroupOffsetsPlan::new("share".to_owned(), topics.clone())
        .unwrap_or_else(|error| panic!("bounded plan: {error}"));
    let mut machine = DeleteShareGroupOffsetsMachine::new(
        OperationId::from_raw(93),
        Deadline::from_tick(20),
        plan,
    );
    machine
        .apply(DeleteShareGroupOffsetsInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(DeleteShareGroupOffsetsInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("submit machine: {error}"));
    let outcomes = topics
        .into_iter()
        .map(|topic| {
            DeleteShareGroupOffsetsTopicOutcome::failed(
                topic,
                DeleteShareGroupOffsetsTopicBrokerError::new(
                    nonzero(1),
                    Some("x".repeat(DELETE_SHARE_GROUP_OFFSETS_DIAGNOSTIC_BYTES)),
                    false,
                ),
            )
        })
        .collect();
    let transition = machine
        .apply(DeleteShareGroupOffsetsInput::BrokerResponded {
            batch: DeleteShareGroupOffsetsBatch::new(0, outcomes),
        })
        .unwrap_or_else(|error| panic!("oversized response should settle: {error}"));

    assert_failure(
        transition,
        DeleteShareGroupOffsetsFailureKind::ResponseTooLarge,
        DeliveryStatus::PossiblySent,
    );
}

#[test]
fn deadlines_and_driver_owned_failures_preserve_certainty_without_retry() {
    let mut elapsed = machine(4);
    assert_failure(
        elapsed
            .apply(DeleteShareGroupOffsetsInput::Start {
                now: Moment::from_tick(4),
            })
            .unwrap_or_else(|error| panic!("elapsed start should settle: {error}")),
        DeleteShareGroupOffsetsFailureKind::DeadlineElapsed,
        DeliveryStatus::NotSent,
    );

    let mut rejected = machine(20);
    rejected
        .apply(DeleteShareGroupOffsetsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start should succeed: {error}"));
    assert_failure(
        rejected
            .apply(DeleteShareGroupOffsetsInput::DriverRejected)
            .unwrap_or_else(|error| panic!("driver rejection should settle: {error}")),
        DeleteShareGroupOffsetsFailureKind::DriverRejected,
        DeliveryStatus::NotSent,
    );

    for (input, kind, delivery) in [
        (
            DeleteShareGroupOffsetsInput::DriverDeadlineElapsed {
                delivery: DeliveryStatus::PossiblySent,
            },
            DeleteShareGroupOffsetsFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            DeleteShareGroupOffsetsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::NotSent,
            },
            DeleteShareGroupOffsetsFailureKind::Compatibility,
            DeliveryStatus::NotSent,
        ),
        (
            DeleteShareGroupOffsetsInput::TransportFailed {
                delivery: DeliveryStatus::PossiblySent,
            },
            DeleteShareGroupOffsetsFailureKind::Transport,
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

fn machine(deadline: u64) -> DeleteShareGroupOffsetsMachine {
    let plan = DeleteShareGroupOffsetsPlan::new(
        "payments-share".to_owned(),
        vec!["orders".to_owned(), "audit".to_owned()],
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"));
    DeleteShareGroupOffsetsMachine::new(
        OperationId::from_raw(92),
        Deadline::from_tick(deadline),
        plan,
    )
}

fn submitted_machine() -> DeleteShareGroupOffsetsMachine {
    let mut machine = machine(20);
    machine
        .apply(DeleteShareGroupOffsetsInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(DeleteShareGroupOffsetsInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("submit machine: {error}"));
    machine
}

fn nonzero(code: i16) -> NonZeroI16 {
    NonZeroI16::new(code).unwrap_or_else(|| panic!("test code must be nonzero"))
}

fn assert_failure(
    transition: DeleteShareGroupOffsetsTransition,
    kind: DeleteShareGroupOffsetsFailureKind,
    delivery: DeliveryStatus,
) {
    let Some(DeleteShareGroupOffsetsEffect::Complete {
        terminal: DeleteShareGroupOffsetsTerminal::Failed(failure),
        ..
    }) = transition.into_effect()
    else {
        panic!("expected failed terminal");
    };
    assert_eq!(failure.kind(), kind);
    assert_eq!(failure.delivery(), delivery);
}
