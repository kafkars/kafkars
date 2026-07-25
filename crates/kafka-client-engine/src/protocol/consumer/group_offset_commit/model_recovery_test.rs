//! Lossless recovery scenarios for rejected prepared commit snapshots.

use std::sync::Arc;

use kafka_client_core::{
    Deadline, GroupOffsetCommitBatch, GroupOffsetCommitEffect, GroupOffsetCommitTerminal,
    OperationId, TopicId,
};

use crate::clock::OperationDeadline;

use super::{
    model_test::{entry, generation, group_id, inputs, member_id, prepare, session, topic},
    preparation::{GroupOffsetCommitPreparationError, GroupOffsetCommitPreparationErrorKind},
    session::ClassicGroupCommitSession,
};

#[test]
fn deadline_generation_and_unknown_topic_failures_return_every_owner() {
    let (effect, _deadline, session, topics) = inputs(
        vec![entry(1, 0, 10, None)],
        4,
        Arc::from("readers"),
        Arc::from("member-a"),
        vec![topic(1, Arc::from("orders"))],
    );
    let supplied = OperationDeadline::from_core_for_test(Deadline::from_tick(101));
    let error = result_error(
        prepare(effect, supplied, session, topics),
        "deadline mismatch",
    );
    assert_eq!(
        error.kind(),
        GroupOffsetCommitPreparationErrorKind::DeadlineMismatch {
            effect: Deadline::from_tick(100),
            operation: Deadline::from_tick(101),
        }
    );
    let (effect, returned, _session, topics, entry_reservation, result_reservation) =
        error.into_parts();
    assert!(matches!(effect, GroupOffsetCommitEffect::Submit { .. }));
    assert_eq!(returned, supplied);
    assert_eq!(topics.len(), 1);
    assert_eq!(entry_reservation.entry_count(), 1);
    assert_eq!(result_reservation.entry_count(), 1);

    let (effect, deadline, _session, topics) = inputs(
        vec![entry(1, 0, 10, None)],
        4,
        Arc::from("readers"),
        Arc::from("member-a"),
        vec![topic(1, Arc::from("orders"))],
    );
    let session = ClassicGroupCommitSession::new(
        group_id(),
        Arc::from("readers"),
        member_id(),
        Arc::from("member-a"),
        generation(5),
        4,
    );
    assert_recovered(
        result_error(
            prepare(effect, deadline, session, topics),
            "generation mismatch",
        ),
        GroupOffsetCommitPreparationErrorKind::GenerationMismatch,
    );

    let (effect, deadline, session, _) = inputs(
        vec![entry(1, 0, 10, None)],
        4,
        Arc::from("readers"),
        Arc::from("member-a"),
        vec![],
    );
    assert_recovered(
        result_error(prepare(effect, deadline, session, vec![]), "unknown topic"),
        GroupOffsetCommitPreparationErrorKind::UnknownTopic(TopicId::from_raw(1)),
    );
}

#[test]
fn unexpected_complete_effect_is_recovered_intact() {
    let effect = GroupOffsetCommitEffect::Complete {
        operation_id: OperationId::from_raw(9),
        terminal: GroupOffsetCommitTerminal::Committed(GroupOffsetCommitBatch::new(0, vec![])),
    };
    let deadline = OperationDeadline::from_core_for_test(Deadline::from_tick(100));
    let session = session(4, Arc::from("readers"), Arc::from("member-a"));
    let error = result_error(
        prepare(effect, deadline, session, vec![]),
        "complete is not a submit",
    );
    assert_eq!(
        error.kind(),
        GroupOffsetCommitPreparationErrorKind::UnexpectedEffect
    );
    let (effect, returned_deadline, _, topics, entry_reservation, result_reservation) =
        error.into_parts();
    assert!(matches!(effect, GroupOffsetCommitEffect::Complete { .. }));
    assert_eq!(returned_deadline, deadline);
    assert!(topics.is_empty());
    assert_eq!(entry_reservation.entry_count(), 0);
    assert_eq!(result_reservation.entry_count(), 0);
}

fn assert_recovered(
    error: GroupOffsetCommitPreparationError,
    expected: GroupOffsetCommitPreparationErrorKind,
) {
    assert_eq!(error.kind(), expected);
    let (effect, _deadline, _session, topics, entry_reservation, result_reservation) =
        error.into_parts();
    let GroupOffsetCommitEffect::Submit {
        operation_id,
        checkpoint,
        ..
    } = effect
    else {
        panic!("submit effect must be recovered");
    };
    assert_eq!(operation_id, OperationId::from_raw(9));
    assert!(!checkpoint.entries().is_empty());
    assert_eq!(entry_reservation.entry_count(), checkpoint.entries().len());
    assert_eq!(result_reservation.entry_count(), checkpoint.entries().len());
    drop(topics);
}

fn result_error<T, E>(result: Result<T, E>, context: &str) -> E {
    match result {
        Ok(_) => panic!("{context}"),
        Err(error) => error,
    }
}
