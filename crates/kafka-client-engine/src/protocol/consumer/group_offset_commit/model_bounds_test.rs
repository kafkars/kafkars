//! Scalar, collection, and allocation bounds for prepared commit snapshots.

use std::sync::Arc;

use kafka_client_core::{GroupOffsetCommitEffect, OperationId};

use super::{
    model::PreparedGroupOffsetCommit,
    model_test::{entry, entry_reservation, inputs, prepare, reservation, topic},
    preparation::{GroupOffsetCommitPreparationError, GroupOffsetCommitPreparationErrorKind},
    session::{MAX_GROUP_OFFSET_COMMIT_ID_BYTES, MAX_GROUP_OFFSET_COMMIT_TOPIC_BYTES},
    validation::MAX_GROUP_OFFSET_COMMIT_ENTRIES,
};

#[test]
fn collection_capacity_failures_are_distinct_and_recoverable() {
    let maximum_partition = u32::try_from(MAX_GROUP_OFFSET_COMMIT_ENTRIES)
        .unwrap_or_else(|_| panic!("bounded test partition count fits u32"));
    let entries = (0..=maximum_partition)
        .map(|partition| entry(1, partition, i64::from(partition), None))
        .collect();
    let (effect, deadline, session, topics) = inputs(
        entries,
        4,
        Arc::from("readers"),
        Arc::from("member-a"),
        vec![topic(1, Arc::from("orders"))],
    );
    assert_recovered(
        result_error(
            PreparedGroupOffsetCommit::from_effect(
                effect,
                deadline,
                session,
                topics,
                entry_reservation(MAX_GROUP_OFFSET_COMMIT_ENTRIES),
                reservation(MAX_GROUP_OFFSET_COMMIT_ENTRIES),
            ),
            "entry capacity",
        ),
        GroupOffsetCommitPreparationErrorKind::EntryCapacity {
            actual: MAX_GROUP_OFFSET_COMMIT_ENTRIES + 1,
            limit: MAX_GROUP_OFFSET_COMMIT_ENTRIES,
        },
    );

    let topic_names = (1..=MAX_GROUP_OFFSET_COMMIT_ENTRIES + 1)
        .map(|topic_id| topic(topic_id as u64, Arc::from(format!("topic-{topic_id}"))))
        .collect::<Vec<_>>();
    let (effect, deadline, session, _topics) = inputs(
        vec![entry(1, 0, 10, None)],
        4,
        Arc::from("readers"),
        Arc::from("member-a"),
        vec![topic(1, Arc::from("orders"))],
    );
    let error = result_error(
        prepare(effect, deadline, session, topic_names),
        "topic binding capacity",
    );
    assert_eq!(
        error.kind(),
        GroupOffsetCommitPreparationErrorKind::TopicCapacity {
            actual: MAX_GROUP_OFFSET_COMMIT_ENTRIES + 1,
            limit: MAX_GROUP_OFFSET_COMMIT_ENTRIES,
        }
    );
    let (effect, returned_deadline, _session, topic_names, entry_reservation, result_reservation) =
        error.into_parts();
    assert!(matches!(effect, GroupOffsetCommitEffect::Submit { .. }));
    assert_eq!(returned_deadline, deadline);
    assert_eq!(topic_names.len(), MAX_GROUP_OFFSET_COMMIT_ENTRIES + 1);
    assert_eq!(entry_reservation.entry_count(), 1);
    assert_eq!(result_reservation.entry_count(), 1);
}

#[test]
fn classic_generation_and_exact_string_bounds_are_checked_at_preparation() {
    for generation in [-1, i64::from(i32::MAX) + 1] {
        let (effect, deadline, session, topics) = inputs(
            vec![entry(1, 0, 10, None)],
            generation,
            Arc::from("readers"),
            Arc::from("member-a"),
            vec![topic(1, Arc::from("orders"))],
        );
        assert_recovered(
            result_error(
                prepare(effect, deadline, session, topics),
                "classic generation bound",
            ),
            GroupOffsetCommitPreparationErrorKind::GroupEpochOutOfRange,
        );
    }
    assert_spelling_failure(
        Arc::from(""),
        Arc::from("member-a"),
        Arc::from("orders"),
        GroupOffsetCommitPreparationErrorKind::EmptyGroup,
    );
    assert_spelling_failure(
        Arc::from("g".repeat(MAX_GROUP_OFFSET_COMMIT_ID_BYTES + 1)),
        Arc::from("member-a"),
        Arc::from("orders"),
        GroupOffsetCommitPreparationErrorKind::GroupTooLong {
            actual: MAX_GROUP_OFFSET_COMMIT_ID_BYTES + 1,
            limit: MAX_GROUP_OFFSET_COMMIT_ID_BYTES,
        },
    );
    assert_spelling_failure(
        Arc::from("readers"),
        Arc::from(""),
        Arc::from("orders"),
        GroupOffsetCommitPreparationErrorKind::EmptyMember,
    );
    assert_spelling_failure(
        Arc::from("readers"),
        Arc::from("m".repeat(MAX_GROUP_OFFSET_COMMIT_ID_BYTES + 1)),
        Arc::from("orders"),
        GroupOffsetCommitPreparationErrorKind::MemberTooLong {
            actual: MAX_GROUP_OFFSET_COMMIT_ID_BYTES + 1,
            limit: MAX_GROUP_OFFSET_COMMIT_ID_BYTES,
        },
    );
    assert_spelling_failure(
        Arc::from("readers"),
        Arc::from("member-a"),
        Arc::from(""),
        GroupOffsetCommitPreparationErrorKind::EmptyTopicName,
    );
    assert_spelling_failure(
        Arc::from("readers"),
        Arc::from("member-a"),
        Arc::from("t".repeat(MAX_GROUP_OFFSET_COMMIT_TOPIC_BYTES + 1)),
        GroupOffsetCommitPreparationErrorKind::TopicNameTooLong {
            actual: MAX_GROUP_OFFSET_COMMIT_TOPIC_BYTES + 1,
            limit: MAX_GROUP_OFFSET_COMMIT_TOPIC_BYTES,
        },
    );
}

fn assert_spelling_failure(
    group: Arc<str>,
    member: Arc<str>,
    topic_name: Arc<str>,
    expected: GroupOffsetCommitPreparationErrorKind,
) {
    let (effect, deadline, session, topics) = inputs(
        vec![entry(1, 0, 10, None)],
        4,
        group,
        member,
        vec![topic(1, topic_name)],
    );
    assert_recovered(
        result_error(
            prepare(effect, deadline, session, topics),
            "invalid spelling",
        ),
        expected,
    );
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
    assert!(entry_reservation.entry_count() <= MAX_GROUP_OFFSET_COMMIT_ENTRIES);
    assert!(result_reservation.entry_count() <= MAX_GROUP_OFFSET_COMMIT_ENTRIES);
    drop(topics);
}

fn result_error<T, E>(result: Result<T, E>, context: &str) -> E {
    match result {
        Ok(_) => panic!("{context}"),
        Err(error) => error,
    }
}
