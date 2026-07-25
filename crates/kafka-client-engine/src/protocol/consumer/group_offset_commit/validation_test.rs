//! Borrowed validation of exact deadline and checkpoint entry count.

use std::sync::Arc;

use kafka_client_core::Deadline;

use crate::clock::OperationDeadline;

use super::{
    model_test::{entry, inputs, topic},
    preparation::GroupOffsetCommitPreparationErrorKind,
    validation::validate_group_offset_commit_inputs,
};

#[test]
fn validation_is_borrowed_and_deadline_exact() {
    let (effect, deadline, session, topics) = inputs(
        vec![entry(1, 0, 10, None)],
        4,
        Arc::from("readers"),
        Arc::from("member-a"),
        vec![topic(1, Arc::from("orders"))],
    );
    assert_eq!(
        validate_group_offset_commit_inputs(&effect, deadline, &session, &topics),
        Ok(1)
    );
    assert_eq!(
        validate_group_offset_commit_inputs(
            &effect,
            OperationDeadline::from_core_for_test(Deadline::from_tick(101)),
            &session,
            &topics,
        ),
        Err(GroupOffsetCommitPreparationErrorKind::DeadlineMismatch {
            effect: Deadline::from_tick(100),
            operation: Deadline::from_tick(101),
        })
    );
}
