//! Explicit-close leave owner lifecycle scenarios.

use std::sync::Arc;

use kafka_client_core::Deadline;

use crate::clock::OperationDeadline;

use super::{
    completion::{
        GroupConsumerCloseCompletion, GroupConsumerCloseCompletionObservation,
        GroupConsumerCloseTerminal, GroupConsumerCloseTerminalFailure,
        GroupConsumerCloseTerminalFailureKind,
    },
    owner::ClassicGroupLeaveOwner,
};

#[test]
fn terminal_capacity_is_bound_before_the_owner_becomes_unsettled() {
    let mut owner = ClassicGroupLeaveOwner::new();
    let completion = Arc::new(GroupConsumerCloseCompletion::pending());
    let deadline = OperationDeadline::from_core_for_test(Deadline::from_tick(17));

    owner
        .begin(deadline, completion)
        .unwrap_or_else(|_completion| panic!("fresh owner"));

    assert_eq!(owner.unsettled(), 1);
    assert_eq!(owner.next_deadline(), Some(Deadline::from_tick(17)));
    assert!(!owner.allows_local_close());
}

#[test]
fn failed_terminal_publication_preserves_exact_result_and_is_idempotent_at_removal() {
    let mut owner = ClassicGroupLeaveOwner::new();
    let completion = Arc::new(GroupConsumerCloseCompletion::pending());
    owner
        .begin(
            OperationDeadline::from_core_for_test(Deadline::from_tick(17)),
            Arc::clone(&completion),
        )
        .unwrap_or_else(|_| panic!("fresh owner"));
    let terminal = GroupConsumerCloseTerminal::Failed(GroupConsumerCloseTerminalFailure {
        kind: GroupConsumerCloseTerminalFailureKind::BrokerRejected,
        broker_code: Some(-731),
    });
    assert!(owner.resolve_consumer_group(terminal));
    assert!(owner.failed_terminal_is_unpublished());
    assert!(owner.publish_failed_terminal());
    assert!(!owner.failed_terminal_is_unpublished());
    assert_eq!(
        completion.observe(),
        GroupConsumerCloseCompletionObservation::Terminal(terminal)
    );
    assert!(
        owner.publish_terminal(),
        "physical removal does not republish"
    );
    owner.recover_after_driver_shutdown();
    assert!(
        owner.publish_terminal(),
        "shutdown retains the same terminal"
    );
    assert_eq!(
        completion.observe(),
        GroupConsumerCloseCompletionObservation::Terminal(terminal)
    );
}

#[test]
fn successful_broker_leave_is_not_early_failure_publication() {
    let mut owner = ClassicGroupLeaveOwner::new();
    let completion = Arc::new(GroupConsumerCloseCompletion::pending());
    owner
        .begin(
            OperationDeadline::from_core_for_test(Deadline::from_tick(17)),
            Arc::clone(&completion),
        )
        .unwrap_or_else(|_| panic!("fresh owner"));
    assert!(owner.resolve_consumer_group(GroupConsumerCloseTerminal::Succeeded));
    assert!(!owner.failed_terminal_is_unpublished());
    assert!(!owner.publish_failed_terminal());
    assert_eq!(owner.next_deadline(), Some(Deadline::from_tick(17)));
    owner.expire_unpublished_success(kafka_client_core::Moment::from_tick(16));
    assert!(!owner.failed_terminal_is_unpublished());
    assert_eq!(
        completion.observe(),
        GroupConsumerCloseCompletionObservation::Pending
    );
}
