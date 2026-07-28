//! Explicit-close leave owner lifecycle scenarios.

use std::sync::Arc;

use kafka_client_core::Deadline;

use crate::clock::OperationDeadline;

use super::{completion::GroupConsumerCloseCompletion, owner::ClassicGroupLeaveOwner};

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
