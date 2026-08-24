//! Driver-terminal classification scenarios for coordinator invalidation.

use kafka_client_core::GroupId;
use kafka_driver::{CompletionError, InvalidationDisposition};

use super::{
    coordinator_invalidation::{
        ClassicCoordinatorInvalidationPermission, ClassicCoordinatorInvalidationPoll,
        ClassicCoordinatorInvalidationTerminalFailure,
    },
    coordinator_invalidation_drive::terminal,
};

#[test]
fn applied_stale_and_unavailable_dispositions_permit_fresh_discovery() {
    assert_eq!(
        terminal(group(), Ok(InvalidationDisposition::Applied)).result(),
        Ok(ClassicCoordinatorInvalidationPermission::Applied)
    );
    assert_eq!(
        terminal(group(), Ok(InvalidationDisposition::IgnoredStale)).result(),
        Ok(ClassicCoordinatorInvalidationPermission::IgnoredStale)
    );
    assert_eq!(
        terminal(group(), Ok(InvalidationDisposition::Unavailable)).result(),
        Ok(ClassicCoordinatorInvalidationPermission::Unavailable)
    );
}

#[test]
fn capacity_terminal_never_permits_join() {
    assert_eq!(
        terminal(group(), Ok(InvalidationDisposition::CapacityReached)).result(),
        Err(ClassicCoordinatorInvalidationTerminalFailure::CapacityReached)
    );
}

#[test]
fn completion_corruption_remains_distinct_from_driver_disposition() {
    let terminal = terminal(group(), Err(CompletionError::Closed));

    assert_eq!(terminal.group_id(), group());
    assert_eq!(
        terminal.result(),
        Err(ClassicCoordinatorInvalidationTerminalFailure::Completion(
            CompletionError::Closed
        ))
    );
}

#[test]
fn submitted_work_is_distinct_from_an_already_pending_call() {
    assert_ne!(
        ClassicCoordinatorInvalidationPoll::Submitted { group_id: group() },
        ClassicCoordinatorInvalidationPoll::Pending { group_id: group() }
    );
}

fn group() -> GroupId {
    GroupId::try_from_raw(1).unwrap_or_else(|| panic!("test group must be nonzero"))
}
