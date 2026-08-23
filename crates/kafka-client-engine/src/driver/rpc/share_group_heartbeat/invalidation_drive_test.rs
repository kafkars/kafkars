//! Share-coordinator invalidation terminal classification scenarios.

use kafka_client_core::GroupId;
use kafka_driver::{CompletionError, InvalidationDisposition};

use super::{
    invalidation::{
        ShareCoordinatorInvalidationPermission, ShareCoordinatorInvalidationPoll,
        ShareCoordinatorInvalidationTerminalFailure,
    },
    invalidation_drive::terminal,
};

#[test]
fn applied_and_stale_both_permit_the_planned_replacement() {
    assert_eq!(
        terminal(group(), Ok(InvalidationDisposition::Applied)),
        ShareCoordinatorInvalidationPoll::Terminal {
            group_id: group(),
            result: Ok(ShareCoordinatorInvalidationPermission::Applied),
        }
    );
    assert_eq!(
        terminal(group(), Ok(InvalidationDisposition::IgnoredStale)),
        ShareCoordinatorInvalidationPoll::Terminal {
            group_id: group(),
            result: Ok(ShareCoordinatorInvalidationPermission::IgnoredStale),
        }
    );
}

#[test]
fn unavailable_capacity_and_completion_never_permit_replacement() {
    for (terminal, expected) in [
        (
            terminal(group(), Ok(InvalidationDisposition::Unavailable)),
            ShareCoordinatorInvalidationTerminalFailure::Unavailable,
        ),
        (
            terminal(group(), Ok(InvalidationDisposition::CapacityReached)),
            ShareCoordinatorInvalidationTerminalFailure::CapacityReached,
        ),
        (
            terminal(group(), Err(CompletionError::Closed)),
            ShareCoordinatorInvalidationTerminalFailure::Completion(CompletionError::Closed),
        ),
    ] {
        assert_eq!(
            terminal,
            ShareCoordinatorInvalidationPoll::Terminal {
                group_id: group(),
                result: Err(expected),
            }
        );
    }
}

fn group() -> GroupId {
    GroupId::try_from_raw(1).unwrap_or_else(|| panic!("nonzero test group"))
}
