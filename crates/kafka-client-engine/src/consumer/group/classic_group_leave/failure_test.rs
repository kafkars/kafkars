//! Failure-classification evidence for explicit classic-group close.

use crate::driver::ClassicGroupLeaveDriverFailureKind;

use super::{
    completion::GroupConsumerCloseTerminalFailureKind, failure::classify_leave_request_error,
};

#[test]
fn authentication_failure_remains_distinct_from_transport() {
    assert_eq!(
        classify_leave_request_error(ClassicGroupLeaveDriverFailureKind::Authentication),
        GroupConsumerCloseTerminalFailureKind::Authentication
    );
    assert_eq!(
        classify_leave_request_error(ClassicGroupLeaveDriverFailureKind::Transport),
        GroupConsumerCloseTerminalFailureKind::Transport
    );
}
