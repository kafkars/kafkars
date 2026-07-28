//! Accepted-close terminal category preservation evidence.

use super::{GroupConsumerCloseErrorKind, error::terminal_error};
use crate::consumer::group::{
    GroupConsumerCloseTerminalFailure, GroupConsumerCloseTerminalFailureKind,
};

#[test]
fn authentication_terminal_remains_distinct_from_transport() {
    let authentication = terminal_error(GroupConsumerCloseTerminalFailure {
        kind: GroupConsumerCloseTerminalFailureKind::Authentication,
        broker_code: None,
    });
    let transport = terminal_error(GroupConsumerCloseTerminalFailure {
        kind: GroupConsumerCloseTerminalFailureKind::Transport,
        broker_code: None,
    });

    assert_eq!(
        authentication.kind(),
        GroupConsumerCloseErrorKind::Authentication
    );
    assert_eq!(transport.kind(), GroupConsumerCloseErrorKind::Transport);
    assert_ne!(authentication, transport);
}
