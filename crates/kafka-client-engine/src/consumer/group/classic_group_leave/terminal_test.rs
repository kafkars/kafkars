//! Exact explicit-close success and failure normalization after membership loss.

use std::time::Instant;

use kafka_client_core::{Deadline, Moment};

use crate::{clock::OperationDeadline, driver::ClassicGroupLeaveResolution};

use super::{
    completion::{
        GroupConsumerCloseTerminal, GroupConsumerCloseTerminalFailure,
        GroupConsumerCloseTerminalFailureKind,
    },
    terminal::{normalize_terminal, should_rediscover},
};

#[test]
fn unknown_member_after_session_expiry_is_already_closed() {
    assert_eq!(
        normalize_terminal(
            deadline(50),
            Moment::from_tick(20),
            ClassicGroupLeaveResolution::BrokerRejected(25),
        ),
        GroupConsumerCloseTerminal::Succeeded
    );
    assert!(!should_rediscover(
        false,
        false,
        ClassicGroupLeaveResolution::BrokerRejected(25),
    ));
}

#[test]
fn unknown_member_cannot_override_the_original_close_deadline() {
    assert_eq!(
        normalize_terminal(
            deadline(50),
            Moment::from_tick(50),
            ClassicGroupLeaveResolution::BrokerRejected(25),
        ),
        GroupConsumerCloseTerminal::Failed(GroupConsumerCloseTerminalFailure {
            kind: GroupConsumerCloseTerminalFailureKind::DeadlineElapsed,
            broker_code: None,
        })
    );
}

#[test]
fn every_other_signed_broker_code_remains_an_exact_close_failure() {
    assert_eq!(
        normalize_terminal(
            deadline(50),
            Moment::from_tick(20),
            ClassicGroupLeaveResolution::BrokerRejected(22),
        ),
        GroupConsumerCloseTerminal::Failed(GroupConsumerCloseTerminalFailure {
            kind: GroupConsumerCloseTerminalFailureKind::BrokerRejected,
            broker_code: Some(22),
        })
    );
}

fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(Deadline::from_tick(tick), Instant::now())
}
