//! Pre-reserved lock-free terminal cell for one accepted group seek.

use std::sync::atomic::{AtomicI16, AtomicU8, Ordering};

const PENDING: u8 = 0;
const SUCCEEDED: u8 = 1;
const DEADLINE_ELAPSED: u8 = 2;
const DRIVER_REJECTED: u8 = 3;
const TRANSPORT: u8 = 4;
const BROKER_REJECTED: u8 = 5;
const COMPATIBILITY: u8 = 6;
const INVALID_RESPONSE: u8 = 7;
const RESPONSE_TOO_LARGE: u8 = 8;
const ASSIGNMENT_LOST: u8 = 9;
const HOST_UNAVAILABLE: u8 = 10;
const INTERNAL_INVARIANT: u8 = 11;
const WRITING: u8 = u8::MAX;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct GroupConsumerSeekTerminalFailure {
    pub(crate) kind: GroupConsumerSeekTerminalFailureKind,
    pub(crate) broker_code: Option<i16>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupConsumerSeekTerminalFailureKind {
    DeadlineElapsed,
    DriverRejected,
    Transport,
    BrokerRejected,
    Compatibility,
    InvalidResponse,
    ResponseTooLarge,
    AssignmentLost,
    HostUnavailable,
    InternalInvariant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupConsumerSeekTerminal {
    Succeeded,
    Failed(GroupConsumerSeekTerminalFailure),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupConsumerSeekCompletionObservation {
    Pending,
    Terminal(GroupConsumerSeekTerminal),
    Corrupt,
}

pub(crate) struct GroupConsumerSeekCompletion {
    state: AtomicU8,
    broker_code: AtomicI16,
}

impl GroupConsumerSeekCompletion {
    pub(crate) const fn pending() -> Self {
        Self {
            state: AtomicU8::new(PENDING),
            broker_code: AtomicI16::new(0),
        }
    }

    pub(crate) fn publish(&self, terminal: GroupConsumerSeekTerminal) -> bool {
        let (state, broker_code) = encode(terminal);
        if self
            .state
            .compare_exchange(PENDING, WRITING, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return false;
        }
        self.broker_code.store(broker_code, Ordering::Relaxed);
        self.state.store(state, Ordering::Release);
        true
    }

    pub(crate) fn observe(&self) -> GroupConsumerSeekCompletionObservation {
        decode(
            self.state.load(Ordering::Acquire),
            self.broker_code.load(Ordering::Relaxed),
        )
    }
}

const fn encode(terminal: GroupConsumerSeekTerminal) -> (u8, i16) {
    match terminal {
        GroupConsumerSeekTerminal::Succeeded => (SUCCEEDED, 0),
        GroupConsumerSeekTerminal::Failed(failure) => (
            encode_failure(failure.kind),
            match failure.broker_code {
                Some(code) => code,
                None => 0,
            },
        ),
    }
}

const fn encode_failure(kind: GroupConsumerSeekTerminalFailureKind) -> u8 {
    match kind {
        GroupConsumerSeekTerminalFailureKind::DeadlineElapsed => DEADLINE_ELAPSED,
        GroupConsumerSeekTerminalFailureKind::DriverRejected => DRIVER_REJECTED,
        GroupConsumerSeekTerminalFailureKind::Transport => TRANSPORT,
        GroupConsumerSeekTerminalFailureKind::BrokerRejected => BROKER_REJECTED,
        GroupConsumerSeekTerminalFailureKind::Compatibility => COMPATIBILITY,
        GroupConsumerSeekTerminalFailureKind::InvalidResponse => INVALID_RESPONSE,
        GroupConsumerSeekTerminalFailureKind::ResponseTooLarge => RESPONSE_TOO_LARGE,
        GroupConsumerSeekTerminalFailureKind::AssignmentLost => ASSIGNMENT_LOST,
        GroupConsumerSeekTerminalFailureKind::HostUnavailable => HOST_UNAVAILABLE,
        GroupConsumerSeekTerminalFailureKind::InternalInvariant => INTERNAL_INVARIANT,
    }
}

const fn decode(state: u8, broker_code: i16) -> GroupConsumerSeekCompletionObservation {
    let kind = match state {
        PENDING | WRITING => return GroupConsumerSeekCompletionObservation::Pending,
        SUCCEEDED => {
            return GroupConsumerSeekCompletionObservation::Terminal(
                GroupConsumerSeekTerminal::Succeeded,
            );
        }
        DEADLINE_ELAPSED => GroupConsumerSeekTerminalFailureKind::DeadlineElapsed,
        DRIVER_REJECTED => GroupConsumerSeekTerminalFailureKind::DriverRejected,
        TRANSPORT => GroupConsumerSeekTerminalFailureKind::Transport,
        BROKER_REJECTED => GroupConsumerSeekTerminalFailureKind::BrokerRejected,
        COMPATIBILITY => GroupConsumerSeekTerminalFailureKind::Compatibility,
        INVALID_RESPONSE => GroupConsumerSeekTerminalFailureKind::InvalidResponse,
        RESPONSE_TOO_LARGE => GroupConsumerSeekTerminalFailureKind::ResponseTooLarge,
        ASSIGNMENT_LOST => GroupConsumerSeekTerminalFailureKind::AssignmentLost,
        HOST_UNAVAILABLE => GroupConsumerSeekTerminalFailureKind::HostUnavailable,
        INTERNAL_INVARIANT => GroupConsumerSeekTerminalFailureKind::InternalInvariant,
        _ => return GroupConsumerSeekCompletionObservation::Corrupt,
    };
    GroupConsumerSeekCompletionObservation::Terminal(GroupConsumerSeekTerminal::Failed(
        GroupConsumerSeekTerminalFailure {
            kind,
            broker_code: if matches!(kind, GroupConsumerSeekTerminalFailureKind::BrokerRejected) {
                Some(broker_code)
            } else {
                None
            },
        },
    ))
}
