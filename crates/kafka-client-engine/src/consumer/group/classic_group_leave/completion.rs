//! Pre-reserved lock-free terminal cell for one accepted explicit close.

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
const DRIVER_SHUTDOWN: u8 = 9;
const AUTHENTICATION: u8 = 10;
const WRITING: u8 = u8::MAX;

/// Exact stable failure retained after broker-side leave settlement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) struct GroupConsumerCloseTerminalFailure {
    pub(in crate::consumer) kind: GroupConsumerCloseTerminalFailureKind,
    pub(in crate::consumer) broker_code: Option<i16>,
}

/// Closed internal failure vocabulary for one accepted close.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) enum GroupConsumerCloseTerminalFailureKind {
    DeadlineElapsed,
    DriverRejected,
    Transport,
    BrokerRejected,
    Compatibility,
    InvalidResponse,
    ResponseTooLarge,
    DriverShutdown,
    Authentication,
}

/// Exact terminal installed before physical entry removal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) enum GroupConsumerCloseTerminal {
    Succeeded,
    Failed(GroupConsumerCloseTerminalFailure),
}

/// Nonblocking observation of the pre-reserved terminal cell.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) enum GroupConsumerCloseCompletionObservation {
    Pending,
    Terminal(GroupConsumerCloseTerminal),
    Corrupt,
}

/// One fixed-size completion slot allocated before close admission.
pub(in crate::consumer) struct GroupConsumerCloseCompletion {
    state: AtomicU8,
    broker_code: AtomicI16,
}

impl GroupConsumerCloseCompletion {
    pub(in crate::consumer) const fn pending() -> Self {
        Self {
            state: AtomicU8::new(PENDING),
            broker_code: AtomicI16::new(0),
        }
    }

    pub(super) fn publish(&self, terminal: GroupConsumerCloseTerminal) -> bool {
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

    pub(in crate::consumer) fn observe(&self) -> GroupConsumerCloseCompletionObservation {
        let state = self.state.load(Ordering::Acquire);
        decode(state, self.broker_code.load(Ordering::Relaxed))
    }
}

const fn encode(terminal: GroupConsumerCloseTerminal) -> (u8, i16) {
    match terminal {
        GroupConsumerCloseTerminal::Succeeded => (SUCCEEDED, 0),
        GroupConsumerCloseTerminal::Failed(failure) => (
            match failure.kind {
                GroupConsumerCloseTerminalFailureKind::DeadlineElapsed => DEADLINE_ELAPSED,
                GroupConsumerCloseTerminalFailureKind::DriverRejected => DRIVER_REJECTED,
                GroupConsumerCloseTerminalFailureKind::Transport => TRANSPORT,
                GroupConsumerCloseTerminalFailureKind::BrokerRejected => BROKER_REJECTED,
                GroupConsumerCloseTerminalFailureKind::Compatibility => COMPATIBILITY,
                GroupConsumerCloseTerminalFailureKind::InvalidResponse => INVALID_RESPONSE,
                GroupConsumerCloseTerminalFailureKind::ResponseTooLarge => RESPONSE_TOO_LARGE,
                GroupConsumerCloseTerminalFailureKind::DriverShutdown => DRIVER_SHUTDOWN,
                GroupConsumerCloseTerminalFailureKind::Authentication => AUTHENTICATION,
            },
            match failure.broker_code {
                Some(code) => code,
                None => 0,
            },
        ),
    }
}

const fn decode(state: u8, broker_code: i16) -> GroupConsumerCloseCompletionObservation {
    let kind = match state {
        PENDING | WRITING => return GroupConsumerCloseCompletionObservation::Pending,
        SUCCEEDED => {
            return GroupConsumerCloseCompletionObservation::Terminal(
                GroupConsumerCloseTerminal::Succeeded,
            );
        }
        DEADLINE_ELAPSED => GroupConsumerCloseTerminalFailureKind::DeadlineElapsed,
        DRIVER_REJECTED => GroupConsumerCloseTerminalFailureKind::DriverRejected,
        TRANSPORT => GroupConsumerCloseTerminalFailureKind::Transport,
        BROKER_REJECTED => GroupConsumerCloseTerminalFailureKind::BrokerRejected,
        COMPATIBILITY => GroupConsumerCloseTerminalFailureKind::Compatibility,
        INVALID_RESPONSE => GroupConsumerCloseTerminalFailureKind::InvalidResponse,
        RESPONSE_TOO_LARGE => GroupConsumerCloseTerminalFailureKind::ResponseTooLarge,
        DRIVER_SHUTDOWN => GroupConsumerCloseTerminalFailureKind::DriverShutdown,
        AUTHENTICATION => GroupConsumerCloseTerminalFailureKind::Authentication,
        _ => return GroupConsumerCloseCompletionObservation::Corrupt,
    };
    GroupConsumerCloseCompletionObservation::Terminal(GroupConsumerCloseTerminal::Failed(
        GroupConsumerCloseTerminalFailure {
            kind,
            broker_code: if matches!(kind, GroupConsumerCloseTerminalFailureKind::BrokerRejected) {
                Some(broker_code)
            } else {
                None
            },
        },
    ))
}
