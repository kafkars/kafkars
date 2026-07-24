//! Accepted-call values and exact failures for the private assigned-consumer port.

use crate::clock::ClockError;

use super::{
    super::assigned_owner_model::AssignedConsumerOwnerError, shard::AssignedConsumerShardLockError,
    wake::AssignedConsumerShardWakeError,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AssignedConsumerAcceptedFaultKind {
    Wake,
}

#[must_use = "accepted assigned-consumer work retains any post-commit host fault"]
pub(crate) struct AssignedConsumerAccepted<T> {
    value: T,
    wake: Option<AssignedConsumerShardWakeError>,
}

impl<T> AssignedConsumerAccepted<T> {
    pub(super) fn new(value: T, wake: Result<(), AssignedConsumerShardWakeError>) -> Self {
        Self {
            value,
            wake: wake.err(),
        }
    }

    pub(crate) const fn fault(&self) -> Option<AssignedConsumerAcceptedFaultKind> {
        if self.wake.is_some() {
            Some(AssignedConsumerAcceptedFaultKind::Wake)
        } else {
            None
        }
    }

    pub(crate) fn into_value(self) -> T {
        self.value
    }
}

#[derive(Debug)]
pub(crate) enum AssignedConsumerPortError {
    Clock(ClockError),
    Closed,
    Lock(AssignedConsumerShardLockError),
    Owner {
        error: AssignedConsumerOwnerError,
        wake: Option<AssignedConsumerShardWakeError>,
    },
}

impl AssignedConsumerPortError {
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the next engine boundary will translate the exact clock failure"
        )
    )]
    pub(crate) const fn clock_error(&self) -> Option<ClockError> {
        match self {
            Self::Clock(error) => Some(*error),
            Self::Closed | Self::Lock(_) | Self::Owner { .. } => None,
        }
    }
}
