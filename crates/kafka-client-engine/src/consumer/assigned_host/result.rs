//! Stable close admission and exact private assigned-consumer port results.

use std::fmt;

use kafka_client_core::AssignedConsumerMachineError;

use crate::clock::ClockError;
use crate::completion::CompletionRegistryError;

use super::{
    super::assigned_owner_model::AssignedConsumerOwnerError, AssignedConsumerCloseObserver,
    shard::AssignedConsumerShardLockError, wake::AssignedConsumerShardWakeError,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AssignedConsumerPortAcceptedFaultKind {
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

    pub(crate) const fn fault(&self) -> Option<AssignedConsumerPortAcceptedFaultKind> {
        if self.wake.is_some() {
            Some(AssignedConsumerPortAcceptedFaultKind::Wake)
        } else {
            None
        }
    }

    pub(crate) fn into_value(self) -> T {
        self.value
    }
}

/// Stable post-acceptance degradation that cannot revoke close ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AssignedConsumerAcceptedFaultKind {
    /// Close was accepted, but requesting an immediate host turn failed.
    Wake,
}

/// Accepted assigned-consumer close and its single terminal observer.
#[must_use = "accepted close work must retain or deliberately abandon its observer"]
pub struct AssignedConsumerTryCloseAccepted {
    observer: AssignedConsumerCloseObserver,
    fault: Option<AssignedConsumerAcceptedFaultKind>,
}

impl AssignedConsumerTryCloseAccepted {
    /// Returns post-acceptance degradation without reclassifying close as rejected.
    pub const fn fault(&self) -> Option<AssignedConsumerAcceptedFaultKind> {
        self.fault
    }

    /// Transfers the sole terminal observer to the caller.
    pub fn into_observer(self) -> AssignedConsumerCloseObserver {
        self.observer
    }

    pub(super) fn from_port(
        accepted: AssignedConsumerAccepted<AssignedConsumerCloseObserver>,
    ) -> Self {
        let fault = accepted.fault().map(|fault| match fault {
            AssignedConsumerPortAcceptedFaultKind::Wake => AssignedConsumerAcceptedFaultKind::Wake,
        });
        Self {
            observer: accepted.into_value(),
            fault,
        }
    }
}

impl fmt::Debug for AssignedConsumerTryCloseAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AssignedConsumerTryCloseAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}

/// Stable reason an assigned-consumer close did not cross admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AssignedConsumerTryCloseErrorKind {
    /// Another caller or the host currently owns the assigned-consumer shard.
    Contended,
    /// Assigned-consumer admission is permanently closed.
    Closed,
    /// The sole terminal-completion reservation is still occupied.
    CompletionCapacity,
    /// Earlier accepted effects must be interpreted before close can be admitted.
    Pending,
    /// The synchronized host can no longer execute new assigned-consumer work.
    HostUnavailable,
    /// A non-semantic engine mechanism violated its ownership contract.
    InternalInvariant,
}

/// Immediate assigned-consumer close rejection before ownership crossed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AssignedConsumerTryCloseError {
    kind: AssignedConsumerTryCloseErrorKind,
}

impl AssignedConsumerTryCloseError {
    /// Returns the stable rejection category.
    pub const fn kind(&self) -> AssignedConsumerTryCloseErrorKind {
        self.kind
    }

    pub(super) const fn from_port(error: &AssignedConsumerPortError) -> Self {
        Self {
            kind: close_admission_error_kind(error),
        }
    }
}

impl fmt::Display for AssignedConsumerTryCloseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "assigned-consumer close admission failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for AssignedConsumerTryCloseError {}

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

const fn close_admission_error_kind(
    error: &AssignedConsumerPortError,
) -> AssignedConsumerTryCloseErrorKind {
    match error {
        AssignedConsumerPortError::Closed
        | AssignedConsumerPortError::Owner {
            error: AssignedConsumerOwnerError::Core(AssignedConsumerMachineError::ConsumerClosed),
            ..
        } => AssignedConsumerTryCloseErrorKind::Closed,
        AssignedConsumerPortError::Lock(AssignedConsumerShardLockError::Contended) => {
            AssignedConsumerTryCloseErrorKind::Contended
        }
        AssignedConsumerPortError::Owner {
            error: AssignedConsumerOwnerError::EffectsPending,
            ..
        } => AssignedConsumerTryCloseErrorKind::Pending,
        AssignedConsumerPortError::Owner {
            error: AssignedConsumerOwnerError::Completion(CompletionRegistryError::Full),
            ..
        } => AssignedConsumerTryCloseErrorKind::CompletionCapacity,
        AssignedConsumerPortError::Lock(
            AssignedConsumerShardLockError::Poisoned | AssignedConsumerShardLockError::OwnerMissing,
        )
        | AssignedConsumerPortError::Owner {
            error:
                AssignedConsumerOwnerError::Faulted
                | AssignedConsumerOwnerError::Completion(
                    CompletionRegistryError::NotifierStopped
                    | CompletionRegistryError::GenerationExhausted
                    | CompletionRegistryError::ReclaimDisconnected,
                ),
            ..
        } => AssignedConsumerTryCloseErrorKind::HostUnavailable,
        AssignedConsumerPortError::Clock(_) | AssignedConsumerPortError::Owner { .. } => {
            AssignedConsumerTryCloseErrorKind::InternalInvariant
        }
    }
}
