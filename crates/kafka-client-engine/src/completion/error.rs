//! Explicit failures at completion reservation and observation boundaries.

use std::{error::Error, fmt};

/// Failure to reserve or settle an engine completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompletionRegistryError {
    /// Every fixed slot still owns an admitted operation or retained result.
    Full,
    /// The identity does not name the currently reserved slot generation.
    UnknownCompletion,
    /// The named completion already owns a terminal result or reclaim signal.
    DuplicatePublish,
    /// The bounded notifier queue could not accept a terminal immediately.
    NotificationBackpressure,
    /// An accepted operation has not supplied its terminal result.
    UnsettledCompletion,
    /// Notification has already stopped during terminal host shutdown.
    NotifierStopped,
    /// A slot exhausted its generation fence and was permanently retired.
    GenerationExhausted,
    /// The bounded reclaim channel disconnected before registry teardown.
    ReclaimDisconnected,
}

impl fmt::Display for CompletionRegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full => formatter.write_str("completion registry is full"),
            Self::UnknownCompletion => formatter.write_str("completion identity is stale"),
            Self::DuplicatePublish => formatter.write_str("completion is already terminal"),
            Self::NotificationBackpressure => {
                formatter.write_str("completion notifier is temporarily full")
            }
            Self::UnsettledCompletion => {
                formatter.write_str("an accepted completion is not terminal")
            }
            Self::NotifierStopped => formatter.write_str("completion notifier has stopped"),
            Self::GenerationExhausted => {
                formatter.write_str("completion slot generation is exhausted")
            }
            Self::ReclaimDisconnected => {
                formatter.write_str("completion reclaim channel disconnected")
            }
        }
    }
}

impl Error for CompletionRegistryError {}

/// Failure while a single observer obtains its terminal value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompletionObserverError {
    /// The observer was polled or waited after already taking its value.
    AlreadyObserved,
    /// The slot no longer belongs to this observer generation.
    Stale,
}

impl fmt::Display for CompletionObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyObserved => formatter.write_str("completion was already observed"),
            Self::Stale => formatter.write_str("completion observer is stale"),
        }
    }
}

impl Error for CompletionObserverError {}
