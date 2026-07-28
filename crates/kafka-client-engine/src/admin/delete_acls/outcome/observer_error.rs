//! Stable failure to observe one named Admin `DeleteAcls` completion.

use core::fmt;

/// Failure to observe a named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteAclsObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for DeleteAclsObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "Admin DeleteAcls result was already observed",
            Self::Stale => "Admin DeleteAcls observer is stale",
        })
    }
}

impl std::error::Error for DeleteAclsObserverError {}
