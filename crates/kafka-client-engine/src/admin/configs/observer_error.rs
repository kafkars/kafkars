//! Stable observation errors for one named `DescribeConfigs` completion.

use core::fmt;

/// Failure to observe a named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeConfigsObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for DescribeConfigsObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "DescribeConfigs result was already observed",
            Self::Stale => "DescribeConfigs observer is stale",
        })
    }
}

impl std::error::Error for DescribeConfigsObserverError {}
