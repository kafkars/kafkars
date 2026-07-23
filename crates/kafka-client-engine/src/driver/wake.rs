//! Domain-neutral access to the embedded reactor's coalescing wake source.

use std::{error::Error, fmt, io};

use kafka_driver::WakeHandle;

/// Cloneable engine mechanism for requesting one embedded-reactor turn.
#[derive(Clone, Debug)]
pub(crate) struct ReactorWake {
    handle: WakeHandle,
}

impl ReactorWake {
    pub(super) const fn new(handle: WakeHandle) -> Self {
        Self { handle }
    }

    /// Requests one coalesced embedded-reactor turn.
    pub(crate) fn request(&self) -> Result<(), ReactorWakeError> {
        self.handle.wake().map_err(ReactorWakeError::from_io)
    }
}

/// Exact operating-system failure returned by the shared reactor wake.
#[derive(Debug)]
pub(crate) struct ReactorWakeError {
    source: io::Error,
}

impl ReactorWakeError {
    const fn from_io(source: io::Error) -> Self {
        Self { source }
    }

    /// Returns the original operating-system error category.
    #[cfg(test)]
    pub(crate) fn kind(&self) -> io::ErrorKind {
        self.source.kind()
    }

    /// Transfers the original error into one concrete domain adapter.
    pub(crate) fn into_io(self) -> io::Error {
        self.source
    }

    #[cfg(test)]
    pub(crate) const fn from_io_for_test(source: io::Error) -> Self {
        Self::from_io(source)
    }
}

impl fmt::Display for ReactorWakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "embedded reactor wake failed: {}", self.source)
    }
}

impl Error for ReactorWakeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}
