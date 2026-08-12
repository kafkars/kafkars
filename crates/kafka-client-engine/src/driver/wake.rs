//! Domain-neutral wake requests and the integrated host's sleep handshake.

use std::{
    error::Error,
    fmt, io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use kafka_driver::WakeHandle;

/// Cloneable engine mechanism for requesting one embedded-reactor turn.
#[derive(Clone, Debug)]
pub(crate) struct ReactorWake {
    shared: Arc<ReactorWakeState>,
}

#[derive(Debug)]
struct ReactorWakeState {
    handle: WakeHandle,
    turn_requested: AtomicBool,
}

impl ReactorWake {
    pub(super) fn new(handle: WakeHandle) -> Self {
        Self {
            shared: Arc::new(ReactorWakeState {
                handle,
                turn_requested: AtomicBool::new(false),
            }),
        }
    }

    /// Publishes turn demand before waking the embedded reactor.
    pub(crate) fn request(&self) -> Result<(), ReactorWakeError> {
        self.shared.turn_requested.store(true, Ordering::Release);
        self.shared.handle.wake().map_err(ReactorWakeError::from_io)
    }

    /// Acknowledges requests that the host is about to inspect.
    pub(super) fn acknowledge_host_turn(&self) {
        self.shared.turn_requested.store(false, Ordering::Release);
    }

    /// Returns whether demand arrived after the host began its current turn.
    pub(super) fn host_turn_requested(&self) -> bool {
        self.shared.turn_requested.load(Ordering::Acquire)
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
