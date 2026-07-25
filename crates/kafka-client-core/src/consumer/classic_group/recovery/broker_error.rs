//! Exact nonzero Kafka error codes observed by classic group stages.

use core::num::NonZeroI16;

/// One broker rejection that retains known and future Kafka error codes.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ClassicBrokerError(NonZeroI16);

impl ClassicBrokerError {
    /// Restores one rejection while reserving zero for protocol success.
    pub const fn try_from_code(code: i16) -> Option<Self> {
        match NonZeroI16::new(code) {
            Some(code) => Some(Self(code)),
            None => None,
        }
    }

    /// Returns the exact Kafka error code.
    pub const fn code(self) -> i16 {
        self.0.get()
    }
}
