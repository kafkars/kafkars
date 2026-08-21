//! Stable non-secret SCRAM credential metadata.

/// Exact signed Kafka SCRAM mechanism code.
///
/// The transparent value preserves mechanisms added by future Kafka versions.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ScramMechanism(i8);

impl ScramMechanism {
    /// Kafka's unknown-mechanism sentinel.
    pub const UNKNOWN: Self = Self(0);
    /// SCRAM using SHA-256.
    pub const SHA_256: Self = Self(1);
    /// SCRAM using SHA-512.
    pub const SHA_512: Self = Self(2);

    /// Preserves one exact signed Kafka mechanism code.
    pub const fn from_code(code: i8) -> Self {
        Self(code)
    }

    /// Returns the exact signed Kafka mechanism code.
    pub const fn code(self) -> i8 {
        self.0
    }
}

/// Non-secret facts about one SCRAM credential.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScramCredentialInfo {
    mechanism: ScramMechanism,
    iterations: u32,
}

impl ScramCredentialInfo {
    pub(crate) const fn new(mechanism: ScramMechanism, iterations: u32) -> Self {
        Self {
            mechanism,
            iterations,
        }
    }

    /// Returns the exact Kafka SCRAM mechanism.
    pub const fn mechanism(self) -> ScramMechanism {
        self.mechanism
    }

    /// Returns the credential's positive iteration count.
    pub const fn iterations(self) -> u32 {
        self.iterations
    }
}
