//! Credential metadata safe to expose from a SCRAM description response.

/// One exact Kafka SCRAM mechanism code and its positive iteration count.
///
/// This value deliberately contains no salt, salted password, server key,
/// stored key, or other credential material.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScramCredentialInfo {
    mechanism: i8,
    iterations: u32,
}

impl ScramCredentialInfo {
    /// Creates one protocol-normalized credential metadata fact.
    pub const fn new(mechanism: i8, iterations: u32) -> Self {
        Self {
            mechanism,
            iterations,
        }
    }

    /// Returns Kafka's exact signed mechanism code.
    pub const fn mechanism(self) -> i8 {
        self.mechanism
    }

    /// Returns the positive iteration count Kafka reported.
    pub const fn iterations(self) -> u32 {
        self.iterations
    }

    /// Consumes this metadata into its exact scalar parts.
    pub const fn into_parts(self) -> (i8, u32) {
        (self.mechanism, self.iterations)
    }
}
