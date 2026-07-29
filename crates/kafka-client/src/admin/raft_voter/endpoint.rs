//! Stable metadata-quorum voter endpoint.

/// One named network endpoint through which the quorum can contact a voter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaftVoterEndpoint {
    listener: String,
    host: String,
    port: u16,
}

impl RaftVoterEndpoint {
    /// Creates one inert endpoint validated when AddRaftVoter is submitted.
    pub fn new(listener: impl Into<String>, host: impl Into<String>, port: u16) -> Self {
        Self {
            listener: listener.into(),
            host: host.into(),
            port,
        }
    }

    /// Returns the Kafka listener name.
    pub fn listener(&self) -> &str {
        &self.listener
    }

    /// Returns the voter host exactly as supplied.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns the voter port.
    pub const fn port(&self) -> u16 {
        self.port
    }

    pub(crate) fn into_parts(self) -> (String, String, u16) {
        (self.listener, self.host, self.port)
    }
}
