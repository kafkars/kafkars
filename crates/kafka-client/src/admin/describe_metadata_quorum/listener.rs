//! Stable advertised metadata-quorum listener endpoint.

/// One named listener endpoint advertised by a quorum node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataQuorumListener {
    name: String,
    host: String,
    port: u16,
}

impl MetadataQuorumListener {
    pub(crate) const fn new(name: String, host: String, port: u16) -> Self {
        Self { name, host, port }
    }

    /// Returns the listener name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the advertised hostname.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns the advertised nonzero port.
    pub const fn port(&self) -> u16 {
        self.port
    }

    /// Consumes this endpoint into stable generated-free scalar parts.
    pub fn into_parts(self) -> (String, String, u16) {
        (self.name, self.host, self.port)
    }
}
