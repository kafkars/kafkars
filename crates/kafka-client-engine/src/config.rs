//! Validated engine construction inputs.

/// Validated engine construction inputs retained by the client host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineConfig {
    bootstrap_servers: Vec<String>,
}

impl EngineConfig {
    /// Creates an engine configuration from validated logical endpoints.
    pub fn new(bootstrap_servers: Vec<String>) -> Self {
        Self { bootstrap_servers }
    }

    /// Returns configured logical bootstrap endpoints.
    pub fn bootstrap_servers(&self) -> &[String] {
        &self.bootstrap_servers
    }
}
