//! Stable Rust vocabulary for one incremental topic configuration change.

/// Exact Kafka operation applied to one named topic configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigAlterationOperation {
    /// Replaces the current value.
    Set(String),
    /// Removes the explicit value.
    Delete,
    /// Appends using Kafka's configuration semantics.
    Append(String),
    /// Subtracts using Kafka's configuration semantics.
    Subtract(String),
}

impl ConfigAlterationOperation {
    /// Returns the exact value, with absence reserved exclusively for deletion.
    pub fn value(&self) -> Option<&str> {
        match self {
            Self::Set(value) | Self::Append(value) | Self::Subtract(value) => Some(value),
            Self::Delete => None,
        }
    }
}

/// One named topic configuration change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigAlteration {
    key: String,
    operation: ConfigAlterationOperation,
}

impl ConfigAlteration {
    /// Creates one change from its stable operation representation.
    pub fn new(key: impl Into<String>, operation: ConfigAlterationOperation) -> Self {
        Self {
            key: key.into(),
            operation,
        }
    }

    /// Replaces one configuration value.
    pub fn set(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(key, ConfigAlterationOperation::Set(value.into()))
    }

    /// Removes one explicit configuration value.
    pub fn delete(key: impl Into<String>) -> Self {
        Self::new(key, ConfigAlterationOperation::Delete)
    }

    /// Appends to one configuration using Kafka semantics.
    pub fn append(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(key, ConfigAlterationOperation::Append(value.into()))
    }

    /// Subtracts from one configuration using Kafka semantics.
    pub fn subtract(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(key, ConfigAlterationOperation::Subtract(value.into()))
    }

    /// Returns the configuration key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the exact requested operation.
    pub const fn operation(&self) -> &ConfigAlterationOperation {
        &self.operation
    }

    pub(crate) fn into_parts(self) -> (String, ConfigAlterationOperation) {
        (self.key, self.operation)
    }
}
