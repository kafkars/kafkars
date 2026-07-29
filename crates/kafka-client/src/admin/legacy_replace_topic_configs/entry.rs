//! Stable nullable entry for one legacy full-snapshot topic replacement.

/// One configuration key and its exact value in a legacy topic snapshot.
///
/// A null value asks Kafka to restore that key's default. An empty string is a
/// present value and remains distinct from null.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyTopicConfigEntry {
    key: String,
    value: Option<String>,
}

impl LegacyTopicConfigEntry {
    /// Creates one snapshot entry with an exact nullable value.
    pub fn new(key: impl Into<String>, value: Option<String>) -> Self {
        Self {
            key: key.into(),
            value,
        }
    }

    /// Creates one present snapshot entry, including an explicit empty value.
    pub fn set(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(key, Some(value.into()))
    }

    /// Creates one null snapshot entry that restores Kafka's default.
    pub fn restore_default(key: impl Into<String>) -> Self {
        Self::new(key, None)
    }

    /// Returns the configuration key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the exact nullable configuration value.
    pub fn value(&self) -> Option<&str> {
        self.value.as_deref()
    }

    pub(crate) fn into_parts(self) -> (String, Option<String>) {
        (self.key, self.value)
    }
}
