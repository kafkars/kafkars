//! Stable value or omission directive for one legacy full-snapshot replacement.

/// One configuration key and its exact value in a legacy topic snapshot.
///
/// A null value asks the request builder to omit that dynamic override from the
/// complete snapshot so Kafka restores its default. An empty string is a
/// present value and remains distinct from null.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyTopicConfigEntry {
    key: String,
    value: Option<String>,
}

impl LegacyTopicConfigEntry {
    /// Creates one snapshot entry with an exact value or omission directive.
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

    /// Creates one omission directive that restores Kafka's default.
    pub fn restore_default(key: impl Into<String>) -> Self {
        Self::new(key, None)
    }

    /// Returns the configuration key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the value, or `None` for a default-restoration directive.
    pub fn value(&self) -> Option<&str> {
        self.value.as_deref()
    }

    pub(crate) fn into_parts(self) -> (String, Option<String>) {
        (self.key, self.value)
    }
}
