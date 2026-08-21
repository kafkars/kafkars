//! Stable client-quota key and numeric value.

/// One finite quota configuration value returned by Kafka.
#[derive(Clone, Debug, PartialEq)]
pub struct ClientQuotaValue {
    key: String,
    value: f64,
}

impl ClientQuotaValue {
    pub(crate) const fn new(key: String, value: f64) -> Self {
        Self { key, value }
    }

    /// Returns Kafka's quota configuration key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns Kafka's finite quota configuration value.
    pub const fn value(&self) -> f64 {
        self.value
    }

    /// Consumes this quota into its key and numeric value.
    pub fn into_parts(self) -> (String, f64) {
        (self.key, self.value)
    }
}
