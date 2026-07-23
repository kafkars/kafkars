//! Stable Rust construction of one topic in a batched creation request.

/// Topic creation request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTopic {
    name: String,
    partitions: i32,
    replication_factor: i16,
    configs: Vec<(String, String)>,
}

impl NewTopic {
    /// Creates a topic request with explicit partition count.
    pub fn new(name: impl Into<String>, partitions: i32) -> Self {
        Self {
            name: name.into(),
            partitions,
            replication_factor: -1,
            configs: Vec::new(),
        }
    }

    /// Sets the desired replication factor.
    #[must_use]
    pub const fn replication_factor(mut self, replication_factor: i16) -> Self {
        self.replication_factor = replication_factor;
        self
    }

    /// Appends one named topic configuration.
    #[must_use]
    pub fn config(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.configs.push((name.into(), value.into()));
        self
    }

    /// Returns the requested topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the requested partition count.
    pub const fn partitions(&self) -> i32 {
        self.partitions
    }

    /// Returns the requested replication factor or Kafka's default sentinel.
    pub const fn requested_replication_factor(&self) -> i16 {
        self.replication_factor
    }

    pub(crate) fn into_parts(self) -> (String, i32, i16, Vec<(String, String)>) {
        (
            self.name,
            self.partitions,
            self.replication_factor,
            self.configs,
        )
    }
}
