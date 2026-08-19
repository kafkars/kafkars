//! Stable scalar and task values shared by one `StreamsGroup` description.

/// One string key-value fact reported for a `StreamsGroup`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamsGroupKeyValue {
    key: String,
    value: String,
}

impl StreamsGroupKeyValue {
    pub(crate) const fn new(key: String, value: String) -> Self {
        Self { key, value }
    }

    /// Returns the exact key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the exact value.
    pub fn value(&self) -> &str {
        &self.value
    }
}

/// One user-defined endpoint for Kafka Streams interactive queries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamsGroupEndpoint {
    host: String,
    port: u16,
}

impl StreamsGroupEndpoint {
    pub(crate) const fn new(host: String, port: u16) -> Self {
        Self { host, port }
    }

    /// Returns the endpoint host exactly as reported by Kafka.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns the endpoint port.
    pub const fn port(&self) -> u16 {
        self.port
    }
}

/// One cumulative task offset.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamsGroupTaskOffset {
    subtopology_id: String,
    partition: i32,
    offset: i64,
}

impl StreamsGroupTaskOffset {
    pub(crate) const fn new(subtopology_id: String, partition: i32, offset: i64) -> Self {
        Self {
            subtopology_id,
            partition,
            offset,
        }
    }

    /// Returns the task's subtopology identity.
    pub fn subtopology_id(&self) -> &str {
        &self.subtopology_id
    }

    /// Returns the exact signed task partition.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the exact cumulative offset.
    pub const fn offset(&self) -> i64 {
        self.offset
    }
}

/// Partitions belonging to tasks in one subtopology.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamsGroupTaskIds {
    subtopology_id: String,
    partitions: Vec<i32>,
}

impl StreamsGroupTaskIds {
    pub(crate) const fn new(subtopology_id: String, partitions: Vec<i32>) -> Self {
        Self {
            subtopology_id,
            partitions,
        }
    }

    /// Returns the task group's subtopology identity.
    pub fn subtopology_id(&self) -> &str {
        &self.subtopology_id
    }

    /// Returns task partitions in deterministic ascending order.
    pub fn partitions(&self) -> &[i32] {
        &self.partitions
    }
}
