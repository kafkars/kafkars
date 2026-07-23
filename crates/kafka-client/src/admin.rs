//! Provisional Rust admin API shape; execution is not yet bridged to the engine.

use crate::client::Client;
use crate::error::KafkaError;
use crate::operation::Operation;

/// Cheaply cloneable admin handle.
#[derive(Debug, Clone)]
pub struct Admin {
    client: Client,
}

impl Admin {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Returns the provisional ready-result API probe.
    ///
    /// This facade method is intentionally not yet connected to the live
    /// engine `CreateTopics` path.
    pub fn create_topics<I>(&self, topics: I) -> CreateTopics
    where
        I: IntoIterator<Item = NewTopic>,
    {
        let entries = topics
            .into_iter()
            .map(|topic| {
                let _ = (topic.partitions, topic.replication_factor);
                (topic.name, Ok(()))
            })
            .collect();
        let _ = &self.client;
        Operation::ready(Ok(BatchResult { entries }))
    }
}

/// Topic creation request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTopic {
    name: String,
    partitions: i32,
    replication_factor: i16,
}

impl NewTopic {
    /// Creates a topic request with explicit partition count.
    pub fn new(name: impl Into<String>, partitions: i32) -> Self {
        Self {
            name: name.into(),
            partitions,
            replication_factor: -1,
        }
    }

    /// Sets the desired replication factor.
    pub const fn replication_factor(mut self, replication_factor: i16) -> Self {
        self.replication_factor = replication_factor;
        self
    }

    /// Returns the requested partition count.
    pub const fn partitions(&self) -> i32 {
        self.partitions
    }
}

/// Ordered per-resource outcomes for a batched admin operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchResult<K, V> {
    entries: Vec<(K, Result<V, KafkaError>)>,
}

impl<K, V> BatchResult<K, V> {
    /// Returns ordered resource outcomes.
    pub fn entries(&self) -> &[(K, Result<V, KafkaError>)] {
        &self.entries
    }
}

/// Topic creation operation.
pub type CreateTopics = Operation<Result<BatchResult<String, ()>, KafkaError>>;
