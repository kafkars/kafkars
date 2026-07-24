//! Provisional group-consumer construction and checkpoint operations.

use crate::{
    client::Client,
    error::{ErrorKind, KafkaError},
    operation::Operation,
};

use super::{Checkpoint, ConsumerControl, OffsetReset, RecordBatch};

/// Builder for a group-managed consumer.
#[derive(Debug, Clone)]
pub struct ConsumerBuilder {
    client: Client,
    group_id: String,
    topics: Vec<String>,
    offset_reset: OffsetReset,
}

impl ConsumerBuilder {
    pub(crate) fn new(client: Client, group_id: String) -> Self {
        Self {
            client,
            group_id,
            topics: Vec::new(),
            offset_reset: OffsetReset::Error,
        }
    }

    /// Replaces the topic subscription.
    pub fn subscribe<I, S>(mut self, topics: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.topics = topics.into_iter().map(Into::into).collect();
        self
    }

    /// Selects the explicit missing-offset policy.
    pub const fn on_missing_offset(mut self, policy: OffsetReset) -> Self {
        self.offset_reset = policy;
        self
    }

    /// Builds a uniquely controlled consumer.
    pub fn build(self) -> Result<Consumer, KafkaError> {
        if self.group_id.is_empty() {
            return Err(KafkaError::new(
                ErrorKind::Configuration,
                "consumer group id must not be empty",
            ));
        }
        if self.topics.is_empty() {
            return Err(KafkaError::new(
                ErrorKind::Configuration,
                "consumer subscription must contain at least one topic",
            ));
        }

        let _ = self.offset_reset;
        Ok(Consumer {
            client: self.client,
            group_id: self.group_id,
            topics: self.topics,
            control: ConsumerControl::default(),
        })
    }
}

/// Uniquely controlled group consumer.
#[derive(Debug)]
pub struct Consumer {
    client: Client,
    group_id: String,
    topics: Vec<String>,
    control: ConsumerControl,
}

impl Consumer {
    /// Receives the next owned record batch.
    pub fn next_batch(&mut self) -> NextBatch {
        let _ = (&self.client, &self.topics);
        Operation::ready(Ok(None))
    }

    /// Commits next offsets represented by a fenced checkpoint.
    pub fn commit(&mut self, checkpoint: Checkpoint) -> Commit {
        let Checkpoint {
            group_id,
            assignment_epoch: _,
        } = checkpoint;
        if group_id != self.group_id {
            return Operation::ready(Err(KafkaError::new(
                ErrorKind::State,
                "checkpoint belongs to a different consumer group",
            )));
        }
        Operation::ready(Ok(()))
    }

    /// Returns the thread-safe cross-thread control handle.
    pub fn control(&self) -> ConsumerControl {
        self.control.clone()
    }
}

/// Next consumer batch operation.
pub type NextBatch = Operation<Result<Option<RecordBatch>, KafkaError>>;
/// Offset commit operation.
pub type Commit = Operation<Result<(), KafkaError>>;
