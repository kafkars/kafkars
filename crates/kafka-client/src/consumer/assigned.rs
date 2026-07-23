//! Directly assigned consumer ownership.

use crate::client::Client;
use crate::error::KafkaError;
use crate::operation::Operation;

use super::NextBatch;

/// Builder for a consumer with direct partition ownership.
#[derive(Debug, Clone)]
pub struct AssignedConsumerBuilder {
    client: Client,
}

impl AssignedConsumerBuilder {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Builds a directly assigned consumer prototype.
    pub fn build(self) -> Result<AssignedConsumer, KafkaError> {
        Ok(AssignedConsumer {
            client: self.client,
        })
    }
}

/// Consumer whose positions are controlled directly rather than by a group.
#[derive(Debug)]
pub struct AssignedConsumer {
    client: Client,
}

impl AssignedConsumer {
    /// Receives the next directly assigned record batch.
    pub fn next_batch(&mut self) -> NextBatch {
        let _ = &self.client;
        Operation::ready(Ok(None))
    }
}
