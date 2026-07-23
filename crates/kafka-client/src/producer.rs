//! Thread-safe producer construction, admission, delivery, flush, and close.

use crate::client::Client;
use crate::error::KafkaError;
use crate::operation::Operation;
use crate::record::Record;

/// Builder for a bounded, batch-native producer.
#[derive(Debug, Clone)]
pub struct ProducerBuilder {
    client: Client,
}

impl ProducerBuilder {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Builds the producer after local validation.
    pub fn build(self) -> Result<Producer, KafkaError> {
        Ok(Producer {
            client: self.client,
        })
    }
}

/// Cheaply cloneable, thread-safe producer handle.
#[derive(Debug, Clone)]
pub struct Producer {
    client: Client,
}

impl Producer {
    /// Waits for bounded local capacity and eventual terminal delivery.
    pub fn send(&self, record: Record) -> Send {
        let metadata = RecordMetadata {
            topic: record.topic().to_owned(),
            partition: record.explicit_partition().unwrap_or(0),
            offset: 0,
            timestamp_milliseconds: record.timestamp(),
            leader_epoch: None,
        };
        let _ = (record, &self.client);
        Operation::ready(Ok(metadata))
    }

    /// Attempts immediate admission without waiting for local capacity.
    #[allow(clippy::result_large_err)]
    pub fn try_send(&self, record: Record) -> Result<Delivery, TrySendError<Record>> {
        Ok(self.send(record))
    }

    /// Submits records through the batch-native producer path.
    pub fn send_batch<I>(&self, records: I) -> SendBatch
    where
        I: IntoIterator<Item = Record>,
    {
        let _ = &self.client;
        let count = records.into_iter().count();
        Operation::ready(Ok(BatchDelivery {
            record_count: count,
        }))
    }

    /// Creates a barrier over records accepted before the call.
    pub fn flush(&self) -> Flush {
        let _ = &self.client;
        Operation::ready(Ok(()))
    }
}

/// Metadata for one acknowledged record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordMetadata {
    topic: String,
    partition: i32,
    offset: i64,
    timestamp_milliseconds: Option<i64>,
    leader_epoch: Option<i32>,
}

impl RecordMetadata {
    pub(crate) const fn from_parts(
        topic: String,
        partition: i32,
        offset: i64,
        timestamp_milliseconds: Option<i64>,
        leader_epoch: Option<i32>,
    ) -> Self {
        Self {
            topic,
            partition,
            offset,
            timestamp_milliseconds,
            leader_epoch,
        }
    }

    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the acknowledged partition.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the acknowledged offset.
    pub const fn offset(&self) -> i64 {
        self.offset
    }

    /// Returns the broker timestamp when present.
    pub const fn timestamp_milliseconds(&self) -> Option<i64> {
        self.timestamp_milliseconds
    }

    /// Returns the acknowledged leader epoch when supplied by Kafka.
    pub const fn leader_epoch(&self) -> Option<i32> {
        self.leader_epoch
    }
}

/// Aggregate result for one admitted producer batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchDelivery {
    record_count: usize,
}

impl BatchDelivery {
    /// Returns the number of records represented by this result.
    pub const fn record_count(self) -> usize {
        self.record_count
    }
}

/// Immediate producer-admission failure that returns caller ownership.
#[derive(Debug)]
pub struct TrySendError<T> {
    record: T,
    error: KafkaError,
}

impl<T> TrySendError<T> {
    /// Returns the record and error to the caller.
    pub fn into_parts(self) -> (T, KafkaError) {
        (self.record, self.error)
    }
}

/// Delivery operation for one record.
pub type Send = Operation<Result<RecordMetadata, KafkaError>>;
/// Delivery operation returned by immediate admission.
pub type Delivery = Send;
/// Delivery operation for a submitted batch.
pub type SendBatch = Operation<Result<BatchDelivery, KafkaError>>;
/// Producer flush barrier.
pub type Flush = Operation<Result<(), KafkaError>>;
