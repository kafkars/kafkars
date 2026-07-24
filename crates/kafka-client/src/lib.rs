//! Idiomatic Rust facade over the shared reactor-native Kafka client engine.
//!
//! Immediate explicit-partition producer admission, stage-aware cancellation,
//! flush observation, atomic close-and-drain, and batched topic creation form
//! the implemented vertical slices. Later API domains remain design probes.

#![forbid(unsafe_code)]

mod admin;
mod bridge;
mod client;
mod consumer;
mod error;
mod operation;
mod producer;
mod record;
mod transaction;

pub use admin::{
    Admin, BatchResult, ClusterBroker, ClusterDescription, CreateTopics, CreateTopicsBuilder,
    DeleteTopics, DeleteTopicsBuilder, DescribeCluster, DescribeClusterBuilder, NewTopic,
};
pub use client::{Client, ClientBuilder, Shutdown};
pub use consumer::{
    AssignedConsumer, AssignedConsumerBuilder, Checkpoint, Commit, Consumer, ConsumerBuilder,
    ConsumerControl, ConsumerRecord, NextBatch, OffsetReset, RecordBatch,
};
pub use error::{DeliveryStatus, ErrorKind, KafkaError};
pub use operation::Operation;
pub use producer::{
    CancellationOutcome, CloseProducer, Delivery, Flush, Producer, ProducerBuilder, RecordMetadata,
    TrySendError,
};
pub use record::{Header, Record};
pub use transaction::{
    AbortTransaction, BeginTransaction, BeginTransactionProducer, CommitTransaction, Transaction,
    TransactionalProducer, TransactionalProducerBuilder,
};

#[cfg(test)]
mod client_test;
#[cfg(test)]
mod error_test;
