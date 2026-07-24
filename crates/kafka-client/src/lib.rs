//! Idiomatic Rust facade over the shared reactor-native Kafka client engine.
//!
//! Immediate explicit-partition producer admission, stage-aware cancellation,
//! flush observation, atomic close-and-drain, batched topic mutation, and
//! bounded topic and configuration description form the implemented vertical
//! slices. Later API domains remain design probes.

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
    Admin, BatchResult, ClusterBroker, ClusterDescription, ConfigEntry, ConfigSynonym,
    CreatePartitions, CreatePartitionsBuilder, CreateTopics, CreateTopicsBuilder, DeleteTopics,
    DeleteTopicsBuilder, DescribeCluster, DescribeClusterBuilder, DescribeConfigs,
    DescribeConfigsBuilder, DescribeConfigsResult, DescribeTopics, DescribeTopicsBuilder,
    NewPartitions, NewTopic, TopicConfigQuery, TopicDescription, TopicPartitionDescription,
};
pub use client::{Client, ClientBuilder, Shutdown};
pub use consumer::{
    AssignedConsumer, AssignedConsumerBuilder, Checkpoint, CloseAssignedConsumer, Commit, Consumer,
    ConsumerBuilder, ConsumerControl, ConsumerRecord, NextBatch, OffsetReset, RecordBatch,
};
pub use error::{DeliveryStatus, ErrorKind, KafkaError};
pub use operation::Operation;
pub use producer::{
    CancellationOutcome, CloseProducer, Compression, Delivery, Flush, Producer, ProducerBuilder,
    RecordMetadata, TrySendError,
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
#[cfg(test)]
mod silent_broker_test;
