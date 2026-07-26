//! Idiomatic Rust facade over the shared reactor-native Kafka client engine.
//!
//! Immediate explicit-partition producer admission, stage-aware cancellation,
//! flush observation, atomic close-and-drain, batched topic mutation, and
//! bounded topic description, committed group-offset listing and deletion,
//! committed group-offset alteration, configuration description, incremental
//! configuration alteration, and
//! transactional-owner initialization form the implemented vertical slices.
//! Later API domains remain design probes.

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
    Admin, AlterConsumerGroupOffsets, AlterConsumerGroupOffsetsBuilder,
    AlterConsumerGroupOffsetsResult, BatchResult, ClusterBroker, ClusterDescription,
    ConfigAlteration, ConfigAlterationOperation, ConfigEntry, ConfigSynonym, ConsumerGroupOffset,
    ConsumerGroupOffsetAlteration, CreatePartitions, CreatePartitionsBuilder, CreateTopics,
    CreateTopicsBuilder, DeleteConsumerGroupOffsets, DeleteConsumerGroupOffsetsBuilder,
    DeleteConsumerGroupOffsetsResult, DeleteTopics, DeleteTopicsBuilder, DescribeCluster,
    DescribeClusterBuilder, DescribeConfigs, DescribeConfigsBuilder, DescribeConfigsResult,
    DescribeTopics, DescribeTopicsBuilder, IncrementalAlterConfigs, IncrementalAlterConfigsBuilder,
    IncrementalAlterConfigsResult, ListConsumerGroupOffsets, ListConsumerGroupOffsetsBuilder,
    ListConsumerGroupOffsetsResult, ListTopics, ListTopicsBuilder, NewPartitions, NewTopic,
    TopicConfigAlterations, TopicConfigQuery, TopicDescription, TopicPartitionDescription,
};
pub use client::{Client, ClientBuilder, Shutdown};
pub use consumer::{
    AssignedConsumer, AssignedConsumerBuildError, AssignedConsumerBuilder, AssignedConsumerEvent,
    AssignedConsumerFetchFailureKind, AssignedConsumerFetchFence,
    AssignedConsumerFetchThrottleFailureKind, AssignedConsumerPositionFence,
    AssignedConsumerPositionResolutionFailureKind, Checkpoint, CloseAssignedConsumer, Commit,
    Consumer, ConsumerBuilder, ConsumerControl, ConsumerHeader, ConsumerRecord, ConsumerRecords,
    NextAssignedEvent, NextBatch, OffsetReset, ReadIsolation, RecordBatch, RecvAssignedBatch,
    StartPosition, TopicPartition,
};
pub use error::{DeliveryStatus, ErrorKind, KafkaError};
pub use operation::Operation;
pub use producer::{
    CancellationOutcome, CloseProducer, Compression, Delivery, Flush, Producer, ProducerBuilder,
    RecordMetadata, TrySendError,
};
pub use record::{Header, Record};
pub use transaction::{
    InitializeTransactionalProducer, TransactionalProducer, TransactionalProducerBuilder,
    TransactionalProducerIdentity,
};

#[cfg(test)]
mod client_test;
#[cfg(test)]
mod error_test;
#[cfg(test)]
mod silent_broker_test;
