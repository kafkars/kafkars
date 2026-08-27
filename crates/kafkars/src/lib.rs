//! Experimental, runtime-neutral Rust client with bounded deterministic operations.
//! Version 0.0.2-rc.1 is a release-candidate preview, not a production or broker-support claim.
//! Apache Kafka and the Kafka logo are trademarks of The Apache Software Foundation.
//! kafkars has no affiliation with or endorsement from the Foundation.
#![forbid(unsafe_code)]
pub mod admin;
mod bridge;
pub mod client;
pub mod consumer;
pub mod error;
mod exports;
mod header_name;
pub mod metrics;
pub mod producer;
mod readiness;
mod record;
pub mod security;
mod shutdown;
pub mod topic;
mod topic_uuid;
pub mod transaction;

pub use admin::Admin;
pub use client::Client;
pub use consumer::Consumer;
pub use error::{Error, Result};
pub(crate) use exports::{
    AlterConsumerGroupOffsets, AlterConsumerGroupOffsetsBuilder, AlterConsumerGroupOffsetsResult,
    BatchResult, CancellationOutcome, Checkpoint, ClassicGroupAssignor, ClassicGroupConfig,
    ConsumerFetchConfig, ConsumerGroupMemberRemoval, ConsumerGroupOffsetAlteration,
    ConsumerGroupProtocol, ConsumerLimits, DeleteConsumerGroupOffsets,
    DeleteConsumerGroupOffsetsBuilder, DeleteConsumerGroupOffsetsResult, DeliveryStatus, ErrorKind,
    GroupConsumerOperationConfig, GroupMetadata, KafkaError, LeaderElectionTarget,
    LeaderElectionType, OffsetReset, PartitionReassignmentChange, ReadIsolation, Record,
    RecordMetadata, SendBatchResult, ShareConsumerFetchConfig, Tls, TopicPartition, TopicUuid,
    TransactionEndIntent, TrySendError,
};
#[cfg(test)]
pub(crate) use exports::{
    AssignedConsumer, AssignedConsumerBuilder, CloseAssignedConsumer, Compression,
    ConfigAlteration, ConfigResourceAlterations, ConfigResourceQuery, ConfigResourceType,
    ConsumerGroupAssignment, Delivery, DescribeTopicPartitionsCursor, DescribeTopicsByIdBuilder,
    Header, HeaderName, LegacyConfigResourceReplacement, LegacyTopicConfigEntry, NewTopic,
    ProducerConfig, ProducerLimits, ProducerRetryConfig, Ready, RetryAdvice, Sasl, Security,
    Shutdown, StartPosition, TopicConfigAlterations, TopicConfigQuery,
};
pub use producer::Producer;
#[cfg(test)]
mod client_test;
#[cfg(test)]
mod error_test;
#[cfg(test)]
mod facade_test;
#[cfg(test)]
mod header_name_test;
#[cfg(test)]
mod readiness_test;
#[cfg(test)]
mod record_test;
#[cfg(test)]
mod shutdown_test;
#[cfg(test)]
mod silent_broker_test;
#[cfg(test)]
mod topic_uuid_test;
