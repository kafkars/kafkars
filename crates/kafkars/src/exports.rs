//! Minimal crate-private imports retained while implementation modules use root paths.

pub(crate) use crate::admin::{
    AlterConsumerGroupOffsets, AlterConsumerGroupOffsetsBuilder, AlterConsumerGroupOffsetsResult,
    BatchResult, ConsumerGroupMemberRemoval, ConsumerGroupOffsetAlteration,
    DeleteConsumerGroupOffsets, DeleteConsumerGroupOffsetsBuilder,
    DeleteConsumerGroupOffsetsResult, LeaderElectionTarget, LeaderElectionType,
    PartitionReassignmentChange,
};
pub(crate) use crate::consumer::{
    Checkpoint, ClassicGroupAssignor, ClassicGroupConfig, ConsumerFetchConfig,
    ConsumerGroupProtocol, ConsumerLimits, GroupConsumerOperationConfig, GroupMetadata,
    OffsetReset, ReadIsolation, ShareConsumerFetchConfig, TopicPartition,
};
pub(crate) use crate::error::{DeliveryStatus, Error as KafkaError, ErrorKind};
pub(crate) use crate::producer::{
    CancellationOutcome, RecordMetadata, SendBatchResult, TrySendError,
};
pub(crate) use crate::record::Record;
pub(crate) use crate::security::Tls;
pub(crate) use crate::topic_uuid::TopicUuid;
pub(crate) use crate::transaction::TransactionEndIntent;

#[cfg(test)]
pub(crate) use crate::admin::{
    ConfigAlteration, ConfigResourceAlterations, ConfigResourceQuery, ConfigResourceType,
    ConsumerGroupAssignment, DescribeTopicPartitionsCursor, DescribeTopicsByIdBuilder,
    LegacyConfigResourceReplacement, LegacyTopicConfigEntry, NewTopic, TopicConfigAlterations,
    TopicConfigQuery,
};
#[cfg(test)]
pub(crate) use crate::consumer::{
    AssignedConsumer, AssignedConsumerBuilder, CloseAssignedConsumer, StartPosition,
};
#[cfg(test)]
pub(crate) use crate::error::RetryAdvice;
#[cfg(test)]
pub(crate) use crate::header_name::HeaderName;
#[cfg(test)]
pub(crate) use crate::producer::{
    Compression, Delivery, ProducerConfig, ProducerLimits, ProducerRetryConfig,
};
#[cfg(test)]
pub(crate) use crate::readiness::Ready;
#[cfg(test)]
pub(crate) use crate::record::Header;
#[cfg(test)]
pub(crate) use crate::security::{Sasl, Security};
#[cfg(test)]
pub(crate) use crate::shutdown::Shutdown;
