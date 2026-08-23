//! Declarative facade for group and directly assigned consumer ownership.

mod assigned;
mod assigned_build_error;
mod assigned_builder;
mod assigned_close;
mod assigned_next_event;
mod assigned_recv;
mod assignment;
mod checkpoint;
mod checkpoint_builder;
mod classic_group_config;
mod consumer_batch;
mod event;
mod fetch_config;
mod group;
mod group_acknowledge;
mod group_acknowledge_error;
mod group_build_error;
mod group_close;
mod group_close_error;
mod group_commit;
mod group_commit_error;
mod group_control;
mod group_event;
mod group_handle;
mod group_next_event;
mod group_operation_config;
mod group_rebalance_event;
mod group_record;
mod group_recv;
mod group_seek;
mod limits;
mod offset_reset;
mod read_isolation;
mod record;
mod record_batch;
mod share_assignment;
mod share_build_error;
mod share_builder;
mod share_close;
mod share_close_error;
mod share_fetch_config;
mod share_handle;

pub use assigned::AssignedConsumer;
pub use assigned_build_error::AssignedConsumerBuildError;
pub use assigned_builder::AssignedConsumerBuilder;
pub use assigned_close::CloseAssignedConsumer;
pub use assigned_next_event::NextAssignedEvent;
pub use assigned_recv::RecvAssignedBatch;
pub use assignment::{StartPosition, TopicPartition};
pub use checkpoint::Checkpoint;
pub use checkpoint_builder::{CheckpointBuilder, CheckpointMarkError, CheckpointMarkErrorKind};
pub use classic_group_config::ClassicGroupConfig;
pub use consumer_batch::ConsumerBatch;
pub use event::{
    AssignedConsumerEvent, AssignedConsumerFetchFailureKind, AssignedConsumerFetchFence,
    AssignedConsumerFetchThrottleFailureKind, AssignedConsumerPositionFence,
    AssignedConsumerPositionResolutionFailureKind,
};
pub use fetch_config::ConsumerFetchConfig;
pub use group::{ClassicGroupAssignor, ConsumerBuilder, ConsumerGroupProtocol};
pub use group_acknowledge_error::ConsumerAcknowledgeError;
pub use group_build_error::ConsumerBuildError;
pub use group_close::CloseConsumer;
pub use group_close_error::ConsumerCloseAdmissionError;
pub use group_commit::CommitConsumerCheckpoint;
pub use group_commit_error::{ConsumerCommitAdmissionError, ConsumerCommitError};
pub use group_control::ConsumerControl;
pub use group_event::{
    ConsumerAssignment, ConsumerAssignmentPartition, GroupMembershipEpoch, GroupMetadata,
};
pub use group_handle::Consumer;
pub use group_next_event::NextConsumerEvent;
pub use group_operation_config::GroupConsumerOperationConfig;
pub use group_rebalance_event::{ConsumerEvent, ConsumerRevocation};
pub use group_record::{GroupConsumerHeader, GroupConsumerRecord, GroupConsumerRecords};
pub use group_recv::RecvConsumerBatch;
pub use group_seek::Seek;
pub use limits::ConsumerLimits;
pub use offset_reset::OffsetReset;
pub use read_isolation::ReadIsolation;
pub use record::{ConsumerHeader, ConsumerRecord, ConsumerRecords};
pub use record_batch::RecordBatch;
pub use share_assignment::{ShareConsumerAssignment, ShareConsumerAssignmentPartition};
pub use share_build_error::ShareConsumerBuildError;
pub use share_builder::ShareConsumerBuilder;
pub use share_close::CloseShareConsumer;
pub use share_close_error::ShareConsumerCloseAdmissionError;
pub use share_fetch_config::ShareConsumerFetchConfig;
pub use share_handle::ShareConsumer;

#[cfg(test)]
mod assigned_build_error_test;
#[cfg(test)]
mod assigned_builder_test;
#[cfg(test)]
mod assigned_close_test;
#[cfg(test)]
mod assigned_next_event_test;
#[cfg(test)]
mod assigned_recv_test;
#[cfg(test)]
mod assigned_test;
#[cfg(test)]
mod assignment_test;
#[cfg(test)]
mod checkpoint_builder_test;
#[cfg(test)]
mod checkpoint_test;
#[cfg(test)]
mod classic_group_config_test;
#[cfg(test)]
mod consumer_batch_test;
#[cfg(test)]
mod event_test;
#[cfg(test)]
mod fetch_config_test;
#[cfg(test)]
mod group_acknowledge_error_test;
#[cfg(test)]
mod group_acknowledge_test;
#[cfg(test)]
mod group_build_error_test;
#[cfg(test)]
mod group_close_error_test;
#[cfg(test)]
mod group_close_test;
#[cfg(test)]
mod group_commit_error_test;
#[cfg(test)]
mod group_commit_test;
#[cfg(test)]
mod group_control_test;
#[cfg(test)]
mod group_event_test;
#[cfg(test)]
mod group_next_event_test;
#[cfg(test)]
mod group_operation_config_test;
#[cfg(test)]
mod group_rebalance_event_test;
#[cfg(test)]
mod group_recv_test;
#[cfg(test)]
mod group_seek_test;
#[cfg(test)]
mod group_startup_error_test;
#[cfg(test)]
mod group_test;
#[cfg(test)]
mod limits_test;
#[cfg(test)]
mod read_isolation_test;
#[cfg(test)]
mod record_batch_test;
#[cfg(test)]
mod record_test;
#[cfg(test)]
mod share_assignment_test;
#[cfg(test)]
mod share_builder_test;
#[cfg(test)]
mod share_close_test;
#[cfg(test)]
mod share_fetch_config_test;
#[cfg(test)]
mod share_test;
