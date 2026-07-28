//! Declarative facade for group and directly assigned consumer ownership.

mod assigned;
mod assigned_build_error;
mod assigned_builder;
mod assigned_close;
mod assigned_next_event;
mod assigned_recv;
mod assignment;
mod checkpoint;
mod consumer_batch;
mod event;
mod group;
mod group_build_error;
mod group_commit;
mod group_commit_error;
mod group_event;
mod group_handle;
mod group_record;
mod group_recv;
mod offset_reset;
mod read_isolation;
mod record;
mod record_batch;

pub use assigned::AssignedConsumer;
pub use assigned_build_error::AssignedConsumerBuildError;
pub use assigned_builder::AssignedConsumerBuilder;
pub use assigned_close::CloseAssignedConsumer;
pub use assigned_next_event::NextAssignedEvent;
pub use assigned_recv::RecvAssignedBatch;
pub use assignment::{StartPosition, TopicPartition};
pub use checkpoint::Checkpoint;
pub use consumer_batch::ConsumerBatch;
pub use event::{
    AssignedConsumerEvent, AssignedConsumerFetchFailureKind, AssignedConsumerFetchFence,
    AssignedConsumerFetchThrottleFailureKind, AssignedConsumerPositionFence,
    AssignedConsumerPositionResolutionFailureKind,
};
pub use group::ConsumerBuilder;
pub use group_build_error::ConsumerBuildError;
pub use group_commit::CommitConsumerCheckpoint;
pub use group_commit_error::{ConsumerCommitAdmissionError, ConsumerCommitError};
pub use group_event::{ConsumerAssignment, ConsumerAssignmentPartition, GroupMetadata};
pub use group_handle::Consumer;
pub use group_record::{GroupConsumerHeader, GroupConsumerRecord, GroupConsumerRecords};
pub use group_recv::RecvConsumerBatch;
pub use offset_reset::OffsetReset;
pub use read_isolation::ReadIsolation;
pub use record::{ConsumerHeader, ConsumerRecord, ConsumerRecords};
pub use record_batch::RecordBatch;

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
mod checkpoint_test;
#[cfg(test)]
mod consumer_batch_test;
#[cfg(test)]
mod event_test;
#[cfg(test)]
mod group_build_error_test;
#[cfg(test)]
mod group_commit_error_test;
#[cfg(test)]
mod group_commit_test;
#[cfg(test)]
mod group_event_test;
#[cfg(test)]
mod group_recv_test;
#[cfg(test)]
mod group_test;
#[cfg(test)]
mod read_isolation_test;
#[cfg(test)]
mod record_batch_test;
#[cfg(test)]
mod record_test;
