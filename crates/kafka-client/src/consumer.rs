//! Declarative facade for group and directly assigned consumer ownership.

mod assigned;
mod assigned_build_error;
mod assigned_builder;
mod assigned_close;
mod assigned_next_event;
mod assigned_recv;
mod assignment;
mod checkpoint;
mod control;
mod event;
mod group;
mod offset_reset;
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
pub use control::ConsumerControl;
pub use event::{
    AssignedConsumerEvent, AssignedConsumerFetchFailureKind, AssignedConsumerFetchFence,
    AssignedConsumerFetchThrottleFailureKind, AssignedConsumerPositionFence,
    AssignedConsumerPositionResolutionFailureKind,
};
pub use group::{Commit, Consumer, ConsumerBuilder, NextBatch};
pub use offset_reset::OffsetReset;
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
mod event_test;
#[cfg(test)]
mod record_batch_test;
#[cfg(test)]
mod record_test;
