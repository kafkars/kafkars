//! Declarative facade for group and directly assigned consumer ownership.

mod assigned;
mod assigned_builder;
mod assigned_close;
mod assignment;
mod control;
mod group;
mod offset_reset;
mod record_batch;

pub use assigned::AssignedConsumer;
pub use assigned_builder::AssignedConsumerBuilder;
pub use assigned_close::CloseAssignedConsumer;
pub use assignment::{StartPosition, TopicPartition};
pub use control::ConsumerControl;
pub use group::{Commit, Consumer, ConsumerBuilder, NextBatch};
pub use offset_reset::OffsetReset;
pub use record_batch::{Checkpoint, ConsumerRecord, RecordBatch};

#[cfg(test)]
mod assigned_builder_test;
#[cfg(test)]
mod assigned_close_test;
#[cfg(test)]
mod assigned_test;
#[cfg(test)]
mod assignment_test;
