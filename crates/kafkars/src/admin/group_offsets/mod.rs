//! Declarative facade for stable consumer-group offset values and alteration.

mod alter_builder;
mod alter_operation;
mod alter_result;
mod alteration;
mod offset;

pub use alter_builder::AlterConsumerGroupOffsetsBuilder;
pub use alter_operation::AlterConsumerGroupOffsets;
pub use alter_result::AlterConsumerGroupOffsetsResult;
pub use alteration::ConsumerGroupOffsetAlteration;
pub use offset::ConsumerGroupOffset;

#[cfg(test)]
mod alter_builder_test;
#[cfg(test)]
mod alter_operation_test;
#[cfg(test)]
mod alter_result_test;
#[cfg(test)]
mod alteration_test;
#[cfg(test)]
mod offset_test;
