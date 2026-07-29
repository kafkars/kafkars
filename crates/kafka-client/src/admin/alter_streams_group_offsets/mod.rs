//! Typed StreamsGroup offset alteration over the consumer-group execution path.

mod builder;
mod operation;
mod result;

pub use builder::AlterStreamsGroupOffsetsBuilder;
pub use operation::AlterStreamsGroupOffsets;
pub use result::AlterStreamsGroupOffsetsResult;

#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod result_test;
