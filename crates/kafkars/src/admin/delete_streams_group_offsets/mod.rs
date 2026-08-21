//! Typed Streams-group facade over the consumer-group offset-deletion path.

mod builder;
mod operation;
mod result;

pub use builder::DeleteStreamsGroupOffsetsBuilder;
pub use operation::DeleteStreamsGroupOffsets;
pub use result::DeleteStreamsGroupOffsetsResult;

#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod result_test;
