//! Typed Streams-group facade over the consumer-group offset query owner.

mod builder;
mod operation;
mod result;

pub use builder::ListStreamsGroupOffsetsBuilder;
pub use operation::ListStreamsGroupOffsets;
pub use result::ListStreamsGroupOffsetsResult;

#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod result_test;
