//! Declarative facade for typed multi-Streams-group OffsetFetch operations.

mod builder;
mod operation;
mod result;

pub use builder::ListStreamsGroupsOffsetsBuilder;
pub use operation::ListStreamsGroupsOffsets;
pub use result::ListStreamsGroupsOffsetsResult;

#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod result_test;
