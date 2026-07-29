//! Public Admin API for deleting ShareGroup offsets by topic.

mod builder;
mod operation;
mod result;

pub use builder::DeleteShareGroupOffsetsBuilder;
pub use operation::DeleteShareGroupOffsets;
pub use result::DeleteShareGroupOffsetsResult;

#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod result_test;
