//! Public Admin API for altering `ShareGroup` partition offsets.

mod alteration;
mod builder;
mod operation;
mod result;

pub use alteration::ShareGroupOffsetAlteration;
pub use builder::AlterShareGroupOffsetsBuilder;
pub use operation::AlterShareGroupOffsets;
pub use result::AlterShareGroupOffsetsResult;

#[cfg(test)]
mod alteration_test;
#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod result_test;
