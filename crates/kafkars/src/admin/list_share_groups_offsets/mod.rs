//! Declarative facade for multi-ShareGroup offset listing.

mod builder;
mod operation;
mod query;
mod result;

pub use builder::ListShareGroupsOffsetsBuilder;
pub use operation::ListShareGroupsOffsets;
pub use query::ListShareGroupOffsetsQuery;
pub use result::ListShareGroupsOffsetsResult;

#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod query_test;
#[cfg(test)]
mod result_test;
