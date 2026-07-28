//! Declarative facade for public Admin `ListOffsets` values and operation.

mod builder;
mod operation;
mod query;
mod result;
mod spec;
mod value;

pub use builder::ListOffsetsBuilder;
pub use operation::ListOffsets;
pub use query::ListOffsetsQuery;
pub use result::ListOffsetsResult;
pub use spec::OffsetSpec;
pub use value::ListOffsetsResultInfo;

#[cfg(test)]
mod query_test;
#[cfg(test)]
mod spec_test;
#[cfg(test)]
mod value_test;
