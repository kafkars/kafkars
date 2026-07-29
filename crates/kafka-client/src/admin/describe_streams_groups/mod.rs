//! Public Admin API for describing multiple StreamsGroups in caller order.

mod builder;
mod operation;
mod result;

pub use builder::DescribeStreamsGroupsBuilder;
pub use operation::DescribeStreamsGroups;
pub use result::DescribeStreamsGroupsResult;

#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod result_test;
