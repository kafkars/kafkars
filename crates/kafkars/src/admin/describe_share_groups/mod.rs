//! Public Admin API for describing multiple `ShareGroups` in caller order.

mod builder;
mod operation;
mod result;

pub use builder::DescribeShareGroupsBuilder;
pub use operation::DescribeShareGroups;
pub use result::DescribeShareGroupsResult;

#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod result_test;
