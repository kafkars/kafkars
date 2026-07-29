//! Public Admin API for describing one modern ShareGroup.

mod assignment;
mod builder;
mod description;
mod operation;
mod result;

pub use assignment::{ShareGroupAssignment, ShareGroupTopicPartitions};
pub use builder::DescribeShareGroupBuilder;
pub use description::{ShareGroupDescription, ShareGroupMember};
pub use operation::DescribeShareGroup;
pub use result::DescribeShareGroupResult;

#[cfg(test)]
mod assignment_test;
#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod description_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod result_test;
