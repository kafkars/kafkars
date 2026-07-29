//! Typed Kafka 4.2 Admin facade for classic group descriptions.

mod builder;
mod description;
mod operation;
mod result;

pub use builder::DescribeClassicGroupsBuilder;
pub use description::{ClassicGroupDescription, ClassicGroupMember};
pub use operation::DescribeClassicGroups;
pub use result::DescribeClassicGroupsResult;

#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod description_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod result_test;
