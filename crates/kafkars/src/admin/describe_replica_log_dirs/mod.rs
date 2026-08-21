//! Public selected-replica log-directory descriptions, builder, and operation.

mod builder;
mod info;
mod location;
mod operation;
mod result;

pub use builder::DescribeReplicaLogDirsBuilder;
pub use info::ReplicaLogDirInfo;
pub use location::ReplicaLogDirLocation;
pub use operation::DescribeReplicaLogDirs;
pub use result::DescribeReplicaLogDirsResult;

#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod info_test;
#[cfg(test)]
mod location_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod result_test;
