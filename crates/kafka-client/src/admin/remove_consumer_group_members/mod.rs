//! Public static consumer-group member removal model and operation.

mod builder;
mod model;
mod operation;
mod result;

pub use builder::RemoveConsumerGroupMembersBuilder;
pub use model::ConsumerGroupMemberRemoval;
pub use operation::RemoveConsumerGroupMembers;
pub use result::RemoveConsumerGroupMembersResult;

#[cfg(test)]
mod model_test;
