//! Public consumer-group description values, builder, result, and operation.

mod assignment;
mod builder;
mod description;
mod description_details;
mod operation;
mod result;

pub use assignment::{ConsumerGroupAssignment, ConsumerGroupTopicPartitions};
pub use builder::DescribeConsumerGroupsBuilder;
pub use description::{ConsumerGroupDescription, ConsumerGroupMember};
pub use description_details::{
    ClassicConsumerGroupDetails, ClassicConsumerGroupMemberDetails,
    ConsumerGroupDescriptionDetails, ConsumerGroupMemberDetails, ConsumerProtocolGroupDetails,
    ConsumerProtocolMemberDetails,
};
pub use operation::DescribeConsumerGroups;
pub use result::DescribeConsumerGroupsResult;

#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod description_test;
#[cfg(test)]
mod operation_test;
