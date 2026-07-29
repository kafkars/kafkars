//! Deterministic policy for caller-ordered consumer-group description.

mod assignment;
mod failure;
mod group_description;
mod machine;
mod member_description;
mod model;
mod outcome;
mod transition;

pub use assignment::{AdminConsumerGroupAssignment, AdminConsumerGroupTopicPartitions};
pub use group_description::{
    AdminClassicConsumerGroupDetails, AdminConsumerGroupDescription,
    AdminConsumerGroupDescriptionDetails, AdminModernConsumerGroupDetails,
};
pub use machine::{
    AdminDescribeConsumerGroupsCallKind, AdminDescribeConsumerGroupsEffect,
    AdminDescribeConsumerGroupsInput, AdminDescribeConsumerGroupsMachine,
    AdminDescribeConsumerGroupsMachineError, AdminDescribeConsumerGroupsState,
    AdminDescribeConsumerGroupsTransition,
};
pub use member_description::{
    AdminClassicConsumerGroupMemberDetails, AdminConsumerGroupDescriptionMember,
    AdminConsumerGroupMemberDetails, AdminModernConsumerGroupMemberDetails,
};
pub use model::{
    AdminDescribeConsumerGroupsPlan, AdminDescribeConsumerGroupsPlanError,
    AdminDescribeConsumerGroupsScope,
};
pub use outcome::{
    AdminConsumerGroupBrokerError, AdminConsumerGroupDescriptionOutcome,
    AdminConsumerGroupDescriptionResult, AdminDescribeConsumerGroupsBatch,
    AdminDescribeConsumerGroupsFailure, AdminDescribeConsumerGroupsFailureKind,
    AdminDescribeConsumerGroupsTerminal,
};

#[cfg(test)]
mod fallback_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod partial_test;
#[cfg(test)]
mod scope_test;
#[cfg(test)]
mod transition_test;
