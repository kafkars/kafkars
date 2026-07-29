//! Declarative facade for deterministic share-group description policy.

mod correlation;
mod machine;
mod model;
mod result;
mod transition;

pub use machine::{
    DescribeShareGroupEffect, DescribeShareGroupInput, DescribeShareGroupMachine,
    DescribeShareGroupMachineError, DescribeShareGroupOutcome, DescribeShareGroupState,
    DescribeShareGroupTerminal, DescribeShareGroupTransition, DescribeShareGroupsBatch,
};
pub(crate) use model::DescribeShareGroupPlanShape;
pub use model::{
    DESCRIBE_SHARE_GROUP_MAX_GROUP_ID_BYTES, DESCRIBE_SHARE_GROUP_MAX_GROUPS,
    DESCRIBE_SHARE_GROUP_MAX_REQUEST_TEXT_BYTES, DescribeShareGroupPlan,
    DescribeShareGroupPlanError,
};
pub use result::{
    DESCRIBE_SHARE_GROUP_DIAGNOSTIC_BYTES, DESCRIBE_SHARE_GROUP_MAX_ASSIGNMENT_TOPICS,
    DESCRIBE_SHARE_GROUP_MAX_MEMBERS, DESCRIBE_SHARE_GROUP_MAX_PARTITIONS_PER_TOPIC,
    DESCRIBE_SHARE_GROUP_MAX_RESPONSE_TEXT_BYTES, DESCRIBE_SHARE_GROUP_MAX_RETAINED_BYTES,
    DESCRIBE_SHARE_GROUP_MAX_SCALAR_BYTES, DESCRIBE_SHARE_GROUP_MAX_SUBSCRIPTIONS,
    DescribeShareGroupAssignment, DescribeShareGroupBrokerError, DescribeShareGroupDescription,
    DescribeShareGroupFailure, DescribeShareGroupFailureKind, DescribeShareGroupMember,
    DescribeShareGroupResult, DescribeShareGroupTopicAssignment,
};

#[cfg(test)]
mod correlation_test;
#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod result_test;
#[cfg(test)]
mod transition_test;
