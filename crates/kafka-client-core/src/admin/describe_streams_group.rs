//! Declarative facade for deterministic streams-group description policy.

mod correlation;
mod machine;
mod model;
mod result;
mod transition;

pub use machine::{
    DescribeStreamsGroupEffect, DescribeStreamsGroupInput, DescribeStreamsGroupMachine,
    DescribeStreamsGroupMachineError, DescribeStreamsGroupState, DescribeStreamsGroupTransition,
};
pub(crate) use model::DescribeStreamsGroupPlanShape;
pub use model::{
    DESCRIBE_STREAMS_GROUP_MAX_GROUP_ID_BYTES, DESCRIBE_STREAMS_GROUP_MAX_GROUPS,
    DESCRIBE_STREAMS_GROUP_MAX_REQUEST_TEXT_BYTES, DescribeStreamsGroupPlan,
    DescribeStreamsGroupPlanError,
};
pub use result::{
    DESCRIBE_STREAMS_GROUP_DIAGNOSTIC_BYTES, DESCRIBE_STREAMS_GROUP_MAX_COLLECTION_ITEMS,
    DESCRIBE_STREAMS_GROUP_MAX_PARTITIONS_PER_TASK, DESCRIBE_STREAMS_GROUP_MAX_RESPONSE_TEXT_BYTES,
    DESCRIBE_STREAMS_GROUP_MAX_RETAINED_BYTES, DESCRIBE_STREAMS_GROUP_MAX_SCALAR_BYTES,
    DescribeStreamsGroupAssignment, DescribeStreamsGroupBrokerError,
    DescribeStreamsGroupDescription, DescribeStreamsGroupEndpoint, DescribeStreamsGroupFailure,
    DescribeStreamsGroupFailureKind, DescribeStreamsGroupKeyValue, DescribeStreamsGroupMember,
    DescribeStreamsGroupOutcome, DescribeStreamsGroupResult, DescribeStreamsGroupSubtopology,
    DescribeStreamsGroupTaskIds, DescribeStreamsGroupTaskOffset, DescribeStreamsGroupTerminal,
    DescribeStreamsGroupTopicInfo, DescribeStreamsGroupTopology,
    DescribeStreamsGroupTopologyDescription, DescribeStreamsGroupTopologyDescriptionGlobalStore,
    DescribeStreamsGroupTopologyDescriptionNode, DescribeStreamsGroupTopologyDescriptionStatus,
    DescribeStreamsGroupTopologyDescriptionSubtopology, DescribeStreamsGroupsBatch,
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
