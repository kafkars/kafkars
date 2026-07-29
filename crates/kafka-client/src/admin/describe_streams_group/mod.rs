//! Public Admin API for describing one modern StreamsGroup.

mod assignment;
mod builder;
mod description;
mod operation;
mod result;
mod topology;
mod topology_description;
mod values;

pub use assignment::StreamsGroupAssignment;
pub use builder::DescribeStreamsGroupBuilder;
pub use description::{StreamsGroupDescription, StreamsGroupMember};
pub use operation::DescribeStreamsGroup;
pub use result::DescribeStreamsGroupResult;
pub use topology::{StreamsGroupSubtopology, StreamsGroupTopicInfo, StreamsGroupTopology};
pub use topology_description::{
    StreamsGroupTopologyDescription, StreamsGroupTopologyDescriptionStatus,
    StreamsGroupTopologyDescriptionSubtopology, StreamsGroupTopologyGlobalStore,
    StreamsGroupTopologyNode, StreamsGroupTopologyNodeType,
};
pub use values::{
    StreamsGroupEndpoint, StreamsGroupKeyValue, StreamsGroupTaskIds, StreamsGroupTaskOffset,
};

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
#[cfg(test)]
mod topology_description_test;
#[cfg(test)]
mod topology_test;
#[cfg(test)]
mod values_test;
