//! Isolated names for the engine-owned `StreamsGroup` description contract.

pub(super) use kafka_client_engine::{
    DescribeStreamsGroupAccepted as Accepted,
    DescribeStreamsGroupAcceptedFaultKind as AcceptedFaultKind,
    DescribeStreamsGroupAdmissionError as AdmissionError,
    DescribeStreamsGroupAdmissionErrorKind as AdmissionErrorKind,
    DescribeStreamsGroupAssignment as Assignment, DescribeStreamsGroupBrokerError as BrokerError,
    DescribeStreamsGroupDeliveryStatus as DeliveryStatus,
    DescribeStreamsGroupDescription as Description, DescribeStreamsGroupEndpoint as Endpoint,
    DescribeStreamsGroupFailure as Failure, DescribeStreamsGroupFailureKind as FailureKind,
    DescribeStreamsGroupKeyValue as KeyValue, DescribeStreamsGroupMember as Member,
    DescribeStreamsGroupObserver as Observer, DescribeStreamsGroupObserverError as ObserverError,
    DescribeStreamsGroupOutcome as Outcome, DescribeStreamsGroupRequest as Request,
    DescribeStreamsGroupSubtopology as Subtopology, DescribeStreamsGroupTaskIds as TaskIds,
    DescribeStreamsGroupTaskOffset as TaskOffset, DescribeStreamsGroupTopicInfo as TopicInfo,
    DescribeStreamsGroupTopology as Topology,
    DescribeStreamsGroupTopologyDescription as TopologyDescription,
    DescribeStreamsGroupTopologyDescriptionGlobalStore as TopologyDescriptionGlobalStore,
    DescribeStreamsGroupTopologyDescriptionNode as TopologyDescriptionNode,
    DescribeStreamsGroupTopologyDescriptionStatus as TopologyDescriptionStatus,
    DescribeStreamsGroupTopologyDescriptionSubtopology as TopologyDescriptionSubtopology,
};

#[cfg(test)]
pub(super) use kafka_client_engine::DescribeStreamsGroupResult as Result;
