//! Isolated names for the engine-owned `ShareGroup` description contract.

pub(super) use kafka_client_engine::{
    DescribeShareGroupAccepted as Accepted,
    DescribeShareGroupAcceptedFaultKind as AcceptedFaultKind,
    DescribeShareGroupAdmissionError as AdmissionError,
    DescribeShareGroupAdmissionErrorKind as AdmissionErrorKind,
    DescribeShareGroupAssignment as Assignment, DescribeShareGroupBrokerError as BrokerError,
    DescribeShareGroupDeliveryStatus as DeliveryStatus,
    DescribeShareGroupDescription as Description, DescribeShareGroupFailure as Failure,
    DescribeShareGroupFailureKind as FailureKind, DescribeShareGroupMember as Member,
    DescribeShareGroupObserver as Observer, DescribeShareGroupObserverError as ObserverError,
    DescribeShareGroupOutcome as Outcome, DescribeShareGroupRequest as Request,
    DescribeShareGroupTopicAssignment as TopicAssignment,
};
