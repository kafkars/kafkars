//! Isolated names for the engine-owned DescribeMetadataQuorum adapter contract.

pub(super) use kafka_client_engine::{
    DescribeMetadataQuorumAccepted as Accepted,
    DescribeMetadataQuorumAcceptedFaultKind as AcceptedFaultKind,
    DescribeMetadataQuorumAdmissionError as AdmissionError,
    DescribeMetadataQuorumAdmissionErrorKind as AdmissionErrorKind,
    DescribeMetadataQuorumBrokerError as BrokerError,
    DescribeMetadataQuorumDeliveryStatus as DeliveryStatus,
    DescribeMetadataQuorumDescription as Description, DescribeMetadataQuorumFailure as Failure,
    DescribeMetadataQuorumFailureKind as FailureKind, DescribeMetadataQuorumListener as Listener,
    DescribeMetadataQuorumNode as Node, DescribeMetadataQuorumObserver as Observer,
    DescribeMetadataQuorumObserverError as ObserverError, DescribeMetadataQuorumOutcome as Outcome,
    DescribeMetadataQuorumPartitionError as PartitionError,
    DescribeMetadataQuorumReplica as Replica,
};
