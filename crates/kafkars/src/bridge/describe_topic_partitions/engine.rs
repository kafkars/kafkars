//! Isolated names for the engine-owned Admin `DescribeTopicPartitions` contract.

pub(super) use kafka_client_engine::{
    AdminDescribeTopicPartition as Partition, AdminDescribeTopicPartitionsAccepted as Accepted,
    AdminDescribeTopicPartitionsAcceptedFaultKind as AcceptedFaultKind,
    AdminDescribeTopicPartitionsAdmissionError as AdmissionError,
    AdminDescribeTopicPartitionsAdmissionErrorKind as AdmissionErrorKind,
    AdminDescribeTopicPartitionsCursor as Cursor,
    AdminDescribeTopicPartitionsDeliveryStatus as DeliveryStatus,
    AdminDescribeTopicPartitionsFailure as Failure,
    AdminDescribeTopicPartitionsFailureKind as FailureKind,
    AdminDescribeTopicPartitionsObserver as Observer,
    AdminDescribeTopicPartitionsObserverError as ObserverError,
    AdminDescribeTopicPartitionsOutcome as Outcome, AdminDescribeTopicPartitionsPage as Page,
    AdminDescribeTopicPartitionsRequest as Request, AdminDescribeTopicPartitionsTopic as Topic,
};
