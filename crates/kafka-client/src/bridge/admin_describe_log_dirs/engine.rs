//! Isolated names for the engine-owned DescribeLogDirs adapter contract.

pub(super) use kafka_client_engine::{
    DescribeLogDirDescription as DirectoryDescription,
    DescribeLogDirEngineBrokerError as BrokerError,
    DescribeLogDirEngineOutcome as DirectoryOutcome, DescribeLogDirsAccepted as Accepted,
    DescribeLogDirsAcceptedFaultKind as AcceptedFaultKind,
    DescribeLogDirsAdmissionError as AdmissionError,
    DescribeLogDirsAdmissionErrorKind as AdmissionErrorKind,
    DescribeLogDirsBrokerFailure as BrokerFailure, DescribeLogDirsBrokerFailureKind as FailureKind,
    DescribeLogDirsDeliveryStatus as DeliveryStatus, DescribeLogDirsEngineBatch as Batch,
    DescribeLogDirsEngineBrokerOutcome as BrokerOutcome,
    DescribeLogDirsEngineBrokerResult as BrokerResult, DescribeLogDirsFailure as Failure,
    DescribeLogDirsObserver as Observer, DescribeLogDirsObserverError as ObserverError,
    DescribeLogDirsOutcome as Outcome, DescribeLogDirsReplicaInfo as ReplicaInfo,
    DescribeLogDirsRequest as Request,
};
