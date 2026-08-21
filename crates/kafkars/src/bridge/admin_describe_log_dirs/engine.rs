//! Isolated names for the engine-owned `DescribeLogDirs` adapter contract.

pub(super) use kafka_client_engine::{
    DescribeLogDirDescription as DirectoryDescription,
    DescribeLogDirEngineBrokerError as BrokerError,
    DescribeLogDirEngineOutcome as DirectoryOutcome, DescribeLogDirTarget as Target,
    DescribeLogDirsAccepted as Accepted, DescribeLogDirsAcceptedFaultKind as AcceptedFaultKind,
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

pub(super) fn target(topic: String, partition: i32) -> Target {
    Target::new(topic, partition)
}

pub(super) fn all_request(broker_ids: Vec<i32>) -> Request {
    Request::all(broker_ids)
}

pub(super) fn selected_request(broker_ids: Vec<i32>, targets: Vec<Target>) -> Request {
    Request::selected(broker_ids, targets)
}
