//! Isolated names for the engine-owned DescribeReplicaLogDirs adapter contract.

pub(super) use kafka_client_engine::{
    DescribeReplicaLogDirsAccepted as Accepted,
    DescribeReplicaLogDirsAcceptedFaultKind as AcceptedFaultKind,
    DescribeReplicaLogDirsAdmissionError as AdmissionError,
    DescribeReplicaLogDirsAdmissionErrorKind as AdmissionErrorKind,
    DescribeReplicaLogDirsBrokerError as BrokerError,
    DescribeReplicaLogDirsDeliveryStatus as DeliveryStatus,
    DescribeReplicaLogDirsEngineBatch as Batch,
    DescribeReplicaLogDirsEngineReplicaOutcome as ReplicaOutcome,
    DescribeReplicaLogDirsEngineReplicaResult as ReplicaResult,
    DescribeReplicaLogDirsFailure as Failure, DescribeReplicaLogDirsFailureKind as FailureKind,
    DescribeReplicaLogDirsObserver as Observer,
    DescribeReplicaLogDirsObserverError as ObserverError, DescribeReplicaLogDirsOutcome as Outcome,
    DescribeReplicaLogDirsRequest as Request, DescribeReplicaLogDirsTarget as Target,
    ReplicaLogDirInfo as Info, ReplicaLogDirLocation as Location,
};

pub(super) fn target(topic: String, partition: i32, broker_id: i32) -> Target {
    Target::new(topic, partition, broker_id)
}

pub(super) fn request(targets: Vec<Target>) -> Request {
    Request::new(targets)
}
