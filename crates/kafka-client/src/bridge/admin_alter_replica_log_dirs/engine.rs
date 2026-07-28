//! Isolated names for the engine-owned AlterReplicaLogDirs adapter contract.

pub(super) use kafka_client_engine::{
    AlterReplicaLogDirAssignment as Assignment, AlterReplicaLogDirEngineBrokerError as BrokerError,
    AlterReplicaLogDirEngineOutcome as ReplicaOutcome,
    AlterReplicaLogDirEngineResult as ReplicaResult, AlterReplicaLogDirsAccepted as Accepted,
    AlterReplicaLogDirsAcceptedFaultKind as AcceptedFaultKind,
    AlterReplicaLogDirsAdmissionError as AdmissionError,
    AlterReplicaLogDirsAdmissionErrorKind as AdmissionErrorKind,
    AlterReplicaLogDirsDeliveryStatus as DeliveryStatus, AlterReplicaLogDirsEngineBatch as Batch,
    AlterReplicaLogDirsFailure as Failure, AlterReplicaLogDirsFailureKind as FailureKind,
    AlterReplicaLogDirsObserver as Observer, AlterReplicaLogDirsObserverError as ObserverError,
    AlterReplicaLogDirsOutcome as Outcome, AlterReplicaLogDirsRequest as Request,
};

pub(super) fn assignment(
    topic: String,
    partition: i32,
    broker_id: i32,
    target_path: String,
) -> Assignment {
    Assignment::new(topic, partition, broker_id, target_path)
}

pub(super) fn request(assignments: Vec<Assignment>) -> Request {
    Request::new(assignments)
}
