//! Isolated names for the engine-owned ShareGroup offset-alteration contract.

pub(super) use kafka_client_engine::{
    AlterShareGroupOffset as Target, AlterShareGroupOffsetsAccepted as Accepted,
    AlterShareGroupOffsetsAcceptedFaultKind as AcceptedFaultKind,
    AlterShareGroupOffsetsAdmissionError as AdmissionError,
    AlterShareGroupOffsetsAdmissionErrorKind as AdmissionErrorKind,
    AlterShareGroupOffsetsBrokerError as BrokerError,
    AlterShareGroupOffsetsDeliveryStatus as DeliveryStatus,
    AlterShareGroupOffsetsFailure as Failure, AlterShareGroupOffsetsFailureKind as FailureKind,
    AlterShareGroupOffsetsObserver as Observer,
    AlterShareGroupOffsetsObserverError as ObserverError, AlterShareGroupOffsetsOutcome as Outcome,
    AlterShareGroupOffsetsPartitionError as PartitionError,
    AlterShareGroupOffsetsPartitionResult as PartitionResult,
    AlterShareGroupOffsetsRequest as Request,
};
