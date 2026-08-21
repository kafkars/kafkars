//! Isolated names for the engine-owned `ShareGroup` offset-deletion contract.

pub(super) use kafka_client_engine::{
    DeleteShareGroupOffsetsAccepted as Accepted,
    DeleteShareGroupOffsetsAcceptedFaultKind as AcceptedFaultKind,
    DeleteShareGroupOffsetsAdmissionError as AdmissionError,
    DeleteShareGroupOffsetsAdmissionErrorKind as AdmissionErrorKind,
    DeleteShareGroupOffsetsBrokerError as BrokerError,
    DeleteShareGroupOffsetsDeliveryStatus as DeliveryStatus,
    DeleteShareGroupOffsetsFailure as Failure, DeleteShareGroupOffsetsFailureKind as FailureKind,
    DeleteShareGroupOffsetsObserver as Observer,
    DeleteShareGroupOffsetsObserverError as ObserverError,
    DeleteShareGroupOffsetsOutcome as Outcome, DeleteShareGroupOffsetsRequest as Request,
    DeleteShareGroupOffsetsTopicError as TopicError,
    DeleteShareGroupOffsetsTopicResult as TopicResult,
};
