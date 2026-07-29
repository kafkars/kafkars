//! Isolated names for the engine-owned RemoveRaftVoter contract.

pub(super) use kafka_client_engine::{
    RemoveRaftVoterAccepted as Accepted, RemoveRaftVoterAcceptedFaultKind as AcceptedFaultKind,
    RemoveRaftVoterAdmissionError as AdmissionError,
    RemoveRaftVoterAdmissionErrorKind as AdmissionErrorKind,
    RemoveRaftVoterBrokerError as BrokerError, RemoveRaftVoterDeliveryStatus as DeliveryStatus,
    RemoveRaftVoterFailure as Failure, RemoveRaftVoterFailureKind as FailureKind,
    RemoveRaftVoterObserver as Observer, RemoveRaftVoterObserverError as ObserverError,
    RemoveRaftVoterOutcome as Outcome, RemoveRaftVoterRequest as Request,
    RemoveRaftVoterResult as Result,
};
