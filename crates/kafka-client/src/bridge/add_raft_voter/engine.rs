//! Isolated names for the engine-owned AddRaftVoter contract.

pub(super) use kafka_client_engine::{
    AddRaftVoterAccepted as Accepted, AddRaftVoterAcceptedFaultKind as AcceptedFaultKind,
    AddRaftVoterAdmissionError as AdmissionError,
    AddRaftVoterAdmissionErrorKind as AdmissionErrorKind, AddRaftVoterBrokerError as BrokerError,
    AddRaftVoterDeliveryStatus as DeliveryStatus, AddRaftVoterEndpoint as Endpoint,
    AddRaftVoterFailure as Failure, AddRaftVoterFailureKind as FailureKind,
    AddRaftVoterObserver as Observer, AddRaftVoterObserverError as ObserverError,
    AddRaftVoterOutcome as Outcome, AddRaftVoterRequest as Request, AddRaftVoterResult as Result,
};
