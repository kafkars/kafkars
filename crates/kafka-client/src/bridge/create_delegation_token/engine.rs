//! Isolated names for the engine-owned delegation-token creation contract.

pub(super) use kafka_client_engine::{
    CreateDelegationTokenAccepted as Accepted,
    CreateDelegationTokenAcceptedFaultKind as AcceptedFaultKind,
    CreateDelegationTokenAdmissionError as AdmissionError,
    CreateDelegationTokenAdmissionErrorKind as AdmissionErrorKind,
    CreateDelegationTokenBrokerError as BrokerError,
    CreateDelegationTokenDeliveryStatus as DeliveryStatus, CreateDelegationTokenFailure as Failure,
    CreateDelegationTokenFailureKind as FailureKind, CreateDelegationTokenObserver as Observer,
    CreateDelegationTokenObserverError as ObserverError, CreateDelegationTokenOutcome as Outcome,
    CreateDelegationTokenPrincipal as Principal, CreateDelegationTokenRequest as Request,
    CreateDelegationTokenResult as Result, CreatedDelegationToken as Token,
};
