//! Isolated names for the engine-owned delegation-token renewal contract.

pub(super) use kafka_client_engine::{
    RenewDelegationTokenAccepted as Accepted,
    RenewDelegationTokenAcceptedFaultKind as AcceptedFaultKind,
    RenewDelegationTokenAdmissionError as AdmissionError,
    RenewDelegationTokenAdmissionErrorKind as AdmissionErrorKind,
    RenewDelegationTokenBrokerError as BrokerError,
    RenewDelegationTokenDeliveryStatus as DeliveryStatus, RenewDelegationTokenFailure as Failure,
    RenewDelegationTokenFailureKind as FailureKind, RenewDelegationTokenHmac as Hmac,
    RenewDelegationTokenObserver as Observer, RenewDelegationTokenObserverError as ObserverError,
    RenewDelegationTokenOutcome as Outcome, RenewDelegationTokenRequest as Request,
    RenewDelegationTokenResult as Result,
};
