//! Isolated names for the engine-owned delegation-token description contract.

pub(super) use kafka_client_engine::{
    DescribeDelegationTokenPrincipal as Principal, DescribeDelegationTokensAccepted as Accepted,
    DescribeDelegationTokensAcceptedFaultKind as AcceptedFaultKind,
    DescribeDelegationTokensAdmissionError as AdmissionError,
    DescribeDelegationTokensAdmissionErrorKind as AdmissionErrorKind,
    DescribeDelegationTokensBrokerError as BrokerError,
    DescribeDelegationTokensDeliveryStatus as DeliveryStatus,
    DescribeDelegationTokensFailure as Failure, DescribeDelegationTokensFailureKind as FailureKind,
    DescribeDelegationTokensObserver as Observer,
    DescribeDelegationTokensObserverError as ObserverError,
    DescribeDelegationTokensOutcome as Outcome, DescribeDelegationTokensRequest as Request,
    DescribeDelegationTokensResult as Result, DescribedDelegationToken as Token,
};
