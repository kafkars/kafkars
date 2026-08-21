//! Isolated names for the engine-owned SCRAM credential-description contract.

pub(super) use kafka_client_engine::{
    DescribeUserScramCredentialInfo as CredentialInfo,
    DescribeUserScramCredentialOutcome as UserOutcome,
    DescribeUserScramCredentialsAccepted as Accepted,
    DescribeUserScramCredentialsAcceptedFaultKind as AcceptedFaultKind,
    DescribeUserScramCredentialsAdmissionError as AdmissionError,
    DescribeUserScramCredentialsAdmissionErrorKind as AdmissionErrorKind,
    DescribeUserScramCredentialsBatch as Batch,
    DescribeUserScramCredentialsBrokerError as BrokerError,
    DescribeUserScramCredentialsDeliveryStatus as DeliveryStatus,
    DescribeUserScramCredentialsFailure as Failure,
    DescribeUserScramCredentialsFailureKind as FailureKind,
    DescribeUserScramCredentialsObserver as Observer,
    DescribeUserScramCredentialsObserverError as ObserverError,
    DescribeUserScramCredentialsOutcome as Outcome, DescribeUserScramCredentialsRequest as Request,
    DescribeUserScramCredentialsUserResult as UserResult,
};
