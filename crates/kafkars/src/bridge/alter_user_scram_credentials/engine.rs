//! Isolated names for the engine-owned SCRAM credential-alteration contract.

pub(super) use kafka_client_engine::{
    AlterUserScramCredential as EngineAlteration,
    AlterUserScramCredentialBrokerError as BrokerError,
    AlterUserScramCredentialOutcome as UserOutcome, AlterUserScramCredentialsAccepted as Accepted,
    AlterUserScramCredentialsAcceptedFaultKind as AcceptedFaultKind,
    AlterUserScramCredentialsAdmissionError as AdmissionError,
    AlterUserScramCredentialsAdmissionErrorKind as AdmissionErrorKind,
    AlterUserScramCredentialsBatch as Batch,
    AlterUserScramCredentialsDeliveryStatus as DeliveryStatus,
    AlterUserScramCredentialsFailure as Failure,
    AlterUserScramCredentialsFailureKind as FailureKind,
    AlterUserScramCredentialsObserver as Observer,
    AlterUserScramCredentialsObserverError as ObserverError,
    AlterUserScramCredentialsOutcome as Outcome, AlterUserScramCredentialsRequest as Request,
};
