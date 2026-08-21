//! Isolated names for the engine-owned legacy full-snapshot replacement contract.

pub(super) use kafka_client_engine::{
    LegacyAlterConfigError as TopicError, LegacyAlterConfigResult as TopicResult,
    LegacyAlterConfigsAccepted as Accepted,
    LegacyAlterConfigsAcceptedFaultKind as AcceptedFaultKind,
    LegacyAlterConfigsAdmissionError as AdmissionError,
    LegacyAlterConfigsAdmissionErrorKind as AdmissionErrorKind,
    LegacyAlterConfigsDeliveryStatus as DeliveryStatus, LegacyAlterConfigsFailure as Failure,
    LegacyAlterConfigsFailureKind as FailureKind, LegacyAlterConfigsObserver as Observer,
    LegacyAlterConfigsObserverError as ObserverError, LegacyAlterConfigsOutcome as Outcome,
    LegacyAlterConfigsRequest as Request, LegacyAlterConfigsResult as Result,
    LegacyConfigEntry as Entry, LegacyConfigResourceReplacement as ResourceReplacement,
    LegacyTopicConfigReplacement as TopicReplacement,
};
