//! Isolated names for the engine-owned DeleteAcls adapter contract.

pub(super) use kafka_client_engine::{
    DeleteAclBrokerError as BrokerError, DeleteAclFilterOutcome as FilterOutcome,
    DeleteAclFilterResult as FilterResult, DeleteAclMatchResult as MatchResult,
    DeleteAclMatchingBinding as MatchingBinding, DeleteAclsAccepted as Accepted,
    DeleteAclsAcceptedFaultKind as AcceptedFaultKind, DeleteAclsAdmissionError as AdmissionError,
    DeleteAclsAdmissionErrorKind as AdmissionErrorKind, DeleteAclsBatch as Batch,
    DeleteAclsDeliveryStatus as DeliveryStatus, DeleteAclsFailure as Failure,
    DeleteAclsFailureKind as FailureKind, DeleteAclsFilter as Filter,
    DeleteAclsObserver as Observer, DeleteAclsObserverError as ObserverError,
    DeleteAclsOutcome as Outcome, DeleteAclsRequest as Request,
};
