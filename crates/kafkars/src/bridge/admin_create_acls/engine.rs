//! Isolated names for the engine-owned `CreateAcls` adapter contract.

pub(super) use kafka_client_engine::{
    CreateAclBinding as Binding, CreateAclBrokerError as BrokerError,
    CreateAclOutcome as AclOutcome, CreateAclResult as AclResult, CreateAclsAccepted as Accepted,
    CreateAclsAcceptedFaultKind as AcceptedFaultKind, CreateAclsAdmissionError as AdmissionError,
    CreateAclsAdmissionErrorKind as AdmissionErrorKind, CreateAclsBatch as Batch,
    CreateAclsDeliveryStatus as DeliveryStatus, CreateAclsFailure as Failure,
    CreateAclsFailureKind as FailureKind, CreateAclsObserver as Observer,
    CreateAclsObserverError as ObserverError, CreateAclsOutcome as Outcome,
    CreateAclsRequest as Request,
};
