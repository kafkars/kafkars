//! Isolated names for the engine-owned DescribeAcls adapter contract.

pub(super) use kafka_client_engine::{
    DescribeAclBinding as Binding, DescribeAclsAccepted as Accepted,
    DescribeAclsAcceptedFaultKind as AcceptedFaultKind,
    DescribeAclsAdmissionError as AdmissionError,
    DescribeAclsAdmissionErrorKind as AdmissionErrorKind, DescribeAclsBatch as Batch,
    DescribeAclsBrokerError as BrokerError, DescribeAclsDeliveryStatus as DeliveryStatus,
    DescribeAclsFailure as Failure, DescribeAclsFailureKind as FailureKind,
    DescribeAclsFilter as Filter, DescribeAclsObserver as Observer,
    DescribeAclsObserverError as ObserverError, DescribeAclsOutcome as Outcome,
    DescribeAclsRequest as Request,
};
