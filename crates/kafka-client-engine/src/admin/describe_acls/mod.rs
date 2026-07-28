//! Declarative facade for the concrete Admin `DescribeAcls` engine owner.

mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod shard;

pub use error::{DescribeAclsAdmissionError, DescribeAclsAdmissionErrorKind};
pub use handle::{DescribeAclsAccepted, DescribeAclsAcceptedFaultKind};
pub use model::{DescribeAclsFilter, DescribeAclsRequest};
pub use observer::DescribeAclsObserver;
pub use outcome::{
    DescribeAclBinding, DescribeAclsBatch, DescribeAclsBrokerError, DescribeAclsDeliveryStatus,
    DescribeAclsFailure, DescribeAclsFailureKind, DescribeAclsObserverError, DescribeAclsOutcome,
};

pub(crate) use error::DescribeAclsHostError;
pub(crate) use host::{DESCRIBE_ACLS_CAPACITY, DescribeAclsHost, DescribeAclsTurn};
pub(crate) use shard::{
    DescribeAclsAdmissionPort, DescribeAclsShardLockError, DescribeAclsShardOwner,
    DescribeAclsShardWake, DescribeAclsShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
