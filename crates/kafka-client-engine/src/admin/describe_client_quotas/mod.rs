//! Declarative facade for the concrete Admin `DescribeClientQuotas` engine owner.

mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod shard;

pub use error::{DescribeClientQuotasAdmissionError, DescribeClientQuotasAdmissionErrorKind};
pub use handle::{DescribeClientQuotasAccepted, DescribeClientQuotasAcceptedFaultKind};
pub use model::{
    DescribeClientQuotaFilterComponent, DescribeClientQuotaMatch, DescribeClientQuotasRequest,
};
pub use observer::DescribeClientQuotasObserver;
pub use outcome::{
    DescribeClientQuotaEntity, DescribeClientQuotaEntityComponent, DescribeClientQuotaValue,
    DescribeClientQuotasBatch, DescribeClientQuotasBrokerError, DescribeClientQuotasDeliveryStatus,
    DescribeClientQuotasFailure, DescribeClientQuotasFailureKind,
    DescribeClientQuotasObserverError, DescribeClientQuotasOutcome,
};

pub(crate) use error::DescribeClientQuotasHostError;
pub(crate) use host::{
    DESCRIBE_CLIENT_QUOTAS_CAPACITY, DescribeClientQuotasHost, DescribeClientQuotasTurn,
};
pub(crate) use shard::{
    DescribeClientQuotasAdmissionPort, DescribeClientQuotasShardLockError,
    DescribeClientQuotasShardOwner, DescribeClientQuotasShardWake,
    DescribeClientQuotasShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
