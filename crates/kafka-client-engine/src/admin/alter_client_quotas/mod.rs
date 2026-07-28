//! Declarative facade for the concrete Admin `AlterClientQuotas` engine owner.

mod error;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod shard;

pub use error::{AlterClientQuotasAdmissionError, AlterClientQuotasAdmissionErrorKind};
pub use handle::{AlterClientQuotasAccepted, AlterClientQuotasAcceptedFaultKind};
pub use model::{
    AlterClientQuotaEntity, AlterClientQuotaEntityComponent, AlterClientQuotaEntry,
    AlterClientQuotaOperation, AlterClientQuotasRequest,
};
pub use observer::AlterClientQuotasObserver;
pub use outcome::{
    AlterClientQuotaBrokerError, AlterClientQuotaOutcome, AlterClientQuotasBatch,
    AlterClientQuotasDeliveryStatus, AlterClientQuotasFailure, AlterClientQuotasFailureKind,
    AlterClientQuotasObserverError, AlterClientQuotasOutcome,
};

pub(crate) use error::AlterClientQuotasHostError;
pub(crate) use host::{ALTER_CLIENT_QUOTAS_CAPACITY, AlterClientQuotasHost, AlterClientQuotasTurn};
pub(crate) use shard::{
    AlterClientQuotasAdmissionPort, AlterClientQuotasShardLockError, AlterClientQuotasShardOwner,
    AlterClientQuotasShardWake, AlterClientQuotasShardWakeError,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
