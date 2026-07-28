//! Generated API-key 49 adaptation for caller-ordered client-quota alterations.

mod model;
mod request;
mod request_validation;
mod response;
mod response_retention;
mod response_validation;
mod retention;
mod version;

pub(crate) use model::{
    AlterClientQuotaAlterationRef, AlterClientQuotaEntityComponentRef,
    AlterClientQuotaOperationKindRef, AlterClientQuotaOperationRef, AlterClientQuotasRequestRef,
    NormalizedAlterClientQuotaEntityComponent, NormalizedAlterClientQuotaOutcome,
    NormalizedAlterClientQuotasResponse,
};
pub(crate) use request::{AlterClientQuotasRequestFailure, alter_client_quotas_request};
pub(crate) use response::{
    AlterClientQuotasResponseFailure, normalize_alter_client_quotas_response,
};
#[cfg(test)]
pub(crate) use version::{ALTER_CLIENT_QUOTAS_MAX_VERSION, ALTER_CLIENT_QUOTAS_MIN_VERSION};

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
#[cfg(test)]
mod retention_test;
#[cfg(test)]
mod version_test;
