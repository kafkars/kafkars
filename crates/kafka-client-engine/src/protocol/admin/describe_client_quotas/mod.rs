//! Generated API-key 48 adaptation for one read-only client-quota filter.

mod model;
mod request;
mod request_charge;
mod response;
mod retention;
mod validation;
mod version;

pub(crate) use model::{
    DescribeClientQuotaFilterComponentRef, DescribeClientQuotaMatchRef,
    DescribeClientQuotasFilterRef, NormalizedClientQuotaEntityComponent,
    NormalizedClientQuotaEntry, NormalizedClientQuotaValue, NormalizedDescribeClientQuotasResponse,
};
#[cfg(test)]
pub(crate) use request::DescribeClientQuotasRequestFailure;
pub(crate) use request::describe_client_quotas_request;
pub(crate) use request_charge::plan_request_peak_charge;
pub(crate) use response::{
    DescribeClientQuotasResponseFailure, normalize_describe_client_quotas_response,
};

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_shape_test;
#[cfg(test)]
mod response_test;
#[cfg(test)]
mod retention_test;
#[cfg(test)]
mod version_test;
