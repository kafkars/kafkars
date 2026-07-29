//! Flexible-v0 API-key 74 request construction and bounded response normalization.

mod materialize;
mod model;
mod request;
mod response;
mod retention;
mod validation;

pub(crate) use model::ListClientMetricsResourcesResponseFacts;
pub(crate) use request::list_client_metrics_resources_request;
pub(crate) use response::{
    ListClientMetricsResourcesProtocolFailure, normalize_list_client_metrics_resources_response,
};

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_failure_test;
#[cfg(test)]
mod response_success_test;
