//! Flexible-v1 API-key 74 request construction and bounded response normalization.

mod materialize;
mod model;
mod request;
mod response;
mod retention;
mod validation;

pub(crate) use model::{ListConfigResource, ListConfigResourcesResponseFacts};
#[cfg(test)]
pub(crate) use request::ListConfigResourcesRequestFailure;
pub(crate) use request::list_config_resources_request;
pub(crate) use response::{
    ListConfigResourcesProtocolFailure, normalize_list_config_resources_response,
};

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
