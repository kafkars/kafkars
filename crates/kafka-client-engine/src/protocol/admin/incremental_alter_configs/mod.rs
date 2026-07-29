//! Generated-message adaptation for resource-generic `IncrementalAlterConfigs`.
mod request;
mod resource;
mod response;
mod retention;
pub(crate) use request::incremental_alter_configs_request;
pub(crate) use response::{
    IncrementalAlterConfigsProtocolFailure, normalize_incremental_alter_configs_response_bounded,
};
#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
#[cfg(test)]
mod retention_test;
