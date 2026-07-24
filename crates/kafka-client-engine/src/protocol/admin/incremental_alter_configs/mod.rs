//! Generated-message adaptation for topic-only `IncrementalAlterConfigs`.
mod request;
mod resource;
mod response;
mod retention;
#[allow(
    unused_imports,
    reason = "the protocol seam intentionally lands before concrete driver execution"
)]
pub(crate) use request::incremental_alter_configs_request;
#[allow(
    unused_imports,
    reason = "the protocol seam intentionally lands before concrete driver execution"
)]
pub(crate) use response::{
    IncrementalAlterConfigsProtocolFailure, normalize_incremental_alter_configs_response_bounded,
};
#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
#[cfg(test)]
mod retention_test;
