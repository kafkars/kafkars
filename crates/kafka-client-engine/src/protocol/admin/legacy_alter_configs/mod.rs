//! Generated-message adaptation for explicit legacy resource configuration replacement.
mod request;
mod resource;
mod response;
mod retention;

pub(crate) use request::legacy_alter_configs_request;
pub(crate) use response::{
    LegacyAlterConfigsProtocolFailure, normalize_legacy_alter_configs_response_bounded,
};

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
#[cfg(test)]
mod retention_test;
