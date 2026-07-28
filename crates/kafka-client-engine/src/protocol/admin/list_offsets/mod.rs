//! Generated API-key 2 adaptation for one leader-routed Admin `ListOffsets` target.

mod model;
mod request;
mod response;
mod version;

pub(crate) use super::request_timeout_error::remaining_timeout_ms;
pub(crate) use model::NormalizedAdminListOffsetsResponse;
pub(crate) use request::admin_list_offsets_request;
pub(crate) use response::{AdminListOffsetsResponseFailure, normalize_admin_list_offsets_response};
pub(crate) use version::minimum_api_version;

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
#[cfg(test)]
mod version_test;
