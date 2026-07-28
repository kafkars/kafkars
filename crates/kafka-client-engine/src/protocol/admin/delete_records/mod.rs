//! Generated API-key 21 adaptation for one leader-routed Admin `DeleteRecords` target.

mod model;
mod request;
mod response;

pub(crate) use super::request_timeout_error::remaining_timeout_ms;
pub(crate) use model::NormalizedDeleteRecordsResponse;
pub(crate) use request::delete_records_request;
pub(crate) use response::{DeleteRecordsResponseFailure, normalize_delete_records_response};

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
