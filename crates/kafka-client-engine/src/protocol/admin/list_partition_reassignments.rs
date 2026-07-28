//! Generated API-key 46 v0 adaptation for controller reassignment queries.

mod request;
mod response;
mod retention;

pub(crate) use super::request_timeout_error::{
    AdminRequestDeadlineError as ListPartitionReassignmentsRequestError, remaining_timeout_ms,
};
pub(crate) use request::list_partition_reassignments_request;
pub(crate) use response::{
    ListPartitionReassignmentsProtocolFailure, normalize_list_partition_reassignments_response,
};

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
#[cfg(test)]
mod retention_test;
