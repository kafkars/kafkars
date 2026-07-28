//! Generated API-key 45 adaptation for caller-ordered reassignment changes.

mod model;
mod request;
mod response;
mod retention;
mod version;

pub(crate) use super::request_timeout_error::{
    AdminRequestDeadlineError as AlterPartitionReassignmentsDeadlineError, remaining_timeout_ms,
};
pub(crate) use model::{
    AlterPartitionReassignmentRef, ValidatedAlterPartitionReassignmentsResponse,
};
pub(crate) use request::{
    AlterPartitionReassignmentsRequestFailure, alter_partition_reassignments_request,
};
pub(crate) use response::{
    AlterPartitionReassignmentsProtocolFailure, validate_alter_partition_reassignments_response,
};
pub(crate) use retention::generated_request_peak_charge;
pub(crate) use version::{ALTER_PARTITION_REASSIGNMENTS_MAX_VERSION, minimum_version_for_policy};

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
#[cfg(test)]
mod version_test;
