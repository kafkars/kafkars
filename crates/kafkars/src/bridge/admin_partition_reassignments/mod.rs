//! Private bridge for concrete partition-reassignment alteration.

mod alter_operation;
#[cfg(test)]
mod alter_operation_test;
mod alter_request;
#[cfg(test)]
mod alter_request_test;
mod alter_result;
#[cfg(test)]
mod alter_result_test;
mod engine;

pub(crate) use alter_operation::AdminAlterPartitionReassignments;
pub(crate) use alter_request::AlterPartitionReassignmentsAdminRequest;
