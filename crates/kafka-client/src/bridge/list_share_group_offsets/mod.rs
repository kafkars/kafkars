//! Declarative private bridge for `ShareGroup` offset listing.

mod engine;
mod groups_operation;
mod groups_result;
mod operation;
mod request;
mod result;

pub(crate) use groups_operation::AdminListShareGroupsOffsets;
pub(crate) use operation::AdminListShareGroupOffsets;
pub(crate) use request::{ListShareGroupOffsetsAdminRequest, ListShareGroupsOffsetsAdminRequest};

#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod request_test;
#[cfg(test)]
mod result_test;
