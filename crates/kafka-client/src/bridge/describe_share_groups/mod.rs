//! Declarative private bridge for caller-ordered ShareGroup descriptions.

mod engine;
mod operation;
mod request;
mod result;

pub(crate) use operation::AdminDescribeShareGroups;
pub(crate) use request::DescribeShareGroupsAdminRequest;
