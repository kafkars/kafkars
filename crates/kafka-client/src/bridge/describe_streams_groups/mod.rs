//! Declarative private bridge for caller-ordered `StreamsGroup` descriptions.

mod engine;
mod operation;
mod request;
mod result;

pub(crate) use operation::AdminDescribeStreamsGroups;
pub(crate) use request::DescribeStreamsGroupsAdminRequest;
