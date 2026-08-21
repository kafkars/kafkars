//! Private bridge for concrete static-member removal.

mod operation;
mod request;
mod result;

pub(crate) use operation::AdminRemoveConsumerGroupMembers;
pub(crate) use request::RemoveConsumerGroupMembersAdminRequest;
