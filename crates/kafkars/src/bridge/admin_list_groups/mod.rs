//! Declarative private bridge for exactly filtered cluster-wide group listing.

mod operation;
mod request;
mod result;

pub(crate) use operation::AdminListGroups;
pub(crate) use request::ListGroupsAdminRequest;
