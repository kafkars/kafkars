//! Declarative private bridge for positional ACL deletion.

mod engine;
mod operation;
mod request;
mod result;
mod value;

pub(crate) use operation::AdminDeleteAcls;
pub(crate) use request::DeleteAclsAdminRequest;

#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod request_test;
#[cfg(test)]
mod result_test;
