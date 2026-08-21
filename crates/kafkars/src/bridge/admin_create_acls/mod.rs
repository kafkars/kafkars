//! Declarative private bridge for caller-ordered ACL creation.

mod engine;
mod operation;
mod request;
mod result;

pub(crate) use operation::AdminCreateAcls;
pub(crate) use request::CreateAclsAdminRequest;

#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod request_test;
#[cfg(test)]
mod result_test;
