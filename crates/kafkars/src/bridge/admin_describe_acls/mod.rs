//! Declarative private bridge for ACL descriptions.

mod engine;
mod operation;
mod request;
mod result;

pub(crate) use operation::AdminDescribeAcls;
pub(crate) use request::DescribeAclsAdminRequest;

#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod request_test;
#[cfg(test)]
mod result_test;
