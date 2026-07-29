//! Declarative private bridge for one modern ShareGroup description.

mod engine;
mod operation;
mod request;
pub(in crate::bridge) mod result;

pub(crate) use operation::AdminDescribeShareGroup;
pub(crate) use request::DescribeShareGroupAdminRequest;

#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod request_test;
#[cfg(test)]
mod result_test;
