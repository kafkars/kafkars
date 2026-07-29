//! Declarative private bridge for one modern StreamsGroup description.

mod engine;
mod operation;
mod request;
pub(in crate::bridge) mod result;

pub(crate) use operation::AdminDescribeStreamsGroup;
pub(crate) use request::DescribeStreamsGroupAdminRequest;

#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod request_test;
#[cfg(test)]
mod result_test;
