//! Declarative private bridge for Admin `DescribeProducers`.

mod engine;
mod operation;
mod request;
mod result;

pub(crate) use operation::AdminDescribeProducers;
pub(crate) use request::DescribeProducersAdminRequest;

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod result_test;
