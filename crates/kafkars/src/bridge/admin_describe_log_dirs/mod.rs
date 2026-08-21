//! Declarative private bridge for broker log-directory descriptions.

mod engine;
mod operation;
mod request;
mod result;

pub(crate) use operation::AdminDescribeLogDirs;
pub(crate) use request::DescribeLogDirsAdminRequest;

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod result_test;
