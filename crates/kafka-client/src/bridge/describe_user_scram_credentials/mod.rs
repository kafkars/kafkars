//! Declarative private bridge for SCRAM credential descriptions.

mod engine;
mod operation;
mod request;
mod result;

pub(crate) use operation::AdminDescribeUserScramCredentials;
pub(crate) use request::DescribeUserScramCredentialsAdminRequest;

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod result_test;
