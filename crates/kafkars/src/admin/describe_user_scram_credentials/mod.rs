//! Declarative facade for SCRAM credential metadata and observation.

mod builder;
mod credential;
mod operation;
mod result;

pub use builder::DescribeUserScramCredentialsBuilder;
pub use credential::{ScramCredentialInfo, ScramMechanism};
pub use operation::DescribeUserScramCredentials;
pub use result::DescribeUserScramCredentialsResult;

#[cfg(test)]
mod credential_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod result_test;
