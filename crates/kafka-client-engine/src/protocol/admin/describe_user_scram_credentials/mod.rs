//! Generated API-key 50 adaptation for bounded SCRAM credential descriptions.

mod correlation;
mod model;
mod request;
mod response;
mod retention;
mod validation;
mod version;

pub(crate) use model::{
    DescribeUserScramCredentialsRequestRef, NormalizedDescribeUserScramCredentialsResponse,
    NormalizedScramCredentialInfo, NormalizedUserScramCredentials,
};
#[cfg(test)]
pub(crate) use request::DescribeUserScramCredentialsRequestFailure;
pub(crate) use request::describe_user_scram_credentials_request;
pub(crate) use response::{
    DescribeUserScramCredentialsResponseFailure, normalize_describe_user_scram_credentials_response,
};

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_shape_test;
#[cfg(test)]
mod response_test;
#[cfg(test)]
mod retention_test;
#[cfg(test)]
mod version_test;
