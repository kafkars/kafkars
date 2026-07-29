//! Exact API-key 51 adaptation with secret-safe request ownership.

mod allocation;
mod correlation;
mod crypto;
mod model;
mod request;
mod request_validation;
mod response;
mod retention;
mod version;

pub(crate) use model::{
    AlterUserScramCredentialAlterationRef, AlterUserScramCredentialsCorrelationRef,
    AlterUserScramCredentialsRequestRef, NormalizedAlterUserScramCredentialOutcome,
    NormalizedAlterUserScramCredentialsResponse,
};
pub(crate) use request::{
    AlterUserScramCredentialsRequestFailure, PreparedAlterUserScramCredentialsRequest,
    alter_user_scram_credentials_request,
};
pub(crate) use response::{
    AlterUserScramCredentialsResponseFailure, normalize_alter_user_scram_credentials_response,
};
#[cfg(test)]
pub(crate) use version::{
    ALTER_USER_SCRAM_CREDENTIALS_MAX_VERSION, ALTER_USER_SCRAM_CREDENTIALS_MIN_VERSION,
};

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
#[cfg(test)]
mod retention_test;
#[cfg(test)]
mod version_test;
