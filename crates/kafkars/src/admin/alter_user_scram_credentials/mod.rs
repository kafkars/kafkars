//! Declarative facade for SCRAM credential alterations and observation.

mod alteration;
mod builder;
mod operation;
mod result;

pub use alteration::UserScramCredentialAlteration;
pub use builder::AlterUserScramCredentialsBuilder;
pub use operation::AlterUserScramCredentials;
pub use result::AlterUserScramCredentialsResult;

#[cfg(test)]
mod alteration_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod result_test;
