//! Declarative private bridge for SCRAM credential alterations.

mod engine;
mod operation;
mod request;
mod result;

pub(crate) use operation::AdminAlterUserScramCredentials;
pub(crate) use request::AlterUserScramCredentialsAdminRequest;

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod result_test;
