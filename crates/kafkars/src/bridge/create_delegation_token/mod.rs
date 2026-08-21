//! Declarative private bridge for delegation-token creation.

mod engine;
mod operation;
mod request;
mod result;

pub(crate) use operation::AdminCreateDelegationToken;
pub(crate) use request::CreateDelegationTokenAdminRequest;

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod result_test;
