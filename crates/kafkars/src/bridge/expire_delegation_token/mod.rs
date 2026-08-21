//! Declarative private bridge for delegation-token expiration.

mod engine;
mod operation;
mod request;
mod result;

pub(crate) use operation::AdminExpireDelegationToken;
pub(crate) use request::ExpireDelegationTokenAdminRequest;

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod result_test;
