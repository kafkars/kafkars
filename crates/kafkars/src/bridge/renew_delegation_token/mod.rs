//! Declarative private bridge for delegation-token renewal.

mod engine;
mod operation;
mod request;
mod result;

pub(crate) use operation::AdminRenewDelegationToken;
pub(crate) use request::RenewDelegationTokenAdminRequest;

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod result_test;
