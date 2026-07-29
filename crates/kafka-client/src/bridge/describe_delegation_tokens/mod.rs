//! Declarative private bridge for delegation-token description.

mod engine;
mod operation;
mod request;
mod result;

pub(crate) use operation::AdminDescribeDelegationTokens;
pub(crate) use request::DescribeDelegationTokensAdminRequest;

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod result_test;
