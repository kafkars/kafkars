//! Public delegation-token description builder, observer, and result.

mod builder;
mod operation;
mod result;

pub use builder::DescribeDelegationTokensBuilder;
pub use operation::DescribeDelegationTokens;
pub use result::DescribeDelegationTokensResult;

pub(super) use super::{DelegationToken, DelegationTokenPrincipal};

#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod result_test;
