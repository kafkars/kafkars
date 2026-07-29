//! Deterministic single-attempt policy for Admin `DescribeDelegationToken`.

mod listing;
mod machine;
mod model;
mod outcome;
mod response;
mod transition;

pub use listing::DescribeDelegationTokensListing;
pub use machine::{
    DescribeDelegationTokensEffect, DescribeDelegationTokensInput, DescribeDelegationTokensMachine,
    DescribeDelegationTokensMachineError, DescribeDelegationTokensState,
    DescribeDelegationTokensTransition,
};
pub use model::{
    DESCRIBE_DELEGATION_TOKENS_MAX_OWNERS, DESCRIBE_DELEGATION_TOKENS_MAX_REQUEST_TEXT_BYTES,
    DescribeDelegationTokensPlan, DescribeDelegationTokensPlanError,
    DescribeDelegationTokensSelection,
};
pub use outcome::{
    DescribeDelegationTokensBrokerError, DescribeDelegationTokensFailure,
    DescribeDelegationTokensFailureKind, DescribeDelegationTokensTerminal,
};
pub use response::{
    DESCRIBE_DELEGATION_TOKENS_MAX_TOKENS, DescribeDelegationTokenResponse,
    DescribeDelegationTokenResponseError, DescribeDelegationTokensResponse,
};

#[cfg(test)]
mod model_test;
#[cfg(test)]
mod response_test;
#[cfg(test)]
mod transition_test;
