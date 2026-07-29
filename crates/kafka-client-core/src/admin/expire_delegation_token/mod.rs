//! Deterministic policy for one Admin `ExpireDelegationToken` request.
//!
//! The engine constructs this machine only after atomically reserving retained
//! request/result bytes and one completion cell. The host supplies the single
//! absolute deadline captured at the public `submit` boundary. Core transfers
//! the unique HMAC through exactly one AnyBroker submission, owns the sole
//! terminal decision, and has no retry or cancellation transition.

mod hmac;
mod machine;
mod model;
mod outcome;
mod response;
mod result;
mod transition;

pub use hmac::{EXPIRE_DELEGATION_TOKEN_MAX_HMAC_BYTES, ExpireDelegationTokenHmac};
pub use machine::{
    ExpireDelegationTokenEffect, ExpireDelegationTokenInput, ExpireDelegationTokenMachine,
    ExpireDelegationTokenMachineError, ExpireDelegationTokenState, ExpireDelegationTokenTransition,
};
pub use model::{ExpireDelegationTokenPlan, ExpireDelegationTokenPlanError};
pub use outcome::{
    ExpireDelegationTokenBrokerError, ExpireDelegationTokenFailure,
    ExpireDelegationTokenFailureKind, ExpireDelegationTokenTerminal,
};
pub use response::{ExpireDelegationTokenResponse, ExpireDelegationTokenResponseError};
pub use result::ExpireDelegationTokenSuccess;

#[cfg(test)]
mod hmac_test;
#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod response_test;
#[cfg(test)]
mod transition_test;
