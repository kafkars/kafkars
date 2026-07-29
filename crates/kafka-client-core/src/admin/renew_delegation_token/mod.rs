//! Deterministic policy for one Admin `RenewDelegationToken` request.
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

pub use hmac::{RENEW_DELEGATION_TOKEN_MAX_HMAC_BYTES, RenewDelegationTokenHmac};
pub use machine::{
    RenewDelegationTokenEffect, RenewDelegationTokenInput, RenewDelegationTokenMachine,
    RenewDelegationTokenMachineError, RenewDelegationTokenState, RenewDelegationTokenTransition,
};
pub use model::{RenewDelegationTokenPlan, RenewDelegationTokenPlanError};
pub use outcome::{
    RenewDelegationTokenBrokerError, RenewDelegationTokenFailure, RenewDelegationTokenFailureKind,
    RenewDelegationTokenTerminal,
};
pub use response::{RenewDelegationTokenResponse, RenewDelegationTokenResponseError};
pub use result::RenewDelegationTokenSuccess;

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
