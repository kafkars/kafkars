//! Deterministic policy for one Admin `CreateDelegationToken` request.
//!
//! The engine constructs this machine only after atomically reserving retained
//! request/result bytes and one completion cell. The host supplies the single
//! absolute deadline captured at the public `submit` boundary and remains its
//! scheduling owner. Core emits exactly one AnyBroker submission, owns the
//! sole terminal decision, and has no retry or cancellation transition.
//! Dropping a later observer therefore abandons observation without changing
//! this accepted machine.

mod hmac;
mod machine;
mod model;
mod outcome;
mod response;
mod token;
mod transition;

pub use hmac::{CREATE_DELEGATION_TOKEN_MAX_HMAC_BYTES, DelegationTokenHmac};
pub use machine::{
    CreateDelegationTokenEffect, CreateDelegationTokenInput, CreateDelegationTokenMachine,
    CreateDelegationTokenMachineError, CreateDelegationTokenState, CreateDelegationTokenTransition,
};
pub use model::{
    CREATE_DELEGATION_TOKEN_MAX_PRINCIPAL_BYTES, CREATE_DELEGATION_TOKEN_MAX_RENEWERS,
    CREATE_DELEGATION_TOKEN_MAX_REQUEST_TEXT_BYTES, CreateDelegationTokenPlan,
    CreateDelegationTokenPlanError, DelegationTokenPrincipal,
};
pub use outcome::{
    CreateDelegationTokenBrokerError, CreateDelegationTokenFailure,
    CreateDelegationTokenFailureKind, CreateDelegationTokenTerminal,
};
pub use response::{
    CREATE_DELEGATION_TOKEN_MAX_TOKEN_ID_BYTES, CreateDelegationTokenResponse,
    CreateDelegationTokenResponseError,
};
pub use token::{CreateDelegationTokenSuccess, DelegationToken};

#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod transition_test;
