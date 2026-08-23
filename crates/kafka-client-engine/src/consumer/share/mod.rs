//! Engine interpretation of deterministic share-group membership effects.
#![allow(dead_code, reason = "share membership lands in bounded checkpoints")]

mod catalog;
#[cfg(test)]
mod catalog_test;
mod close_state;
mod engine;
mod entry;
mod entry_calls;
mod entry_identity;
#[cfg(test)]
mod entry_test;
mod fetch_acquisition_decode;
#[cfg(test)]
mod fetch_acquisition_decode_test;
mod fetch_plan;
#[cfg(test)]
mod fetch_plan_test;
mod fetch_session;
mod fetch_session_execution;
#[cfg(test)]
mod fetch_session_execution_test;
mod fetch_session_settlement;
#[cfg(test)]
mod fetch_session_settlement_test;
#[cfg(test)]
mod fetch_session_test;
mod membership;
#[cfg(test)]
mod membership_test;
mod port;
#[cfg(test)]
mod port_test;
mod prepared;
mod public_close;
#[cfg(test)]
mod public_close_test;
mod public_registration;
mod public_registration_error;
#[cfg(test)]
mod public_registration_test;
mod public_startup;
mod public_state;
#[cfg(test)]
mod public_state_test;
mod registration_admission;
mod registry;
mod registry_close;
mod registry_close_notifier;
#[cfg(test)]
mod registry_close_test;
mod registry_heartbeat_due;
mod registry_heartbeat_settlement;
mod registry_heartbeat_submission;
#[cfg(test)]
mod registry_heartbeat_test;
mod registry_invalidation;
mod registry_membership;
mod registry_observation;
#[cfg(test)]
mod registry_observation_test;
mod registry_recovery;
mod registry_registration;
#[cfg(test)]
mod registry_test;
mod registry_topic_identity;
#[cfg(test)]
mod registry_topic_identity_test;
mod request;
#[cfg(test)]
mod request_test;
mod settlement;
mod shard;
mod shard_wake;
mod topic_identity_call;
mod transition;

pub(super) use catalog::ShareMembershipCatalog;
#[cfg(test)]
pub(super) use catalog::ShareTopicIdentity;
#[cfg(test)]
pub(super) use membership::ShareMembershipFailureTurn;
pub(super) use membership::{ShareMembershipError, ShareMembershipInterpreter};
pub(crate) use port::ShareConsumerPort;
pub use public_close::{
    ShareConsumerClose, ShareConsumerCloseAdmissionError, ShareConsumerCloseAdmissionErrorKind,
    ShareConsumerCloseError, ShareConsumerCloseErrorKind,
};
pub use public_registration::{
    ShareConsumerHandle, ShareConsumerRegistration, ShareConsumerStartCapture,
};
pub use public_registration_error::{
    ShareConsumerRegistrationError, ShareConsumerRegistrationErrorKind,
};
pub use public_startup::ShareConsumerStartupFailureKind;
pub use public_state::{
    ShareConsumerAssignmentPartition, ShareConsumerState, ShareConsumerStateError,
    ShareConsumerStateErrorKind,
};
pub(crate) use registry::ShareConsumerRegistry;
pub(crate) use registry_membership::{ShareMembershipHostError, ShareMembershipTurn};
#[cfg(test)]
pub(super) use registry_registration::{
    ShareConsumerRegistrationFailureKind, ShareConsumerStartError,
};
pub(crate) use shard::{ShareConsumerShardLockError, ShareConsumerShardOwner};
pub(crate) use shard_wake::{ShareConsumerShardWake, ShareConsumerShardWakeError};
