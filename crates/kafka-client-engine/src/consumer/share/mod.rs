//! Engine interpretation of deterministic share-group membership effects.
#![allow(
    dead_code,
    reason = "the closed membership interpreter precedes its registry checkpoint"
)]

mod catalog;
#[cfg(test)]
mod catalog_test;
mod entry;
#[cfg(test)]
mod entry_test;
mod membership;
#[cfg(test)]
mod membership_test;
mod prepared;
mod registry;
mod registry_registration;
#[cfg(test)]
mod registry_test;
mod request;
#[cfg(test)]
mod request_test;
mod settlement;
mod transition;

pub(super) use catalog::ShareMembershipCatalog;
#[cfg(test)]
pub(super) use catalog::ShareTopicIdentity;
#[cfg(test)]
pub(super) use membership::ShareMembershipFailureTurn;
pub(super) use membership::{ShareMembershipError, ShareMembershipInterpreter};
#[cfg(test)]
pub(super) use registry::ShareConsumerRegistry;
#[cfg(test)]
pub(super) use registry_registration::{
    ShareConsumerRegistrationFailureKind, ShareConsumerStartError,
};
