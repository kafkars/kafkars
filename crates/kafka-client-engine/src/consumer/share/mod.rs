//! Engine interpretation of deterministic share-group membership effects.
#![allow(
    dead_code,
    reason = "the closed membership interpreter precedes its registry checkpoint"
)]

mod catalog;
#[cfg(test)]
mod catalog_test;
mod membership;
#[cfg(test)]
mod membership_test;
mod prepared;
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
