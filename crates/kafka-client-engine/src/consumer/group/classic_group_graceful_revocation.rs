//! Private host owner for one assignment-fenced graceful-revocation lease.

mod begin;
mod model;
mod owner;
mod settlement;

pub(in crate::consumer) use model::ClassicGroupRevocationAcknowledgeError;
pub(super) use model::{
    ClassicGroupRevocationHostError, ClassicGroupRevocationStageError, ClassicGroupRevocationTurn,
};
pub(super) use owner::ClassicGroupRevocationOwner;

#[cfg(test)]
mod model_test;
#[cfg(test)]
mod owner_test;
#[cfg(test)]
mod settlement_test;
