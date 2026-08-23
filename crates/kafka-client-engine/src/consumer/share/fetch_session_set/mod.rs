//! Declarative facade for hosted broker-local share-fetch session sets.

mod acknowledgement;
mod config;
pub(super) mod delivery;
mod execution;
mod owner;
mod recovery;

pub(in crate::consumer::share) use config::ShareFetchSessionConfig;
pub(super) use owner::{
    ShareFetchSessionIdentity, ShareFetchSessionSet, ShareFetchSessionSetOpenError,
    ShareFetchSessionSetTurn,
};

#[cfg(test)]
mod delivery_test;
#[cfg(test)]
mod execution_test;
#[cfg(test)]
pub(super) mod owner_test;
