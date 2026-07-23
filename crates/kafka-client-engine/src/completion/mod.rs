//! Bounded terminal retention and runtime-neutral observer notification.

mod cell;
mod cell_lifecycle;
mod error;
mod host_state;
mod identity;
mod notifier;
mod observer;
mod registry;
mod registry_lifecycle;
mod state;

pub(crate) use error::{CompletionObserverError, CompletionRegistryError};
pub(crate) use identity::CompletionId;
pub(crate) use notifier::NotifierJoin;
pub(crate) use observer::CompletionObserver;
#[cfg(test)]
pub(crate) use registry::{CompletionRegistry, ReclaimStatus};

#[cfg(test)]
mod observer_test;
#[cfg(test)]
mod registry_test;
#[cfg(test)]
mod test_support;
