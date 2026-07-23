//! Bounded terminal retention and runtime-neutral observer notification.

mod cell;
mod error;
mod host_state;
mod identity;
mod notifier;
mod observer;
mod registry;
mod state;

pub(crate) use error::{CompletionObserverError, CompletionRegistryError};
pub(crate) use identity::CompletionId;
pub(crate) use notifier::NotifierJoin;
pub(crate) use observer::CompletionObserver;
#[cfg_attr(
    not(test),
    expect(
        unused_imports,
        reason = "production completion ownership precedes the integrated producer host"
    )
)]
pub(crate) use registry::{CompletionRegistry, ReclaimStatus};

#[cfg(test)]
mod observer_test;
#[cfg(test)]
mod registry_generation_test;
#[cfg(test)]
mod registry_test;
#[cfg(test)]
mod test_support;
