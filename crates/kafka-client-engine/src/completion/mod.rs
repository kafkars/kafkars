//! Bounded terminal retention and runtime-neutral observer notification.

mod cell;
mod error;
mod host_state;
mod identity;
mod notifier;
mod notifier_queue;
#[cfg(test)]
mod notifier_queue_test;
#[cfg(test)]
mod notifier_test;
mod observer;
mod publish_ticket;
#[cfg(test)]
mod publish_ticket_test;
mod registry;
mod settlement;
mod shared_notifier;
#[cfg(test)]
mod shared_notifier_test;
mod state;

pub(crate) use error::{CompletionObserverError, CompletionRegistryError};
pub(crate) use identity::CompletionId;
pub(crate) use notifier::{NotificationTicket, NotifierJoin, NotifierJoinError};
pub(crate) use observer::CompletionObserver;
pub(crate) use publish_ticket::PublishTicket;
#[cfg(test)]
pub(crate) use registry::CompletionPublisher;
pub(crate) use registry::{CompletionRegistry, ReclaimStatus};
#[cfg_attr(
    not(test),
    expect(
        unused_imports,
        reason = "host-failure settlement owner follows this generic mechanism"
    )
)]
pub(crate) use settlement::{SettlementFailure, SettlementProgress};
pub(crate) use shared_notifier::{SharedNotificationPort, SharedNotifier, SharedPublishPort};

#[cfg(test)]
mod observer_test;
#[cfg(test)]
mod registry_generation_test;
#[cfg(test)]
mod registry_test;
#[cfg(test)]
mod settlement_test;
#[cfg(test)]
pub(crate) mod test_support;
