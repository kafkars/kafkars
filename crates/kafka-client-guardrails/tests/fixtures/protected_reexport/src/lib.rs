//! Declarative facade with renamed public and crate-private re-exports.

mod owner;
mod positive;
mod private_consumer;
mod public_consumer;

pub use owner::PendingNotificationPermitPool as PublicPool;
pub(crate) use owner::PendingNotificationPermitPool as PrivatePool;
