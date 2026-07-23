//! Declarative facade for immediate producer admission and delivery.

mod delivery;
mod handle;
mod metadata;
mod rejection;

pub use delivery::Delivery;
pub use handle::{Producer, ProducerBuilder};
pub use metadata::RecordMetadata;
pub use rejection::TrySendError;

#[cfg(test)]
mod delivery_test;
#[cfg(test)]
mod handle_test;
#[cfg(test)]
mod metadata_test;
#[cfg(test)]
mod rejection_test;
