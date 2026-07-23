//! Declarative facade for producer admission, delivery, cancellation, flush, and close.

mod cancellation;
mod close;
mod delivery;
mod flush;
mod handle;
mod metadata;
mod rejection;

pub use cancellation::CancellationOutcome;
pub use close::CloseProducer;
pub use delivery::Delivery;
pub use flush::Flush;
pub use handle::{Producer, ProducerBuilder};
pub use metadata::RecordMetadata;
pub use rejection::TrySendError;

#[cfg(test)]
mod cancellation_test;
#[cfg(test)]
mod close_test;
#[cfg(test)]
mod delivery_test;
#[cfg(test)]
mod flush_test;
#[cfg(test)]
mod handle_test;
#[cfg(test)]
mod metadata_test;
#[cfg(test)]
mod rejection_test;
