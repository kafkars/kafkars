//! Declarative facade for producer admission, delivery, cancellation, flush, and close.

mod cancellation;
mod close;
mod compression;
mod delivery;
mod flush;
mod handle;
mod limits;
mod metadata;
mod rejection;
mod send;
mod send_batch;

pub use cancellation::CancellationOutcome;
pub use close::CloseProducer;
pub use compression::Compression;
pub use delivery::Delivery;
pub use flush::Flush;
pub use handle::{Producer, ProducerBuilder};
pub use limits::ProducerLimits;
pub use metadata::RecordMetadata;
pub use rejection::TrySendError;
pub use send::Send;
pub use send_batch::{SendBatch, SendBatchResult};

#[cfg(test)]
mod cancellation_test;
#[cfg(test)]
mod close_test;
#[cfg(test)]
mod compression_test;
#[cfg(test)]
mod delivery_test;
#[cfg(test)]
mod flush_test;
#[cfg(test)]
mod handle_test;
#[cfg(test)]
mod limits_test;
#[cfg(test)]
mod metadata_test;
#[cfg(test)]
mod rejection_test;
#[cfg(test)]
mod send_batch_test;
#[cfg(test)]
mod send_test;
