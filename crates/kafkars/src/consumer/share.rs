//! Declarative facade for share-group membership, delivery, and observation.

mod assignment;
mod batch;
mod build_error;
mod builder;
mod close;
mod close_error;
mod fetch_config;
mod handle;
mod record;
mod recv;

pub use assignment::{ShareConsumerAssignment, ShareConsumerAssignmentPartition};
pub use batch::ShareConsumerBatch;
pub use build_error::ShareConsumerBuildError;
pub use builder::ShareConsumerBuilder;
pub use close::CloseShareConsumer;
pub use close_error::ShareConsumerCloseAdmissionError;
pub use fetch_config::ShareConsumerFetchConfig;
pub use handle::ShareConsumer;
pub use record::{ShareConsumerHeader, ShareConsumerRecord, ShareConsumerRecords};
pub use recv::RecvShareConsumerBatch;

#[cfg(test)]
mod assignment_test;
#[cfg(test)]
mod batch_test;
#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod close_test;
#[cfg(test)]
mod contract_test;
#[cfg(test)]
mod fetch_config_test;
#[cfg(test)]
mod recv_test;
