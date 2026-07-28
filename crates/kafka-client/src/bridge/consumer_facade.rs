//! Declarative facade for classic-group consumer bridges.

#[path = "group_consumer.rs"]
pub(crate) mod group_consumer;
#[path = "group_consumer_batch.rs"]
pub(crate) mod group_consumer_batch;
pub(crate) mod group_consumer_checkpoint;
pub(crate) mod group_consumer_close;
pub(crate) mod group_consumer_close_admission;
pub(crate) mod group_consumer_commit;
pub(crate) mod group_consumer_commit_admission;
#[path = "group_consumer_event.rs"]
pub(crate) mod group_consumer_event;
pub(crate) mod group_consumer_event_observation;
#[path = "group_consumer_metadata.rs"]
pub(crate) mod group_consumer_metadata;
pub(crate) mod group_consumer_next_event;
pub(crate) mod group_consumer_rebalance_event;
#[path = "group_consumer_recv.rs"]
pub(crate) mod group_consumer_recv;
pub(crate) mod group_consumer_recv_result;
#[path = "group_consumer_registration.rs"]
pub(crate) mod group_consumer_registration;
pub(crate) mod group_consumer_registration_result;

#[cfg(test)]
mod group_consumer_checkpoint_test;
#[cfg(test)]
mod group_consumer_close_admission_test;
#[cfg(test)]
mod group_consumer_close_test;
#[cfg(test)]
mod group_consumer_commit_admission_test;
#[cfg(test)]
mod group_consumer_commit_test;
#[cfg(test)]
mod group_consumer_rebalance_event_test;
#[cfg(test)]
mod group_consumer_recv_result_test;
#[cfg(test)]
mod group_consumer_registration_result_test;
