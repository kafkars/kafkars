//! Declarative facade for classic-group consumer bridges.

#[path = "group_consumer.rs"]
pub(crate) mod group_consumer;
#[path = "group_consumer_batch.rs"]
pub(crate) mod group_consumer_batch;
#[path = "group_consumer_checkpoint.rs"]
pub(crate) mod group_consumer_checkpoint;
#[path = "group_consumer_event.rs"]
pub(crate) mod group_consumer_event;
#[path = "group_consumer_metadata.rs"]
pub(crate) mod group_consumer_metadata;
#[path = "group_consumer_recv.rs"]
pub(crate) mod group_consumer_recv;
pub(crate) mod group_consumer_recv_result;
#[path = "group_consumer_registration.rs"]
pub(crate) mod group_consumer_registration;
pub(crate) mod group_consumer_registration_result;

#[cfg(test)]
mod group_consumer_recv_result_test;
#[cfg(test)]
mod group_consumer_registration_result_test;
