//! Linear prepared owner and its exact reservation discriminator.

use kafka_client_core::AssignedTopicPartition;

use super::super::AssignedConsumerEventStore;

#[derive(Clone, Copy)]
pub(super) enum PreparedKind<'input> {
    Replacement(usize),
    Reconciliation(usize),
    Addition(usize),
    Removal(usize),
    Partition(AssignedTopicPartition),
    Pause(&'input [AssignedTopicPartition]),
    Resume(&'input [AssignedTopicPartition]),
}

/// Exclusive proof that terminal capacity was reserved before core mutation.
#[must_use = "prepared event claims must be committed or rolled back"]
pub(crate) struct PreparedEventClaims<'store, 'input> {
    pub(super) store: &'store mut AssignedConsumerEventStore,
    pub(super) kind: PreparedKind<'input>,
}
