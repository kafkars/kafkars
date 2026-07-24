//! Shared concrete fixtures for assigned-owner scenario modules.

use std::{sync::Arc, time::Duration};

use kafka_client_core::{PartitionIndex, StartPosition};

use crate::{
    EngineConfig,
    clock::MonotonicClock,
    driver::DriverOwner,
    protocol::{
        consumer::ListOffsetsIsolation,
        fetch::{FetchDecodeLimits, FetchRequestSettings},
    },
};

use super::{
    assigned_host::AssignedConsumerCompletionNotifier,
    assigned_owner::AssignedConsumerOwner,
    assigned_owner_model::{AssignedConsumerOwnerLimits, AssignedConsumerOwnerSettings},
    assigned_topics::{AssignedPartitionInput, AssignedTopicLimits},
};

pub(super) const OUTPUT_BYTES: usize = 64 * 1024;

#[test]
fn construction_preallocates_every_bounded_owner_queue() {
    let owner = owner(3);
    assert!(owner.effects.capacity() >= 7);
    assert!(owner.raw_position_deadlines.capacity() >= 3);
    assert!(owner.pending_positions.capacity() >= 3);
    assert!(owner.pending_fetches.capacity() >= 3);
    assert!(owner.reclaim_faults.capacity() >= owner.limits.delivery_capacity);
    assert_eq!(owner.limits.call_capacity, 3);
}

pub(super) fn owner(partitions: usize) -> AssignedConsumerOwner {
    let (notifier, publishers) = AssignedConsumerCompletionNotifier::start()
        .unwrap_or_else(|error| panic!("start assigned completion notifier: {error}"));
    let mut owner = AssignedConsumerOwner::new(
        Arc::new(MonotonicClock::new()),
        settings(),
        limits(partitions),
        publishers.close,
    )
    .unwrap_or_else(|error| panic!("construct assigned owner: {error:?}"));
    owner.install_close_notifier_for_test(notifier);
    owner
}

impl AssignedConsumerOwner {
    pub(crate) fn install_close_notifier_for_test(
        &mut self,
        notifier: AssignedConsumerCompletionNotifier,
    ) {
        self.close_notifier = Some(AssignedConsumerNotifierGuard::new(notifier));
    }
}

pub(super) struct AssignedConsumerNotifierGuard {
    notifier: Option<AssignedConsumerCompletionNotifier>,
}

impl AssignedConsumerNotifierGuard {
    const fn new(notifier: AssignedConsumerCompletionNotifier) -> Self {
        Self {
            notifier: Some(notifier),
        }
    }
}

impl Drop for AssignedConsumerNotifierGuard {
    fn drop(&mut self) {
        if let Some(mut notifier) = self.notifier.take()
            && let Some(join) = notifier.take_join()
        {
            let _join_result = join.join_off_notifier();
        }
    }
}

pub(super) fn settings() -> AssignedConsumerOwnerSettings {
    AssignedConsumerOwnerSettings::new(
        ListOffsetsIsolation::ReadUncommitted,
        FetchRequestSettings::new(500, 1, 1_048_576, 1_048_576, 0),
        FetchDecodeLimits::default(),
        Duration::from_secs(30),
        8,
    )
}

pub(super) fn limits(partitions: usize) -> AssignedConsumerOwnerLimits {
    AssignedConsumerOwnerLimits::new(
        partitions,
        partitions,
        partitions,
        OUTPUT_BYTES.saturating_mul(partitions),
        OUTPUT_BYTES,
        AssignedTopicLimits::new(partitions, partitions, 249, 4_096),
    )
    .unwrap_or_else(|error| panic!("valid assigned limits: {error:?}"))
}

pub(super) fn input(topic: &str, partition: u32, start: StartPosition) -> AssignedPartitionInput {
    AssignedPartitionInput::new(Arc::from(topic), PartitionIndex::from_raw(partition), start)
}

pub(super) fn driver() -> DriverOwner {
    DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("build embedded driver: {error}"))
}

pub(super) fn shutdown(driver: &mut DriverOwner) {
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
}
