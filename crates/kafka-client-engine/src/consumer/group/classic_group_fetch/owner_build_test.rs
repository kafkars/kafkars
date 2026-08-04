//! Bounded construction and selected isolation policy for the group Fetch owner.

use kafka_client_core::{GroupPositionMissingOffsetPolicy, ReadIsolation};

use crate::config::{EngineConsumerFetchConfig, EngineConsumerLimits};
use crate::protocol::fetch::{FetchIsolation, fetch_request};

use super::{
    ClassicGroupFetchOwner,
    owner::{
        FIRST_GROUP_FETCH_CALLS, FIRST_GROUP_FETCH_DELIVERIES, FIRST_GROUP_FETCH_DELIVERY_BYTES,
        FIRST_GROUP_FETCH_EFFECTS, FIRST_GROUP_FETCH_OUTPUT_BYTES, FIRST_GROUP_FETCH_PARTITIONS,
    },
};

#[test]
fn first_slice_reserves_every_pre_core_fifo_and_event_owner() {
    let owner =
        ClassicGroupFetchOwner::try_new().unwrap_or_else(|error| panic!("Fetch owner: {error:?}"));

    assert_eq!(FIRST_GROUP_FETCH_PARTITIONS, 64);
    assert_eq!(FIRST_GROUP_FETCH_EFFECTS, 129);
    assert_eq!(FIRST_GROUP_FETCH_CALLS, 8);
    assert_eq!(FIRST_GROUP_FETCH_DELIVERIES, 8);
    assert_eq!(FIRST_GROUP_FETCH_DELIVERY_BYTES, 8 * 1024 * 1024);
    assert_eq!(FIRST_GROUP_FETCH_OUTPUT_BYTES, 1024 * 1024);
    assert!(owner.effects.capacity() >= FIRST_GROUP_FETCH_EFFECTS);
    assert!(owner.raw_position_deadlines.capacity() >= FIRST_GROUP_FETCH_PARTITIONS);
    assert!(owner.pending_positions.capacity() >= FIRST_GROUP_FETCH_PARTITIONS);
    assert!(owner.pending_fetches.capacity() >= FIRST_GROUP_FETCH_PARTITIONS);
    assert_eq!(owner.events.retained(), (0, 0));
    assert_eq!(owner.positions.retained_positions(), 0);
    assert_eq!(owner.fetches.retained(), (0, 0, 0));
    assert_eq!(owner.effect_count_for_test(), 0);
    assert_eq!(owner.pending_count_for_test(), 0);
    assert_eq!(owner.timer_count_for_test(), 0);
    assert_eq!(owner.next_deadline(), None);
}

#[test]
fn selected_read_isolation_reaches_the_core_machine_and_generated_fetch() {
    for (read_isolation, fetch_isolation, wire_isolation) in [
        (
            ReadIsolation::ReadUncommitted,
            FetchIsolation::ReadUncommitted,
            0,
        ),
        (
            ReadIsolation::ReadCommitted,
            FetchIsolation::ReadCommitted,
            1,
        ),
    ] {
        let owner = ClassicGroupFetchOwner::try_new_with_read_isolation(read_isolation)
            .unwrap_or_else(|error| panic!("Fetch owner: {error:?}"));

        assert_eq!(owner.machine.read_isolation(), read_isolation);
        assert_eq!(owner.fetch_settings.isolation(), Some(fetch_isolation));
        let request = fetch_request("orders", 0, 17, owner.fetch_settings)
            .unwrap_or_else(|error| panic!("generated Fetch request: {error:?}"));
        assert_eq!(request.isolation_level, wire_isolation);
    }
}

#[test]
fn selected_missing_offset_policy_reaches_the_fetch_owner() {
    for policy in [
        GroupPositionMissingOffsetPolicy::Error,
        GroupPositionMissingOffsetPolicy::Earliest,
        GroupPositionMissingOffsetPolicy::Latest,
    ] {
        let owner =
            ClassicGroupFetchOwner::try_new_with_policies(ReadIsolation::ReadUncommitted, policy)
                .unwrap_or_else(|error| panic!("Fetch owner: {error:?}"));
        assert_eq!(owner.missing_offset_policy, policy);
    }
}

#[test]
fn selected_consumer_limits_size_the_group_fetch_owner() {
    let default_fetch = EngineConsumerFetchConfig::default();
    let fetch = EngineConsumerFetchConfig::new(
        default_fetch.max_wait(),
        default_fetch.min_bytes(),
        default_fetch.max_bytes(),
        256 * 1024,
        default_fetch.attempt_timeout(),
    )
    .validate()
    .unwrap_or_else(|error| panic!("Fetch policy: {error:?}"));
    let limits = EngineConsumerLimits::new(3, 5, 4 * 1024 * 1024, 256 * 1024)
        .validate()
        .unwrap_or_else(|error| panic!("consumer limits: {error:?}"));
    let owner = ClassicGroupFetchOwner::try_new_with_fetch_configuration(
        ReadIsolation::ReadUncommitted,
        GroupPositionMissingOffsetPolicy::Error,
        fetch,
        limits,
    )
    .unwrap_or_else(|error| panic!("Fetch owner: {error:?}"));

    assert_eq!(owner.fetches.retained(), (0, 0, 0));
    assert_eq!(owner.delivery_capacity, 5);
    assert_eq!(owner.hard_fetch_output_bytes, 256 * 1024);
    assert!(owner.reclaim_faults.capacity() >= 5);
}
