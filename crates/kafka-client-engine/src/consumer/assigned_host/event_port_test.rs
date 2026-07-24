//! Event extraction frees scalar capacity without reactor work or close admission.

use std::time::Duration;

use kafka_client_core::{AssignedConsumerEffect, FetchFailure, StartPosition};

use super::super::{
    assigned_event::AssignedConsumerEvent, assigned_owner_effect::FrontEffect,
    assigned_owner_test::input,
};
use super::shard_test::setup;

#[test]
fn take_event_releases_capacity_without_reactor_wake() {
    let (owner, port, wake) = setup();
    let _accepted = port
        .replace_assignment(
            vec![input("orders", 0, StartPosition::Offset(offset(10)))],
            Duration::from_secs(1),
        )
        .unwrap_or_else(|error| panic!("assign: {error:?}"));
    let fence = owner
        .try_with_owner(|assigned| {
            let Some(AssignedConsumerEffect::FetchReady { fence, .. }) =
                assigned.effects.front().copied()
            else {
                panic!("initial Fetch claim");
            };
            assert_eq!(assigned.interpret_front_effect(), FrontEffect::Interpreted);
            assigned
                .effects
                .push_back(AssignedConsumerEffect::FetchFailed {
                    fence,
                    failure: FetchFailure::Transport,
                });
            assert_eq!(assigned.interpret_front_effect(), FrontEffect::Interpreted);
            assert_eq!(assigned.events.retained(), (0, 1));
            fence
        })
        .unwrap_or_else(|error| panic!("owner slot: {error:?}"));
    let wakes = wake.count();
    owner
        .close_assigned_admission()
        .unwrap_or_else(|error| panic!("close admission: {error:?}"));

    let event = port
        .take_event()
        .unwrap_or_else(|error| panic!("take event after close: {error:?}"))
        .unwrap_or_else(|| panic!("ready event"));

    assert!(matches!(
        event,
        AssignedConsumerEvent::FetchFailed {
            topic,
            fence: actual,
            failure: FetchFailure::Transport,
        } if topic.as_ref() == "orders" && actual == fence
    ));
    assert_eq!(wake.count(), wakes);
    assert_eq!(
        owner
            .try_with_owner(|assigned| assigned.events.retained())
            .unwrap_or_else(|error| panic!("retained events: {error:?}")),
        (0, 0)
    );
}

fn offset(value: i64) -> kafka_client_core::NextFetchOffset {
    kafka_client_core::NextFetchOffset::try_from_raw(value)
        .unwrap_or_else(|| panic!("nonnegative offset"))
}
