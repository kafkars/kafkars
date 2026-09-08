//! Real metadata exchanges required before a group can reset an uncommitted position.

use std::time::{Duration, Instant};

use kafka_client_core::{PartitionIndex, PositionResolutionAttemptFailure};
use kafka_wire::ListOffsetsRequest;

use crate::{EngineConfig, driver::DriverOwner, protocol::consumer::ListOffsetsIsolation};

use super::{
    super::{
        fetch::routed_response_broker_test::RoutedBroker,
        list_offsets_terminal::ListOffsetsResolution, topic_view::TopicRouteViewCall,
    },
    call::ClassicGroupPositionResetCall,
};

#[test]
fn reset_refreshes_cached_leadership_and_keeps_the_original_deadline() {
    let mut broker = RoutedBroker::new();
    let mut owner = DriverOwner::build(&EngineConfig::new(vec![broker.endpoint()]))
        .unwrap_or_else(|error| panic!("build reset driver: {error}"));
    RoutedBroker::await_seed(&mut owner);
    broker.install_cluster(&mut owner);
    let mut cached =
        TopicRouteViewCall::submit(&owner, "events", Instant::now() + Duration::from_secs(10))
            .unwrap_or_else(|error| panic!("cache old leader: {error}"));
    broker.install_topic(&mut owner, 1);
    let view = (0..32)
        .find_map(|_| {
            drive(&mut owner, Duration::from_millis(10));
            cached.try_terminal()
        })
        .unwrap_or_else(|| panic!("old leader metadata must settle"))
        .unwrap_or_else(|error| panic!("old leader metadata: {error:?}"));
    assert_eq!(view.leader_broker_id(PartitionIndex::from_raw(3)), Some(1));

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut call = ClassicGroupPositionResetCall::submit(
        &owner,
        "events",
        3,
        ListOffsetsRequest::default(),
        deadline,
    )
    .unwrap_or_else(|error| panic!("reset admission: {error}"));
    for _ in 0..8 {
        drive(&mut owner, Duration::ZERO);
        assert!(call.try_result(&owner).is_none());
    }
    // The broker must see fresh Metadata even though its old leader was cached.
    // A leaderless reply must retain the same deadline for the next lookup.
    broker.install_topic(&mut owner, -1);
    let terminal = loop {
        drive(&mut owner, Duration::from_millis(10));
        if let Some(terminal) = call.try_result(&owner) {
            break terminal.unwrap_or_else(|error| panic!("reset completion: {error:?}"));
        }
        assert!(Instant::now() < deadline + Duration::from_secs(1));
    };
    let (resolution, route) = terminal.into_resolution(
        "events",
        PartitionIndex::from_raw(3),
        ListOffsetsIsolation::ReadUncommitted,
    );
    assert_eq!(
        resolution,
        ListOffsetsResolution::Failed(PositionResolutionAttemptFailure::DeadlineElapsed)
    );
    drop(route);
    drop(broker);
    owner
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shutdown: {error}"));
}

fn drive(owner: &mut DriverOwner, wait: Duration) {
    owner
        .turn(wait)
        .unwrap_or_else(|error| panic!("drive reset: {error}"));
}
