//! Real driver-routed Fetch response ownership through live and stale settlement.

use std::time::{Duration, Instant};

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine, AssignedPartition,
    AssignedTopicPartition, Deadline, FetchFence, Moment, NextFetchOffset, PartitionIndex,
    StartPosition, TopicId,
};
use kafka_driver::RouteKind;

use crate::{
    EngineConfig,
    clock::OperationDeadline,
    driver::DriverOwner,
    protocol::fetch::{FetchDecodeLimits, FetchRequestSettings},
};

use super::{
    admission::{FetchCallAdmission, PartitionFetchRequest},
    calls::TrackedFetchCalls,
    routed_response_broker_test as broker,
    settlement::{FetchConfirmationError, FetchPoll, StaleFetchConfirmationError},
};

const TEST_DEADLINE_TICK: u64 = 60_000_000_000;
const TEST_TRANSPORT_BUDGET: Duration = Duration::from_secs(60);

#[test]
fn actual_route_token_survives_restore_and_only_exact_live_confirmation_releases_it() {
    let RoutedFetch {
        mut driver,
        mut calls,
        fence,
        _broker,
    } = routed_fetch();
    assert_eq!(settled_route_kind(&calls), Some(RouteKind::PartitionLeader));

    let terminal = calls
        .begin_fetch_settlement(fence)
        .unwrap_or_else(|error| panic!("begin routed settlement: {error:?}"));
    assert_eq!(pending_route_kind(&calls), Some(RouteKind::PartitionLeader));
    let wrong = fence_for_partition(4);
    assert!(matches!(
        calls.confirm_fetch_settlement(wrong),
        Err(FetchConfirmationError::FenceMismatch { pending, supplied })
            if pending == fence && supplied == wrong
    ));
    assert_eq!(pending_route_kind(&calls), Some(RouteKind::PartitionLeader));

    calls
        .restore_fetch_settlement(terminal)
        .unwrap_or_else(|failure| panic!("restore routed terminal: {:?}", failure.into_parts().1));
    assert!(calls.pending_confirmation.is_none());
    assert_eq!(settled_route_kind(&calls), Some(RouteKind::PartitionLeader));

    let terminal = calls
        .begin_fetch_settlement(fence)
        .unwrap_or_else(|error| panic!("begin restored terminal: {error:?}"));
    assert_eq!(pending_route_kind(&calls), Some(RouteKind::PartitionLeader));
    let (request, _observed_at, selected_version, response) = terminal.into_parts();
    assert_eq!(request.fence(), fence);
    assert_eq!(selected_version, Some(12));
    assert!(response.is_ok());
    calls
        .confirm_fetch_settlement(fence)
        .unwrap_or_else(|error| panic!("confirm exact routed terminal: {error:?}"));
    assert!(calls.pending_confirmation.is_none());
    assert_eq!(calls.retained_count(), 0);
    shutdown(&mut driver);
}

#[test]
fn actual_stale_route_token_requires_exact_stale_confirmation() {
    let RoutedFetch {
        mut driver,
        mut calls,
        fence,
        _broker,
    } = routed_fetch();
    let revoke = AssignedConsumerEffect::Revoke {
        assignment_epoch: fence.position().assignment_epoch(),
        partition: fence.position().partition(),
    };
    let returned = calls
        .observe_fetch_control(revoke)
        .unwrap_or_else(|pending| panic!("no live settlement pending: {:?}", pending.fence))
        .into_requests();
    assert_eq!(returned.len(), 1);
    assert_eq!(returned[0].fence(), fence);
    assert_eq!(settled_route_kind(&calls), Some(RouteKind::PartitionLeader));

    let wrong = fence_for_partition(4);
    assert!(matches!(
        calls.confirm_stale_fetch(wrong),
        Err(StaleFetchConfirmationError::FenceMismatch { settled, supplied })
            if settled == fence && supplied == wrong
    ));
    assert_eq!(settled_route_kind(&calls), Some(RouteKind::PartitionLeader));
    calls
        .confirm_stale_fetch(fence)
        .unwrap_or_else(|error| panic!("confirm exact stale route token: {error:?}"));
    assert!(calls.settled.is_none());
    assert_eq!(calls.retained_count(), 0);
    shutdown(&mut driver);
}

fn routed_fetch() -> RoutedFetch {
    let mut broker = broker::RoutedBroker::new();
    let mut driver = DriverOwner::build(&EngineConfig::new(vec![broker.endpoint()]))
        .unwrap_or_else(|error| panic!("build routed Fetch driver: {error}"));
    broker.install_cluster(&mut driver);

    let effect = assignment_effect(3);
    let fence = fetch_fence(effect);
    let mut calls = TrackedFetchCalls::new(1);
    assert!(matches!(
        calls.try_submit_fetch(&driver, request(effect), Moment::from_tick(0),),
        FetchCallAdmission::Accepted
    ));
    broker.install_topic(&mut driver);
    assert_eq!(broker.complete_fetch(&mut driver).value(), 12);
    wait_for_terminal(&mut driver, &mut calls, fence);
    RoutedFetch {
        driver,
        calls,
        fence,
        _broker: broker,
    }
}

fn wait_for_terminal(driver: &mut DriverOwner, calls: &mut TrackedFetchCalls, fence: FetchFence) {
    for turn in 0..32 {
        let now = Moment::from_tick(10 + turn);
        match calls.poll_fetch(now) {
            Ok(FetchPoll::TerminalReady { fence: observed }) => {
                assert_eq!(observed, fence);
                return;
            }
            Ok(FetchPoll::Idle) => {
                broker::drive(driver, Duration::from_millis(100), "settle routed Fetch");
            }
            Ok(FetchPoll::StaleConfirmationReady { .. }) => {
                panic!("live routed Fetch became stale before control")
            }
            Err(error) => panic!("routed Fetch completion ownership: {error:?}"),
        }
    }
    panic!("routed Fetch did not settle")
}

fn settled_route_kind(calls: &TrackedFetchCalls) -> Option<RouteKind> {
    calls
        .settled
        .as_ref()
        .and_then(super::settlement::SettledFetchCall::route_kind)
}

fn pending_route_kind(calls: &TrackedFetchCalls) -> Option<RouteKind> {
    calls
        .pending_confirmation
        .as_ref()
        .and_then(super::settlement::PendingFetchConfirmation::route_kind)
}

fn request(effect: AssignedConsumerEffect) -> PartitionFetchRequest {
    PartitionFetchRequest::from_effect(
        effect,
        "events".to_owned(),
        FetchRequestSettings::new(500, 1, 1024 * 1024, 1024 * 1024, 0),
        FetchDecodeLimits::default(),
        OperationDeadline::from_parts_for_test(
            Deadline::from_tick(TEST_DEADLINE_TICK),
            Instant::now() + TEST_TRANSPORT_BUDGET,
        ),
    )
    .unwrap_or_else(|error| panic!("prepare routed Fetch: {error:?}"))
}

fn fence_for_partition(partition: u32) -> FetchFence {
    fetch_fence(assignment_effect(partition))
}

fn assignment_effect(partition: u32) -> AssignedConsumerEffect {
    let mut machine = AssignedConsumerMachine::new();
    machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![AssignedPartition::new(
                AssignedTopicPartition::new(
                    TopicId::from_raw(1),
                    PartitionIndex::from_raw(partition),
                ),
                StartPosition::Offset(offset()),
            )],
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(TEST_DEADLINE_TICK),
        })
        .unwrap_or_else(|error| panic!("assign routed Fetch: {error}"))
        .effects()[0]
}

fn fetch_fence(effect: AssignedConsumerEffect) -> FetchFence {
    let AssignedConsumerEffect::FetchReady { fence, .. } = effect else {
        panic!("FetchReady effect");
    };
    fence
}

fn offset() -> NextFetchOffset {
    NextFetchOffset::try_from_raw(42).unwrap_or_else(|| panic!("valid offset"))
}

fn shutdown(driver: &mut DriverOwner) {
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded routed driver shutdown: {error}"));
}

struct RoutedFetch {
    driver: DriverOwner,
    calls: TrackedFetchCalls,
    fence: FetchFence,
    _broker: broker::RoutedBroker,
}
