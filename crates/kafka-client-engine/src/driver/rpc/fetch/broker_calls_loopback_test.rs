//! Live exact-broker aggregation and route-token lifetime scenarios.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use kafka_client_core::{
    AssignedConsumerInput, AssignedConsumerMachine, AssignedPartition, AssignedTopicPartition,
    Deadline, FetchFence, Moment, NextFetchOffset, PartitionIndex, StartPosition, TopicId,
};
use kafka_driver::{BrokerId, RouteKind};

use crate::{
    EngineConfig,
    clock::OperationDeadline,
    driver::DriverOwner,
    protocol::fetch::{
        FetchDecodeLimits, FetchRequestSettings, FetchSessionRequest, OwnedForgottenFetchPartition,
    },
};

use super::{
    admission::PartitionFetchRequest,
    broker_calls::{BrokerFetchCallAdmission, TrackedBrokerFetchCalls},
    forgotten::{ForgottenFetchRequest, TrackedForgottenFetchCall},
    routed_response_broker_test::{self as broker, RoutedBroker},
    settlement::FetchPoll,
};

#[test]
fn forgotten_only_call_reaches_broker_and_retains_confirmation_authority() {
    let mut broker = RoutedBroker::new();
    let mut driver = DriverOwner::build(&EngineConfig::new(vec![broker.endpoint()]))
        .unwrap_or_else(|error| panic!("build forgotten Fetch driver: {error}"));
    RoutedBroker::await_seed(&mut driver);
    broker.install_cluster(&mut driver);
    let session =
        FetchSessionRequest::incremental(91, 3).unwrap_or_else(|| panic!("incremental session"));
    let request = ForgottenFetchRequest::new(
        FetchRequestSettings::new(500, 1, 1_024, 1_024, 0),
        session,
        OperationDeadline::from_parts_for_test(
            Deadline::from_tick(60_000_000_000),
            Instant::now() + Duration::from_secs(60),
        ),
        vec![OwnedForgottenFetchPartition::new(Arc::from("events"), 3)],
    );
    let mut call = TrackedForgottenFetchCall::submit(
        &driver,
        BrokerId::new(1).unwrap_or_else(|error| panic!("broker ID: {error}")),
        request,
        Moment::from_tick(0),
    )
    .unwrap_or_else(|failure| {
        let (_request, kind) = failure.into_parts();
        panic!("submit forgotten Fetch: {kind:?}")
    });

    let (version, generated) = broker.complete_fetch_request(&mut driver);
    assert_eq!(version.value(), 12);
    assert!(generated.topics.is_empty());
    assert_eq!((generated.session_id, generated.session_epoch), (91, 3));
    assert_eq!(generated.forgotten_topics_data.len(), 1);
    let terminal = (0..32)
        .find_map(|turn| {
            let terminal = call.try_terminal(Moment::from_tick(7 + turn));
            if terminal.is_none() {
                broker::drive(
                    &mut driver,
                    Duration::from_millis(100),
                    "settle forgotten Fetch",
                );
            }
            terminal
        })
        .unwrap_or_else(|| panic!("forgotten Fetch terminal"))
        .unwrap_or_else(|failure| {
            let (_request, source) = failure.recover_after_driver_shutdown();
            panic!("forgotten Fetch completion: {source}")
        });
    let (request, observed_at, selected_version, result, confirmation) = terminal.into_parts();
    assert_eq!(request.session(), session);
    assert!(observed_at >= Moment::from_tick(7));
    assert_eq!(selected_version, Some(12));
    assert!(result.is_ok());
    confirmation.confirm();
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
}

#[test]
fn one_live_broker_call_contains_two_partitions_and_confirms_once() {
    let mut broker = RoutedBroker::new();
    let mut driver = DriverOwner::build(&EngineConfig::new(vec![broker.endpoint()]))
        .unwrap_or_else(|error| panic!("build aggregate Fetch driver: {error}"));
    RoutedBroker::await_seed(&mut driver);
    broker.install_cluster(&mut driver);
    let requests = requests();
    let fences = requests
        .iter()
        .map(PartitionFetchRequest::fence)
        .collect::<Vec<_>>();
    let mut calls = TrackedBrokerFetchCalls::new(1);
    assert!(matches!(
        calls.try_submit(
            &driver,
            BrokerId::new(1).unwrap_or_else(|error| panic!("broker ID: {error}")),
            requests,
            &[],
            Moment::from_tick(0),
        ),
        BrokerFetchCallAdmission::Accepted
    ));

    let (version, request) = broker.complete_fetch_request(&mut driver);
    assert_eq!(version.value(), 12);
    assert_eq!(request.topics.len(), 1);
    assert_eq!(request.topics[0].topic.as_str(), "events");
    assert_eq!(
        request.topics[0]
            .partitions
            .iter()
            .map(|partition| partition.partition)
            .collect::<Vec<_>>(),
        vec![3, 4]
    );
    wait_for_terminal(&mut driver, &mut calls, fences[0]);
    assert_eq!(calls.settled_route_kind_for_test(), Some(RouteKind::Broker));
    settle(&mut calls, fences[0]);
    assert_eq!(calls.settled_route_kind_for_test(), Some(RouteKind::Broker));
    settle(&mut calls, fences[1]);
    assert_eq!(calls.retained_count(), 0);
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
}

fn wait_for_terminal(
    driver: &mut DriverOwner,
    calls: &mut TrackedBrokerFetchCalls,
    expected: FetchFence,
) {
    for turn in 0..32 {
        match calls.poll_fetch(Moment::from_tick(10 + turn)) {
            Ok(FetchPoll::TerminalReady { fence }) => {
                assert_eq!(fence, expected);
                return;
            }
            Ok(FetchPoll::Idle) => {
                broker::drive(driver, Duration::from_millis(100), "settle aggregate Fetch")
            }
            Ok(FetchPoll::StaleConfirmationReady { .. }) => {
                panic!("live aggregate Fetch became stale")
            }
            Err(error) => panic!("aggregate Fetch completion: {error:?}"),
        }
    }
    panic!("aggregate Fetch did not settle")
}

fn settle(calls: &mut TrackedBrokerFetchCalls, fence: FetchFence) {
    let terminal = calls
        .begin_fetch_settlement(fence)
        .unwrap_or_else(|error| panic!("begin aggregate terminal: {error:?}"));
    assert_eq!(terminal.fence(), fence);
    calls
        .confirm_fetch_settlement(fence)
        .unwrap_or_else(|error| panic!("confirm aggregate terminal: {error:?}"));
}

fn requests() -> Vec<PartitionFetchRequest> {
    let mut machine = AssignedConsumerMachine::new();
    machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![assigned(3, 10), assigned(4, 20)],
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(60_000_000_000),
        })
        .unwrap_or_else(|error| panic!("assignment: {error}"))
        .into_effects()
        .into_iter()
        .map(|effect| {
            PartitionFetchRequest::from_effect(
                effect,
                "events".to_owned(),
                FetchRequestSettings::new(500, 1, 1024 * 1024, 1024 * 1024, 0),
                FetchDecodeLimits::default(),
                OperationDeadline::from_parts_for_test(
                    Deadline::from_tick(60_000_000_000),
                    Instant::now() + Duration::from_secs(60),
                ),
            )
            .unwrap_or_else(|error| panic!("prepared Fetch: {error:?}"))
        })
        .collect()
}

fn assigned(partition: u32, offset: i64) -> AssignedPartition {
    AssignedPartition::new(
        AssignedTopicPartition::new(TopicId::from_raw(1), PartitionIndex::from_raw(partition)),
        StartPosition::Offset(
            NextFetchOffset::try_from_raw(offset).unwrap_or_else(|| panic!("valid offset")),
        ),
    )
}
