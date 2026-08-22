//! Exact-broker admission and request-recovery scenarios.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use kafka_client_core::{
    AssignedConsumerInput, AssignedConsumerMachine, AssignedPartition, AssignedTopicPartition,
    Deadline, Moment, NextFetchOffset, PartitionIndex, StartPosition, TopicId,
};

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
    route::BrokerId,
};

#[test]
fn forgotten_only_call_is_accepted_and_recovers_exact_request_after_shutdown() {
    let mut driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("build forgotten Fetch driver: {error}"));
    let session =
        FetchSessionRequest::incremental(91, 3).unwrap_or_else(|| panic!("incremental session"));
    let request = ForgottenFetchRequest::new(
        FetchRequestSettings::new(500, 1, 1_024, 1_024, 0),
        session,
        OperationDeadline::from_parts_for_test(
            Deadline::from_tick(60_000_000_000),
            Instant::now() + Duration::from_secs(60),
        ),
        vec![OwnedForgottenFetchPartition::new(
            Arc::from("events"),
            [7; 16],
            3,
        )],
    );
    let call = TrackedForgottenFetchCall::submit(
        &driver,
        BrokerId::new(1).unwrap_or_else(|error| panic!("broker ID: {error}")),
        request,
        Moment::from_tick(0),
    )
    .unwrap_or_else(|failure| {
        panic!(
            "exact-broker forgotten Fetch admission: {:?}",
            failure.into_parts().1
        )
    });
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
    assert_eq!(call.recover_after_driver_shutdown().session(), session);
}

#[test]
fn aggregate_call_is_accepted_and_recovers_every_partition_after_shutdown() {
    let mut driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("build aggregate Fetch driver: {error}"));
    let requests = requests();
    let fences = requests
        .iter()
        .map(PartitionFetchRequest::fence)
        .collect::<Vec<_>>();
    let mut calls = TrackedBrokerFetchCalls::new(1);
    match calls.try_submit(
        &driver,
        BrokerId::new(1).unwrap_or_else(|error| panic!("broker ID: {error}")),
        requests,
        &[],
        Moment::from_tick(0),
    ) {
        BrokerFetchCallAdmission::Accepted => {}
        BrokerFetchCallAdmission::Rejected(failure) => {
            panic!("exact-broker Fetch rejected: {:?}", failure.into_parts().1)
        }
        BrokerFetchCallAdmission::Backpressured(_) => {
            panic!("available admission capacity must not report backpressure")
        }
    }
    assert_eq!(calls.retained_count(), 1);
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
    let (requests, completion) = calls.recover_after_driver_shutdown().into_parts();
    assert_eq!(
        requests
            .iter()
            .map(PartitionFetchRequest::fence)
            .collect::<Vec<_>>(),
        fences
    );
    assert!(completion.is_none());
    assert_eq!(calls.retained_count(), 0);
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
            let mut request = PartitionFetchRequest::from_effect(
                effect,
                "events".to_owned(),
                FetchRequestSettings::new(500, 1, 1024 * 1024, 1024 * 1024, 0),
                FetchDecodeLimits::default(),
                OperationDeadline::from_parts_for_test(
                    Deadline::from_tick(60_000_000_000),
                    Instant::now() + Duration::from_secs(60),
                ),
            )
            .unwrap_or_else(|error| panic!("prepared Fetch: {error:?}"));
            request.bind_topic_route(super::topic_route::FetchTopicRoute::new([7; 16], Some(9)));
            request
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
