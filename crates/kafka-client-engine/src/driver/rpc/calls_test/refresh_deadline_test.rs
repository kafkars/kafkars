//! Evidence that retained Produce-route refresh cannot outlive delivery.

use std::{num::NonZeroI16, time::Duration};

use kafka_client_core::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, Deadline, DeliveryStatus, Moment,
    ProducerBrokerFailure, ProducerBrokerFailureKind, ProducerInput,
};

use crate::{
    EngineConfig,
    driver::{DriverOwner, ProduceRouteRefreshPoll, TrackedProduceCalls},
};

#[test]
fn pending_route_refresh_expires_at_the_original_delivery_deadline() {
    let mut driver = owner();
    let input = ProducerInput::BrokerFailed {
        execution: execution(7),
        now: Moment::from_tick(12),
        failure: ProducerBrokerFailure::new(
            ProducerBrokerFailureKind::Routing,
            NonZeroI16::new(6).unwrap_or_else(|| panic!("routing code is nonzero")),
        ),
        delivery: DeliveryStatus::PossiblySent,
        route_refreshed: false,
    };
    let exact_deadline = Deadline::from_tick(14);
    let mut calls = TrackedProduceCalls::with_submit_then_pending_refresh_for_test(
        execution(7),
        exact_deadline,
        input,
    );
    assert_eq!(calls.next_refresh_deadline(), Some(exact_deadline));
    {
        let settled = calls
            .poll_next_ready(Moment::from_tick(13))
            .unwrap_or_else(|error| panic!("poll retained terminal: {error}"))
            .unwrap_or_else(|| panic!("test terminal remains retained"));
        assert_eq!(
            settled.poll_route_refresh(&driver, Moment::from_tick(13)),
            ProduceRouteRefreshPoll::Submitted
        );
        assert_eq!(settled.input(), input);
    }
    let settled = calls
        .poll_next_ready(Moment::from_tick(14))
        .unwrap_or_else(|error| panic!("poll pending refresh: {error}"))
        .unwrap_or_else(|| panic!("pending refresh remains retained"));
    assert_eq!(
        settled.poll_route_refresh(&driver, Moment::from_tick(14)),
        ProduceRouteRefreshPoll::Ready
    );
    assert_eq!(
        settled.input(),
        ProducerInput::RouteRefreshDeadlineElapsed {
            execution: execution(7),
            now: Moment::from_tick(14),
            delivery: DeliveryStatus::PossiblySent,
        }
    );
    calls.discard_settled(Moment::from_tick(14));
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
}

fn execution(batch: u64) -> BatchExecutionId {
    BatchExecutionId::new(
        BatchId::from_raw(batch),
        BatchExecutionGeneration::initial(),
    )
}

fn owner() -> DriverOwner {
    DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("build embedded driver owner: {error}"))
}
