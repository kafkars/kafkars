//! A concrete controller disconnect must settle through causal refresh before replay.

use std::time::{Duration, Instant};

use kafka_client_core::{CreateTopicsInput, Deadline, Moment, OperationId};

use crate::{EngineConfig, clock::OperationDeadline, driver::DriverOwner};

use super::{TrackedCreateTopicsCalls, broker::LoopbackBroker, poll_ready, visibility_plan};

#[test]
fn unsent_controller_disconnect_refreshes_before_one_replacement_request() {
    let mut broker = LoopbackBroker::bind();
    let mut driver = DriverOwner::build(&EngineConfig::new(vec![broker.address()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    broker.initialize(&mut driver);
    let deadline = OperationDeadline::from_parts_for_test(
        Deadline::from_tick(5_000_000_000),
        Instant::now() + Duration::from_secs(10),
    );
    let id = OperationId::from_raw(29);
    let mut calls = TrackedCreateTopicsCalls::new(1);
    submit(&mut calls, &driver, id, deadline);
    broker.fail_controller_before_negotiation(&mut driver);

    let limit = Instant::now() + Duration::from_secs(3);
    while calls.next_deadline().is_none() {
        assert!(
            Instant::now() < limit,
            "unsent controller failure must settle promptly"
        );
        driver
            .turn(Duration::from_millis(10))
            .unwrap_or_else(|error| panic!("observe controller failure: {error}"));
        assert!(
            calls
                .poll_next_ready()
                .unwrap_or_else(|error| panic!("retain controller failure: {error}"))
                .is_none()
        );
    }
    assert_eq!(calls.next_deadline(), Some(deadline.core()));
    assert!(calls.try_reserve().is_none());
    assert!(!calls.advance_one_settlement(&driver, Moment::from_tick(2)));
    broker.respond_controller_refresh(&mut driver);

    while !calls.advance_one_settlement(&driver, Moment::from_tick(3)) {
        assert!(
            Instant::now() < limit,
            "controller invalidation must complete"
        );
        driver
            .turn(Duration::from_millis(10))
            .unwrap_or_else(|error| panic!("settle controller refresh: {error}"));
    }
    let ready = calls
        .poll_next_ready()
        .unwrap_or_else(|error| panic!("poll refreshed route: {error}"))
        .unwrap_or_else(|| panic!("refreshed unsent fact must be ready"));
    assert_eq!(
        ready.take_input(),
        Some(CreateTopicsInput::ControllerRouteUnavailable {
            now: Moment::from_tick(3),
        })
    );
    assert!(calls.try_reserve().is_none());
    assert!(calls.discard_settled(id));
    submit(&mut calls, &driver, id, deadline);
    broker.respond_create_topics(&mut driver);
    assert!(matches!(
        poll_ready(&mut calls, &mut driver, "settle replacement request").take_input(),
        Some(CreateTopicsInput::BrokerResponded { .. })
    ));
    assert!(calls.discard_settled(id));
    assert_eq!(calls.retained_count(), 0);
}

fn submit(
    calls: &mut TrackedCreateTopicsCalls,
    driver: &DriverOwner,
    id: OperationId,
    deadline: OperationDeadline,
) {
    calls
        .try_reserve()
        .unwrap_or_else(|| panic!("one call slot required"))
        .submit(
            driver,
            id,
            deadline,
            visibility_plan(),
            64 * 1024,
            Moment::from_tick(1),
        )
        .unwrap_or_else(|error| panic!("submit creation: {error:?}"));
}
