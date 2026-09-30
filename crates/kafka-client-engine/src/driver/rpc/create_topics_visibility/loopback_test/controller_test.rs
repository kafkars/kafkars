//! A concrete controller disconnect must settle through causal refresh before replay.

use std::time::{Duration, Instant};

use kafka_client_core::{CreateTopicsInput, Deadline, Moment, OperationId};

use crate::{EngineConfig, clock::OperationDeadline, driver::DriverOwner};

use super::{TrackedCreateTopicsCalls, broker::LoopbackBroker, poll_ready, visibility_plan};

#[test]
fn cold_controller_on_a_different_broker_waits_for_negotiation() {
    let mut seed = LoopbackBroker::bind();
    let mut controller = LoopbackBroker::bind();
    let mut driver = DriverOwner::build(&EngineConfig::new(vec![seed.address()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    seed.initialize_with_controller(&mut driver, &controller, "127.0.0.1");
    let deadline = OperationDeadline::from_parts_for_test(
        Deadline::from_tick(5_000_000_000),
        Instant::now() + Duration::from_secs(10),
    );
    let id = OperationId::from_raw(30);
    let mut calls = TrackedCreateTopicsCalls::new(1);
    submit(&mut calls, &driver, id, deadline, false);
    let delay = Instant::now() + Duration::from_millis(150);
    while Instant::now() < delay {
        driver
            .turn(Duration::from_millis(10))
            .unwrap_or_else(|error| panic!("wait for healthy controller negotiation: {error}"));
        assert!(
            calls
                .poll_next_ready()
                .unwrap_or_else(|error| panic!("poll unsent call: {error}"))
                .is_none()
        );
        assert!(
            calls.next_deadline().is_none(),
            "healthy route is not a failure"
        );
    }
    controller.respond_create_topics(&mut driver);
    assert!(matches!(
        poll_ready(&mut calls, &mut driver, "settle cold controller").take_input(),
        Some(CreateTopicsInput::BrokerResponded { .. })
    ));
    assert!(calls.discard_settled(id));
}

#[test]
fn fallback_controller_address_does_not_exhaust_the_public_operation() {
    let mut seed = LoopbackBroker::bind();
    let mut controller = LoopbackBroker::bind_nonpreferred_localhost();
    let mut driver = DriverOwner::build(&EngineConfig::new(vec![seed.address()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    seed.initialize_with_controller(&mut driver, &controller, "localhost");
    let deadline = OperationDeadline::from_parts_for_test(
        Deadline::from_tick(5_000_000_000),
        Instant::now() + Duration::from_secs(10),
    );
    let id = OperationId::from_raw(31);
    let mut calls = TrackedCreateTopicsCalls::new(1);
    submit(&mut calls, &driver, id, deadline, false);
    let limit = Instant::now() + Duration::from_secs(3);
    while calls.next_deadline().is_none() {
        assert!(Instant::now() < limit, "first address failure must settle");
        driver
            .turn(Duration::from_millis(10))
            .unwrap_or_else(|error| panic!("observe first address failure: {error}"));
        assert!(
            calls
                .poll_next_ready()
                .unwrap_or_else(|error| panic!("poll address failure: {error}"))
                .is_none()
        );
    }
    assert!(!calls.advance_one_settlement(&driver, Moment::from_tick(2)));
    seed.refresh_separate_controller(&mut driver, &controller, "localhost");
    while !calls.advance_one_settlement(&driver, Moment::from_tick(3)) {
        assert!(Instant::now() < limit, "causal refresh must settle");
        driver
            .turn(Duration::from_millis(10))
            .unwrap_or_else(|error| panic!("observe refresh: {error}"));
    }
    assert!(matches!(
        poll_ready(&mut calls, &mut driver, "observe retry authorization").take_input(),
        Some(CreateTopicsInput::ControllerRouteUnavailable { .. })
    ));
    assert!(calls.discard_settled(id));
    submit(&mut calls, &driver, id, deadline, true);
    driver
        .turn(Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("submit fallback attempt: {error}"));
    assert!(
        calls
            .poll_next_ready()
            .unwrap_or_else(|error| panic!("poll fallback: {error}"))
            .is_none()
    );
    assert!(
        calls.next_deadline().is_none(),
        "replacement must wait for fallback instead of recycling the same route failure"
    );
    controller.respond_create_topics(&mut driver);
    assert!(matches!(
        poll_ready(&mut calls, &mut driver, "settle fallback controller").take_input(),
        Some(CreateTopicsInput::BrokerResponded { .. })
    ));
    assert!(calls.discard_settled(id));
}

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
    submit(&mut calls, &driver, id, deadline, false);
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
    submit(&mut calls, &driver, id, deadline, true);
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
    controller_retry: bool,
) {
    let permit = calls
        .try_reserve()
        .unwrap_or_else(|| panic!("one call slot required"));
    let permit = if controller_retry {
        permit.for_controller_retry()
    } else {
        permit
    };
    permit
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
