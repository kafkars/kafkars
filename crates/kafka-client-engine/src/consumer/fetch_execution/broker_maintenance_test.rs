//! Forgotten-only scheduling, capacity, and shutdown ownership scenarios.

use std::sync::Arc;

use kafka_client_core::{AssignedConsumerInput, Deadline, Moment};

use crate::{
    clock::MonotonicClock,
    driver::BrokerId,
    protocol::fetch::{FetchRequestSettings, FetchSessionRequest},
};

use super::{
    admission_test::{assignment, configure_broker_sessions, fetch_fence, owner, shutdown},
    broker_maintenance::request_from_plan,
    broker_maintenance_state::BrokerSessionMaintenance,
    broker_session::BrokerSessionMember,
    executor::DirectFetchExecutor,
};

#[test]
fn forgotten_only_work_uses_one_bounded_call_and_recovers_after_shutdown() {
    let (effect, mut machine) = assignment(3, Deadline::from_tick(100));
    let fence = fetch_fence(effect);
    let broker = broker();
    let mut executor = established_executor(effect, broker, 1);
    let transition = machine
        .apply(AssignedConsumerInput::Pause {
            assignment_epoch: fence.position().assignment_epoch(),
            partition: fence.position().partition(),
        })
        .unwrap_or_else(|error| panic!("pause: {error}"));
    executor
        .observe_control(transition.effects()[0])
        .unwrap_or_else(|error| panic!("observe pause: {error:?}"));
    let mut driver = owner();

    let (transition, progressed) = executor
        .drive_broker_fetches(
            &driver,
            &mut machine,
            &MonotonicClock::new(),
            Moment::from_tick(0),
        )
        .unwrap_or_else(|error| panic!("drive maintenance: {error:?}"));
    assert!(transition.is_none());
    assert!(progressed);
    assert_eq!(executor.retained(), (0, 0, 0));
    assert!(!executor.broker_sessions_have_forgotten_ready());
    assert_eq!(executor.broker_session_maintenance_deadline(), None);

    shutdown(&mut driver);
    let recovery = executor.release_fetch_executor_after_driver_shutdown();
    assert!(!recovery.had_fault());
    let (requests, completion) = recovery.into_driver_recovery().into_parts();
    assert!(requests.is_empty());
    assert_eq!(completion, None);
}

#[test]
fn zero_call_capacity_does_not_begin_or_extend_maintenance() {
    let (effect, mut machine) = assignment(3, Deadline::from_tick(100));
    let fence = fetch_fence(effect);
    let mut executor = established_executor(effect, broker(), 0);
    let transition = machine
        .apply(AssignedConsumerInput::Pause {
            assignment_epoch: fence.position().assignment_epoch(),
            partition: fence.position().partition(),
        })
        .unwrap_or_else(|error| panic!("pause: {error}"));
    executor
        .observe_control(transition.effects()[0])
        .unwrap_or_else(|error| panic!("observe pause: {error:?}"));
    let mut driver = owner();

    let (transition, progressed) = executor
        .drive_broker_fetches(
            &driver,
            &mut machine,
            &MonotonicClock::new(),
            Moment::from_tick(0),
        )
        .unwrap_or_else(|error| panic!("drive bounded maintenance: {error:?}"));
    assert!(transition.is_none());
    assert!(!progressed);
    assert_eq!(executor.retained(), (0, 0, 0));
    assert!(
        executor
            .broker_sessions
            .as_ref()
            .is_some_and(super::broker_session::BrokerFetchSessions::has_forgotten_ready)
    );

    shutdown(&mut driver);
}

#[test]
fn assignment_replacement_defers_forgotten_maintenance_until_new_work_is_ready() {
    let (effect, mut machine) = assignment(3, Deadline::from_tick(100));
    let fence = fetch_fence(effect);
    let mut executor = established_executor(effect, broker(), 1);
    let transition = machine
        .apply(AssignedConsumerInput::Pause {
            assignment_epoch: fence.position().assignment_epoch(),
            partition: fence.position().partition(),
        })
        .unwrap_or_else(|error| panic!("pause: {error}"));
    executor
        .observe_control(transition.effects()[0])
        .unwrap_or_else(|error| panic!("observe pause: {error:?}"));
    executor.defer_broker_session_maintenance();
    let mut driver = owner();

    let deferred = executor
        .drive_broker_fetches(
            &driver,
            &mut machine,
            &MonotonicClock::new(),
            Moment::from_tick(0),
        )
        .unwrap_or_else(|error| panic!("defer forgotten maintenance: {error:?}"));
    assert_eq!(deferred, (None, false));
    assert_eq!(executor.retained(), (0, 0, 0));

    executor.resume_broker_session_maintenance();
    let resumed = executor
        .drive_broker_fetches(
            &driver,
            &mut machine,
            &MonotonicClock::new(),
            Moment::from_tick(1),
        )
        .unwrap_or_else(|error| panic!("resume forgotten maintenance: {error:?}"));
    assert_eq!(resumed, (None, true));
    assert_eq!(executor.retained(), (0, 0, 0));
    assert!(!executor.broker_sessions_have_forgotten_ready());
    shutdown(&mut driver);
}

#[test]
fn prepared_maintenance_keeps_its_deadline_and_blocks_ordinary_dispatch() {
    let (effect, mut machine) = assignment(3, Deadline::from_tick(100));
    let fence = fetch_fence(effect);
    let prepared = super::admission_test::prepared(effect, 100, 1_024);
    let broker = broker();
    let mut executor = established_executor(effect, broker, 1);
    let transition = machine
        .apply(AssignedConsumerInput::Pause {
            assignment_epoch: fence.position().assignment_epoch(),
            partition: fence.position().partition(),
        })
        .unwrap_or_else(|error| panic!("pause: {error}"));
    executor
        .observe_control(transition.effects()[0])
        .unwrap_or_else(|error| panic!("observe pause: {error:?}"));
    let plan = executor
        .broker_sessions
        .as_mut()
        .and_then(|sessions| sessions.try_begin_forgotten().ok().flatten())
        .unwrap_or_else(|| panic!("forgotten plan"));
    let deadline = super::admission_test::operation_deadline(77);
    let request = request_from_plan(
        &plan,
        FetchRequestSettings::new(500, 1, 1_024, 1_024, 0),
        deadline,
    )
    .unwrap_or_else(|()| panic!("forgotten request"));
    executor.broker_maintenance = Some(BrokerSessionMaintenance::Prepared { plan, request });
    executor.restore_routed(broker, prepared);
    assert_eq!(
        executor.broker_session_maintenance_deadline(),
        Some(Deadline::from_tick(77))
    );
    let mut driver = owner();

    let first = executor
        .drive_broker_fetches(
            &driver,
            &mut machine,
            &MonotonicClock::new(),
            Moment::from_tick(1),
        )
        .unwrap_or_else(|error| panic!("submit retained maintenance: {error:?}"));
    assert!(first.1);
    assert_eq!(executor.routed.len(), 1);
    shutdown(&mut driver);
    let recovery = executor.release_fetch_executor_after_driver_shutdown();
    assert!(!recovery.had_fault());
}

fn established_executor(
    effect: kafka_client_core::AssignedConsumerEffect,
    broker: BrokerId,
    call_capacity: usize,
) -> DirectFetchExecutor {
    let mut executor = DirectFetchExecutor::create_unbound(call_capacity, 1, 1_024);
    executor
        .try_enable_sessions(1)
        .unwrap_or_else(|()| panic!("reserve broker sessions"));
    configure_broker_sessions(&mut executor);
    let member = BrokerSessionMember::new(fetch_fence(effect).position(), Arc::from("events"));
    let sessions = executor
        .broker_sessions
        .as_mut()
        .unwrap_or_else(|| panic!("broker session owner"));
    let initial = sessions
        .try_begin(broker, vec![member])
        .unwrap_or_else(|(error, _members)| panic!("begin session: {error:?}"));
    sessions
        .complete(
            initial,
            crate::protocol::fetch::FetchSessionUpdate::Continue(incremental()),
        )
        .unwrap_or_else(|error| panic!("establish session: {error:?}"));
    executor
}

fn incremental() -> FetchSessionRequest {
    FetchSessionRequest::incremental(91, 1)
        .unwrap_or_else(|| panic!("positive incremental session"))
}

fn broker() -> BrokerId {
    BrokerId::new(3).unwrap_or_else(|error| panic!("broker ID: {error}"))
}
