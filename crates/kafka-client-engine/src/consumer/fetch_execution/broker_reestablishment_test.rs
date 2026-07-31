//! Automatic recovery from broker-invalidated incremental Fetch sessions.

use std::sync::Arc;

use kafka_client_core::{FetchOwnership, Moment};

use crate::{
    driver::BrokerId,
    protocol::fetch::{FetchSessionRequest, FetchSessionUpdate},
};

use super::{
    broker_execution::ActiveBrokerSession,
    broker_session::BrokerSessionMember,
    executor::DirectFetchExecutor,
    settlement_test::{OUTPUT_BYTES, assignment, fetch_fence, prepared},
};

#[test]
fn invalid_incremental_session_is_reset_and_requeued_without_failing_core() {
    let (effect, mut machine) = assignment();
    let fence = fetch_fence(effect);
    let broker = BrokerId::new(3).unwrap_or_else(|error| panic!("broker ID: {error}"));
    let member = BrokerSessionMember::new(fence.position(), Arc::from("events"));
    let incremental = FetchSessionRequest::incremental(91, 1)
        .unwrap_or_else(|| panic!("valid incremental session"));
    let mut executor = DirectFetchExecutor::create_unbound(1, 1, OUTPUT_BYTES);
    executor
        .try_enable_sessions(1)
        .unwrap_or_else(|()| panic!("reserve session capacity"));

    let initial = executor
        .broker_sessions
        .as_mut()
        .unwrap_or_else(|| panic!("broker sessions"))
        .try_begin(broker, vec![member.clone()])
        .unwrap_or_else(|(error, _active)| panic!("initial plan: {error:?}"));
    executor
        .broker_sessions
        .as_mut()
        .unwrap_or_else(|| panic!("broker sessions"))
        .complete(initial, FetchSessionUpdate::Continue(incremental))
        .unwrap_or_else(|error| panic!("establish session: {error:?}"));
    let plan = executor
        .broker_sessions
        .as_mut()
        .unwrap_or_else(|| panic!("broker sessions"))
        .try_begin(broker, vec![member.clone()])
        .unwrap_or_else(|(error, _active)| panic!("incremental plan: {error:?}"));
    assert_eq!(plan.session(), incremental);

    let mut prepared = prepared(effect);
    prepared.request.bind_session(plan.session());
    let (request, hard_output_bytes) = prepared.into_parts_for_test();
    let original_deadline = request.operation_deadline();
    executor
        .reserve_output_for_test(fence, hard_output_bytes)
        .unwrap_or_else(|error| panic!("reserve output: {error:?}"));
    executor.broker_calls.install_broker_error_for_test(
        vec![request],
        Moment::from_tick(7),
        12,
        70,
    );
    executor.active_broker_sessions.push(ActiveBrokerSession {
        fences: vec![fence],
        plan,
        update: None,
        reset: false,
    });

    assert_eq!(
        executor
            .poll(&mut machine, Moment::from_tick(8))
            .unwrap_or_else(|error| panic!("settle invalid session: {error:?}")),
        None
    );
    assert_eq!(machine.fetch_ownership(fence), Ok(FetchOwnership::Active));
    assert_eq!(executor.retained(), (1, 0, 0));
    assert!(executor.active_broker_sessions.is_empty());
    assert_eq!(
        executor
            .broker_sessions
            .as_ref()
            .and_then(|sessions| sessions.metadata(broker)),
        Some(FetchSessionRequest::INITIAL)
    );

    let retry = executor
        .routed
        .pop()
        .unwrap_or_else(|| panic!("requeued exact Fetch"));
    assert_eq!(retry.broker_id, broker);
    assert_eq!(retry.request.fence(), fence);
    assert_eq!(retry.hard_output_bytes, OUTPUT_BYTES);
    assert_eq!(retry.request.operation_deadline(), original_deadline);
    let reestablished = executor
        .broker_sessions
        .as_mut()
        .unwrap_or_else(|| panic!("broker sessions"))
        .try_begin(broker, vec![member])
        .unwrap_or_else(|(error, _active)| panic!("re-establish plan: {error:?}"));
    assert_eq!(reestablished.session(), FetchSessionRequest::INITIAL);
}
