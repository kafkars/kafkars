//! Automatic recovery from broker-invalidated incremental Fetch sessions.

use std::sync::Arc;

use crate::{
    driver::BrokerId,
    protocol::fetch::{FetchSessionRequest, FetchSessionUpdate},
};
use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, Deadline, FetchOwnership, Moment, StartPosition,
};

use super::{
    broker_execution::ActiveBrokerSession,
    broker_session::BrokerSessionMember,
    broker_session_test::{assignment as aggregate_assignment, incremental, member},
    executor::DirectFetchExecutor,
    fault::{FetchExecutionError, RetainedFetchFault},
    settlement::FetchTerminalPoll,
    settlement_test::{OUTPUT_BYTES, assignment, fetch_fence, prepared},
};

#[test]
fn conflicting_partition_updates_leave_the_active_session_owned() {
    let (effect, mut machine) = assignment();
    let fence = fetch_fence(effect);
    let broker = BrokerId::new(3).unwrap_or_else(|error| panic!("broker ID: {error}"));
    let member = BrokerSessionMember::new(fence.position(), Arc::from("events"));
    let retained =
        FetchSessionRequest::incremental(91, 1).unwrap_or_else(|| panic!("valid retained update"));
    let mut executor = DirectFetchExecutor::create_unbound(1, 1, OUTPUT_BYTES);
    executor
        .try_enable_sessions(1)
        .unwrap_or_else(|()| panic!("reserve session capacity"));
    let plan = executor
        .broker_sessions
        .as_mut()
        .unwrap_or_else(|| panic!("broker sessions"))
        .try_begin(broker, vec![member])
        .unwrap_or_else(|(error, _active)| panic!("initial plan: {error:?}"));
    let mut prepared = prepared(effect);
    prepared.request.bind_session(plan.session());
    let (request, hard_output_bytes) = prepared.into_parts_for_test();
    executor
        .reserve_output_for_test(fence, hard_output_bytes)
        .unwrap_or_else(|error| panic!("reserve output: {error:?}"));
    executor
        .broker_calls
        .install_topic_partition_results_for_test(
            vec![request],
            Moment::from_tick(7),
            12,
            92,
            &[0],
        );
    executor.active_broker_sessions.push(ActiveBrokerSession {
        fences: vec![fence],
        plan,
        update: Some(FetchSessionUpdate::Continue(retained)),
        reset: false,
    });

    assert_eq!(
        executor.poll(&mut machine, Moment::from_tick(8)),
        Err(FetchExecutionError::BrokerSession)
    );
    let Some(RetainedFetchFault::Transition {
        _request: request,
        _transition: transition,
    }) = executor.fault.as_ref()
    else {
        panic!("post-core session failure must retain the request and transition");
    };
    assert_eq!(request.fence(), fence);
    assert!(matches!(
        transition.effects(),
        [kafka_client_core::AssignedConsumerEffect::FetchReady { .. }]
    ));
    let active = &executor.active_broker_sessions[0];
    assert_eq!(active.fences, [fence]);
    assert_eq!(active.update, Some(FetchSessionUpdate::Continue(retained)));
    assert!(!active.reset);
    assert_eq!(
        executor
            .broker_sessions
            .as_ref()
            .and_then(|sessions| sessions.metadata(broker)),
        Some(FetchSessionRequest::INITIAL)
    );
}

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

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "the scenario keeps both partitions and the complete session reset sequence visible"
)]
fn partition_offset_out_of_range_resets_the_complete_aggregated_session() {
    let (effects, mut machine) = aggregate_assignment();
    let reset_fence = fetch_fence(effects[0]);
    let surviving_fence = fetch_fence(effects[1]);
    let broker = BrokerId::new(3).unwrap_or_else(|error| panic!("broker ID: {error}"));
    let members = vec![member(effects[0], "events"), member(effects[1], "events")];
    let retained = incremental(91, 1);
    let mut executor = DirectFetchExecutor::create_unbound(1, 2, OUTPUT_BYTES * 2);
    executor
        .try_enable_sessions(1)
        .unwrap_or_else(|()| panic!("reserve session capacity"));

    let sessions = executor
        .broker_sessions
        .as_mut()
        .unwrap_or_else(|| panic!("broker sessions"));
    let initial = sessions
        .try_begin(broker, members.clone())
        .unwrap_or_else(|(error, _active)| panic!("initial plan: {error:?}"));
    sessions
        .complete(initial, FetchSessionUpdate::Continue(retained))
        .unwrap_or_else(|error| panic!("establish session: {error:?}"));
    let plan = sessions
        .try_begin(broker, members)
        .unwrap_or_else(|(error, _active)| panic!("incremental plan: {error:?}"));
    assert_eq!(plan.session(), retained);

    let mut requests = Vec::new();
    for effect in effects.iter().copied() {
        let fence = fetch_fence(effect);
        let mut prepared = prepared(effect);
        prepared.request.bind_session(plan.session());
        let (request, hard_output_bytes) = prepared.into_parts_for_test();
        executor
            .reserve_output_for_test(fence, hard_output_bytes)
            .unwrap_or_else(|error| panic!("reserve output: {error:?}"));
        requests.push(request);
    }
    executor
        .broker_calls
        .install_topic_partition_results_for_test(requests, Moment::from_tick(7), 12, 91, &[1, 0]);
    executor.active_broker_sessions.push(ActiveBrokerSession {
        fences: vec![reset_fence, surviving_fence],
        plan,
        update: None,
        reset: false,
    });

    let proposal = match executor
        .poll_proposal(Moment::from_tick(8))
        .unwrap_or_else(|error| panic!("poll offset-out-of-range terminal: {error:?}"))
    {
        FetchTerminalPoll::Proposed(proposal) => proposal
            .into_partition_offset_out_of_range()
            .unwrap_or_else(|_proposal| panic!("partition OFFSET_OUT_OF_RANGE proposal")),
        FetchTerminalPoll::Idle | FetchTerminalPoll::Progressed => {
            panic!("partition terminal proposal")
        }
    };
    let reset = AssignedConsumerInput::Seek {
        assignment_epoch: reset_fence.position().assignment_epoch(),
        partition: reset_fence.position().partition(),
        position: StartPosition::Beginning,
        now: Moment::from_tick(8),
        resolution_deadline: Deadline::from_tick(1_000),
    };
    let transition = executor
        .apply_offset_out_of_range_reset(&mut machine, proposal, reset)
        .unwrap_or_else(|error| panic!("apply offset reset: {error:?}"))
        .unwrap_or_else(|| panic!("offset reset transition"));
    let [AssignedConsumerEffect::Suspend { fence: suspended }, ..] = transition.effects() else {
        panic!(
            "offset reset starts with Suspend: {:?}",
            transition.effects()
        );
    };
    assert_eq!(
        suspended.assignment_epoch(),
        reset_fence.position().assignment_epoch()
    );
    assert_eq!(suspended.partition(), reset_fence.position().partition());
    assert_ne!(*suspended, reset_fence.position());
    executor
        .observe_control(transition.effects()[0])
        .unwrap_or_else(|error| panic!("observe reset suspension: {error:?}"));
    assert!(executor.active_broker_sessions[0].reset);

    executor
        .poll(&mut machine, Moment::from_tick(9))
        .unwrap_or_else(|error| panic!("settle surviving partition: {error:?}"))
        .unwrap_or_else(|| panic!("surviving partition transition"));
    assert!(executor.active_broker_sessions.is_empty());
    let sessions = executor
        .broker_sessions
        .as_mut()
        .unwrap_or_else(|| panic!("broker sessions"));
    assert_eq!(
        sessions.metadata(broker),
        Some(FetchSessionRequest::INITIAL)
    );
    assert_eq!(sessions.retained(), (1, 0));

    let next = sessions
        .try_begin(
            broker,
            vec![BrokerSessionMember::new(
                surviving_fence.position(),
                Arc::from("events"),
            )],
        )
        .unwrap_or_else(|(error, _active)| panic!("next plan: {error:?}"));
    assert_eq!(next.session(), FetchSessionRequest::INITIAL);
    assert_eq!(next.active().len(), 1);
    assert_eq!(next.active()[0].position(), surviving_fence.position());
    assert!(next.forgotten().is_empty());
}
