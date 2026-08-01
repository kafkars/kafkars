//! Broker-owned Fetch-session aggregation, forgetting, reset, and close scenarios.

use std::sync::Arc;

use crate::{
    driver::BrokerId,
    protocol::fetch::{FetchSessionRequest, FetchSessionUpdate},
};
use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine, AssignedPartition,
    AssignedTopicPartition, Deadline, Moment, NextFetchOffset, PartitionIndex, StartPosition,
    TopicId,
};

use super::{
    broker_session::{BrokerFetchSessions, BrokerSessionMember},
    broker_session_state::BrokerSessionError,
};

#[test]
fn one_broker_owns_one_epoch_for_every_aggregated_partition() {
    let (effects, _machine) = assignment();
    let broker = broker(3);
    let mut sessions = sessions();
    let plan = sessions
        .try_begin(
            broker,
            vec![member(effects[0], "alpha"), member(effects[1], "beta")],
        )
        .unwrap_or_else(|(error, _active)| panic!("begin session: {error:?}"));
    assert_eq!(plan.session(), FetchSessionRequest::INITIAL);
    assert_eq!(plan.active().len(), 2);
    assert!(plan.forgotten().is_empty());

    sessions
        .complete(plan, FetchSessionUpdate::Continue(incremental(91, 1)))
        .unwrap_or_else(|error| panic!("complete session: {error:?}"));
    assert_eq!(sessions.retained(), (1, 2));
    let next = sessions
        .try_begin(broker, Vec::new())
        .unwrap_or_else(|(error, _active)| panic!("next epoch: {error:?}"));
    assert_eq!(next.session(), incremental(91, 1));
    sessions
        .complete(next, FetchSessionUpdate::Continue(incremental(91, 2)))
        .unwrap_or_else(|error| panic!("advance session: {error:?}"));
}

#[test]
fn control_is_forgotten_only_after_the_exact_incremental_terminal() {
    let (effects, mut machine) = assignment();
    let broker = broker(3);
    let mut sessions = established(&effects, broker);
    let first_fence = fetch_fence(effects[0]);
    let transition = machine
        .apply(AssignedConsumerInput::Pause {
            assignment_epoch: first_fence.position().assignment_epoch(),
            partition: first_fence.position().partition(),
        })
        .unwrap_or_else(|error| panic!("pause: {error}"));
    sessions.observe_control(transition.effects()[0]);

    let plan = sessions
        .try_begin(broker, vec![member(effects[1], "beta")])
        .unwrap_or_else(|(error, _active)| panic!("forget plan: {error:?}"));
    assert_eq!(plan.forgotten().len(), 1);
    assert_eq!(plan.forgotten()[0].topic(), "alpha");
    sessions
        .abort(plan, false)
        .unwrap_or_else(|error| panic!("restore unsent plan: {error:?}"));
    let retry = sessions
        .try_begin(broker, vec![member(effects[1], "beta")])
        .unwrap_or_else(|(error, _active)| panic!("retry forget: {error:?}"));
    assert_eq!(retry.forgotten().len(), 1);
    sessions
        .complete(retry, FetchSessionUpdate::Continue(incremental(91, 2)))
        .unwrap_or_else(|error| panic!("commit forget: {error:?}"));
    assert_eq!(sessions.retained(), (1, 1));
}

#[test]
fn forgotten_only_plan_advances_without_an_active_partition() {
    let (effects, mut machine) = assignment();
    let broker = broker(3);
    let mut sessions = established(&effects, broker);
    let first_fence = fetch_fence(effects[0]);
    let transition = machine
        .apply(AssignedConsumerInput::Pause {
            assignment_epoch: first_fence.position().assignment_epoch(),
            partition: first_fence.position().partition(),
        })
        .unwrap_or_else(|error| panic!("pause: {error}"));
    sessions.observe_control(transition.effects()[0]);

    let plan = sessions
        .try_begin_forgotten()
        .unwrap_or_else(|error| panic!("begin maintenance: {error:?}"))
        .unwrap_or_else(|| panic!("forgotten member must schedule maintenance"));
    assert_eq!(plan.broker_id(), broker);
    assert!(plan.active().is_empty());
    assert_eq!(plan.forgotten(), &[member(effects[0], "alpha")]);
    sessions
        .complete(plan, FetchSessionUpdate::Continue(incremental(91, 2)))
        .unwrap_or_else(|error| panic!("complete maintenance: {error:?}"));
    assert_eq!(sessions.retained(), (1, 1));
    assert!(
        sessions
            .try_begin_forgotten()
            .unwrap_or_else(|error| panic!("check maintenance: {error:?}"))
            .is_none()
    );
}

#[test]
fn rejected_forgotten_only_epoch_clears_cached_members_before_reestablishment() {
    let (effects, mut machine) = assignment();
    let broker = broker(3);
    let mut sessions = established(&effects, broker);
    let first_fence = fetch_fence(effects[0]);
    let transition = machine
        .apply(AssignedConsumerInput::Pause {
            assignment_epoch: first_fence.position().assignment_epoch(),
            partition: first_fence.position().partition(),
        })
        .unwrap_or_else(|error| panic!("pause: {error}"));
    sessions.observe_control(transition.effects()[0]);

    let rejected = sessions
        .try_begin_forgotten()
        .unwrap_or_else(|error| panic!("begin rejected maintenance: {error:?}"))
        .unwrap_or_else(|| panic!("forgotten member must schedule maintenance"));
    assert!(rejected.session().is_incremental());
    assert_eq!(rejected.forgotten(), &[member(effects[0], "alpha")]);
    sessions
        .abort(rejected, true)
        .unwrap_or_else(|error| panic!("reset rejected session: {error:?}"));
    assert_eq!(
        sessions.metadata(broker),
        Some(FetchSessionRequest::INITIAL)
    );
    assert_eq!(sessions.retained(), (1, 0));

    let reestablished = sessions
        .try_begin(broker, vec![member(effects[1], "beta")])
        .unwrap_or_else(|(error, _active)| panic!("re-establish session: {error:?}"));
    assert_eq!(reestablished.session(), FetchSessionRequest::INITIAL);
    assert_eq!(reestablished.active(), &[member(effects[1], "beta")]);
    assert!(reestablished.forgotten().is_empty());
}

#[test]
fn terminal_reset_reestablishes_automatically_and_close_discards_local_state() {
    let (effects, _machine) = assignment();
    let broker = broker(3);
    let mut sessions = established(&effects, broker);
    let plan = sessions
        .try_begin(broker, Vec::new())
        .unwrap_or_else(|(error, _active)| panic!("live session: {error:?}"));
    sessions
        .abort(plan, true)
        .unwrap_or_else(|error| panic!("terminal reset: {error:?}"));
    assert_eq!(
        sessions.metadata(broker),
        Some(FetchSessionRequest::INITIAL)
    );
    assert_eq!(sessions.retained(), (1, 0));

    let initial = sessions
        .try_begin(broker, vec![member(effects[0], "alpha")])
        .unwrap_or_else(|(error, _active)| panic!("re-establish: {error:?}"));
    assert_eq!(initial.session(), FetchSessionRequest::INITIAL);
    sessions
        .complete(initial, FetchSessionUpdate::Continue(incremental(92, 1)))
        .unwrap_or_else(|error| panic!("new session: {error:?}"));
    let close = sessions
        .try_begin_close(broker)
        .unwrap_or_else(|error| panic!("begin close: {error:?}"))
        .unwrap_or_else(|| panic!("established session must close"));
    assert!(close.is_close());
    assert_eq!(
        (
            close.session().session_id(),
            close.session().session_epoch()
        ),
        (92, -1)
    );
    sessions
        .complete_close(close)
        .unwrap_or_else(|error| panic!("complete close: {error:?}"));
    assert_eq!(sessions.retained(), (0, 0));
}

#[test]
fn one_inflight_epoch_per_broker_does_not_block_another_broker() {
    let (effects, _machine) = assignment();
    let mut sessions = sessions();
    let first = sessions
        .try_begin(broker(1), vec![member(effects[0], "alpha")])
        .unwrap_or_else(|(error, _active)| panic!("first broker: {error:?}"));
    let error = sessions
        .try_begin(broker(1), Vec::new())
        .err()
        .map(|(error, _active)| error);
    assert_eq!(error, Some(BrokerSessionError::InFlight));
    assert!(
        sessions
            .try_begin(broker(2), vec![member(effects[1], "beta")])
            .is_ok()
    );
    sessions
        .abort(first, false)
        .unwrap_or_else(|error| panic!("abort first broker: {error:?}"));
}

pub(super) fn established(
    effects: &[AssignedConsumerEffect],
    broker: BrokerId,
) -> BrokerFetchSessions {
    let mut sessions = sessions();
    let plan = sessions
        .try_begin(
            broker,
            vec![member(effects[0], "alpha"), member(effects[1], "beta")],
        )
        .unwrap_or_else(|(error, _active)| panic!("initial plan: {error:?}"));
    sessions
        .complete(plan, FetchSessionUpdate::Continue(incremental(91, 1)))
        .unwrap_or_else(|error| panic!("initial terminal: {error:?}"));
    sessions
}

fn sessions() -> BrokerFetchSessions {
    BrokerFetchSessions::try_new(4, 8)
        .unwrap_or_else(|error| panic!("reserve broker sessions: {error:?}"))
}

pub(super) fn member(effect: AssignedConsumerEffect, topic: &str) -> BrokerSessionMember {
    BrokerSessionMember::new(fetch_fence(effect).position(), Arc::from(topic))
}

pub(super) fn incremental(id: i32, epoch: i32) -> FetchSessionRequest {
    FetchSessionRequest::incremental(id, epoch)
        .unwrap_or_else(|| panic!("positive session metadata"))
}

pub(super) fn broker(value: i32) -> BrokerId {
    BrokerId::new(value).unwrap_or_else(|error| panic!("broker ID: {error}"))
}

pub(super) fn fetch_fence(effect: AssignedConsumerEffect) -> kafka_client_core::FetchFence {
    let AssignedConsumerEffect::FetchReady { fence, .. } = effect else {
        panic!("FetchReady effect");
    };
    fence
}

pub(super) fn assignment() -> (Vec<AssignedConsumerEffect>, AssignedConsumerMachine) {
    let mut machine = AssignedConsumerMachine::new();
    let transition = machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![
                AssignedPartition::new(
                    AssignedTopicPartition::new(TopicId::from_raw(1), PartitionIndex::from_raw(0)),
                    StartPosition::Offset(offset(10)),
                ),
                AssignedPartition::new(
                    AssignedTopicPartition::new(TopicId::from_raw(2), PartitionIndex::from_raw(1)),
                    StartPosition::Offset(offset(20)),
                ),
            ],
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(1_000),
        })
        .unwrap_or_else(|error| panic!("assignment: {error}"));
    (transition.into_effects(), machine)
}

fn offset(value: i64) -> NextFetchOffset {
    NextFetchOffset::try_from_raw(value).unwrap_or_else(|| panic!("nonnegative offset"))
}
