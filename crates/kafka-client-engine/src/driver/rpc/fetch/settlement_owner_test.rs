//! Exact raw-terminal handoff, stale confirmation, and restoration scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine, AssignedPartition,
    AssignedTopicPartition, Deadline, FetchFence, Moment, NextFetchOffset, PartitionIndex,
    StartPosition, TopicId,
};
use kafka_driver::RequestError;

use crate::{
    clock::OperationDeadline,
    protocol::fetch::{FetchDecodeLimits, FetchRequestSettings},
};

use super::{
    admission::PartitionFetchRequest,
    calls::TrackedFetchCalls,
    settlement::{
        FetchBeginSettlementError, FetchConfirmationError, FetchPoll, StaleFetchConfirmationError,
    },
    terminal::{FetchTerminal, retain_fetch_terminal},
};

#[test]
fn raw_settlement_restores_or_confirms_only_the_exact_fence() {
    let (fences, mut machine) = assignment(&[3, 4]);
    let future = replacement_fence(&mut machine, 3, 62);
    let mut calls = TrackedFetchCalls::new(1);
    calls.install_terminal_for_test(terminal(fences[0], 42));
    assert_eq!(
        calls.poll_fetch(Moment::from_tick(6)),
        Ok(FetchPoll::TerminalReady { fence: fences[0] })
    );

    let raw = calls
        .begin_fetch_settlement(fences[0])
        .unwrap_or_else(|error| panic!("begin exact settlement: {error:?}"));
    for supplied in [fences[1], future] {
        assert!(matches!(
            calls.confirm_fetch_settlement(supplied),
            Err(FetchConfirmationError::FenceMismatch { pending, supplied: observed })
                if pending == fences[0] && observed == supplied
        ));
    }
    calls
        .restore_fetch_settlement(raw)
        .unwrap_or_else(|failure| panic!("restore: {:?}", failure.into_parts().1));
    let raw = calls
        .begin_fetch_settlement(fences[0])
        .unwrap_or_else(|error| panic!("begin restored settlement: {error:?}"));
    assert_eq!(raw.fence(), fences[0]);
    calls
        .confirm_fetch_settlement(fences[0])
        .unwrap_or_else(|error| panic!("confirm exact route token: {error:?}"));
    assert_eq!(calls.retained_count(), 0);
}

#[test]
fn stale_control_returns_request_and_requires_exact_stale_confirmation() {
    let (old, mut machine) = assignment(&[3, 4]);
    let seek = seek(&mut machine, old[0], 52);
    let mut calls = TrackedFetchCalls::new(1);
    calls.install_terminal_for_test(terminal(old[0], 42));
    let drains = calls
        .observe_fetch_control(seek.effects()[0])
        .unwrap_or_else(|pending| panic!("no settlement pending: {:?}", pending.fence));
    let returned = drains.into_requests();
    assert_eq!(returned.len(), 1);
    assert_eq!(returned[0].fence(), old[0]);
    assert_eq!(
        calls.poll_fetch(Moment::from_tick(6)),
        Ok(FetchPoll::StaleConfirmationReady { fence: old[0] })
    );
    assert!(matches!(
        calls.begin_fetch_settlement(old[0]),
        Err(FetchBeginSettlementError::StaleSettledCall { supplied })
            if supplied == old[0]
    ));
    assert!(matches!(
        calls.confirm_stale_fetch(old[1]),
        Err(StaleFetchConfirmationError::FenceMismatch { settled, supplied })
            if settled == old[0] && supplied == old[1]
    ));
    calls
        .confirm_stale_fetch(old[0])
        .unwrap_or_else(|error| panic!("confirm exact stale route token: {error:?}"));
    assert_eq!(calls.retained_count(), 0);
}

#[test]
fn unrelated_control_cannot_take_live_terminal_ownership() {
    let (old, _) = assignment(&[3, 4]);
    let mut calls = TrackedFetchCalls::new(1);
    calls.install_terminal_for_test(terminal(old[0], 42));
    let drains = calls
        .observe_fetch_control(AssignedConsumerEffect::Suspend {
            fence: old[1].position(),
        })
        .unwrap_or_else(|pending| panic!("no settlement pending: {:?}", pending.fence));
    assert!(drains.into_requests().is_empty());
    assert_eq!(
        calls.poll_fetch(Moment::from_tick(6)),
        Ok(FetchPoll::TerminalReady { fence: old[0] })
    );
}

pub(super) fn terminal(fence: FetchFence, raw_offset: i64) -> FetchTerminal {
    retain_fetch_terminal(
        request(fetch_effect(fence, raw_offset), "events"),
        Moment::from_tick(5),
        None,
        Err(RequestError::RouteUnavailable),
    )
}

fn assignment(partitions: &[u32]) -> (Vec<FetchFence>, AssignedConsumerMachine) {
    let mut machine = AssignedConsumerMachine::new();
    let partitions = partitions
        .iter()
        .map(|partition| {
            AssignedPartition::new(
                topic_partition(*partition),
                StartPosition::Offset(offset(42)),
            )
        })
        .collect();
    let transition = machine
        .apply(AssignedConsumerInput::Assign {
            partitions,
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("direct assignment: {error}"));
    let fences = transition
        .effects()
        .iter()
        .filter_map(|effect| match effect {
            AssignedConsumerEffect::FetchReady { fence, .. } => Some(*fence),
            _ => None,
        })
        .collect();
    (fences, machine)
}

fn replacement_fence(
    machine: &mut AssignedConsumerMachine,
    partition: u32,
    raw_offset: i64,
) -> FetchFence {
    let replacement = machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![AssignedPartition::new(
                topic_partition(partition),
                StartPosition::Offset(offset(raw_offset)),
            )],
            now: Moment::from_tick(2),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("replacement assignment: {error}"));
    replacement
        .effects()
        .iter()
        .find_map(fetch_fence)
        .unwrap_or_else(|| panic!("FetchReady effect"))
}

fn seek(
    machine: &mut AssignedConsumerMachine,
    old: FetchFence,
    raw_offset: i64,
) -> kafka_client_core::AssignedConsumerTransition {
    machine
        .apply(AssignedConsumerInput::Seek {
            assignment_epoch: old.position().assignment_epoch(),
            partition: old.position().partition(),
            position: StartPosition::Offset(offset(raw_offset)),
            now: Moment::from_tick(1),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("seek: {error}"))
}

fn fetch_fence(effect: &AssignedConsumerEffect) -> Option<FetchFence> {
    match effect {
        AssignedConsumerEffect::FetchReady { fence, .. } => Some(*fence),
        _ => None,
    }
}

fn fetch_effect(fence: FetchFence, raw_offset: i64) -> AssignedConsumerEffect {
    AssignedConsumerEffect::FetchReady {
        fence,
        next_offset: offset(raw_offset),
    }
}

fn request(effect: AssignedConsumerEffect, topic: &str) -> PartitionFetchRequest {
    PartitionFetchRequest::from_effect(
        effect,
        topic.to_owned(),
        FetchRequestSettings::new(500, 1, 1024 * 1024, 1024 * 1024, 0),
        FetchDecodeLimits::default(),
        OperationDeadline::from_parts_for_test(
            Deadline::from_tick(1_000_000_000),
            Instant::now() + Duration::from_secs(1),
        ),
    )
    .unwrap_or_else(|error| panic!("prepare Fetch: {error:?}"))
}

fn topic_partition(partition: u32) -> AssignedTopicPartition {
    AssignedTopicPartition::new(TopicId::from_raw(1), PartitionIndex::from_raw(partition))
}

fn offset(raw: i64) -> NextFetchOffset {
    NextFetchOffset::try_from_raw(raw).unwrap_or_else(|| panic!("valid offset"))
}
