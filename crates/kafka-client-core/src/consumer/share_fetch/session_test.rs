//! Executable broker-session, assignment, and settlement evidence for `ShareFetch`.

use core::fmt::Debug;

use crate::{
    AssignedTopicPartition, ByteCount, Deadline, DeliveryStatus, GroupId, MemberId, Moment,
    PartitionIndex, ShareGroupMemberEpoch, TopicId,
};

use super::{
    ShareAcquiredOffsets, ShareAcquiredRange, ShareAcquisitionAdmissionErrorKind,
    ShareAcquisitionPolicy, ShareDeliveryCount, ShareFetchBrokerId, ShareFetchSessionEpoch,
    ShareFetchSessionFence, ShareFetchSessionMachine, ShareFetchSessionPhase,
    ShareFetchSettlementErrorKind, ShareTopicUuid,
};

#[test]
fn exact_attempt_advances_session_and_stages_acquisitions() {
    let mut machine = machine(vec![partition(1, 0)]);
    let attempt = okay(machine.prepare_fetch(Deadline::from_tick(30), Moment::from_tick(10)));
    assert_eq!(machine.phase(), ShareFetchSessionPhase::InFlight);

    let acquisitions = okay(machine.settle_acquired(
        attempt,
        Moment::from_tick(12),
        vec![range(1, 1, 0, 4, 8, 50)],
    ));
    assert_eq!(acquisitions, 1);
    let acquisition = some(machine.ledger_mut().claim_next(Moment::from_tick(12)));
    assert_eq!(acquisition.fence(), attempt.fence());
    assert_eq!(machine.phase(), ShareFetchSessionPhase::Ready);
    assert_eq!(machine.fence().session_epoch().get(), 1);
    assert_eq!(machine.ledger().retained_records(), 5);
    assert_eq!(machine.ledger().retained_bytes(), ByteCount::new(8));
}

#[test]
fn assignment_change_discards_old_response_without_destroying_ledger() {
    let mut machine = machine(vec![partition(1, 0)]);
    let attempt = okay(machine.prepare_fetch(Deadline::from_tick(30), Moment::from_tick(10)));
    okay(machine.replace_assignment(vec![partition(1, 1)]));
    let ranges = vec![range(1, 1, 0, 0, 5, 50)];
    let error = rejected(machine.settle_acquired(attempt, Moment::from_tick(11), ranges));
    assert_eq!(
        error.kind(),
        ShareFetchSettlementErrorKind::AssignmentChanged
    );
    assert_eq!(error.into_ranges().len(), 1);
    assert_eq!(machine.phase(), ShareFetchSessionPhase::Ready);
    assert_eq!(machine.fence().session_epoch().get(), 1);
    assert!(machine.ledger().is_empty());
    assert_eq!(machine.assignment(), &[partition(1, 1)]);
}

#[test]
fn unassigned_expired_and_overlapping_facts_fail_closed() {
    let mut unassigned = machine(vec![partition(1, 0)]);
    let attempt = okay(unassigned.prepare_fetch(Deadline::from_tick(30), Moment::from_tick(10)));
    let error = rejected(unassigned.settle_acquired(
        attempt,
        Moment::from_tick(11),
        vec![range(1, 2, 0, 0, 5, 50)],
    ));
    assert_eq!(
        error.kind(),
        ShareFetchSettlementErrorKind::UnassignedPartition
    );
    assert_eq!(unassigned.phase(), ShareFetchSessionPhase::Lost);

    let mut expired = machine(vec![partition(1, 0)]);
    let attempt = okay(expired.prepare_fetch(Deadline::from_tick(20), Moment::from_tick(10)));
    let error = rejected(expired.settle_acquired(
        attempt,
        Moment::from_tick(20),
        vec![range(1, 1, 0, 0, 5, 50)],
    ));
    assert_eq!(error.kind(), ShareFetchSettlementErrorKind::DeadlineElapsed);
    assert_eq!(expired.phase(), ShareFetchSessionPhase::Lost);

    let mut overlapping = machine(vec![partition(1, 0)]);
    let first = okay(overlapping.prepare_fetch(Deadline::from_tick(30), Moment::from_tick(10)));
    okay(overlapping.settle_acquired(first, Moment::from_tick(11), vec![range(1, 1, 0, 2, 5, 50)]));
    let second = okay(overlapping.prepare_fetch(Deadline::from_tick(40), Moment::from_tick(12)));
    let error = rejected(overlapping.settle_acquired(
        second,
        Moment::from_tick(13),
        vec![range(1, 1, 2, 3, 5, 50)],
    ));
    assert_eq!(
        error.kind(),
        ShareFetchSettlementErrorKind::Acquisition(
            ShareAcquisitionAdmissionErrorKind::OverlappingRange
        )
    );
    assert_eq!(overlapping.phase(), ShareFetchSessionPhase::Lost);
    assert_eq!(overlapping.ledger().len(), 1);
}

#[test]
fn stale_attempt_cannot_mutate_the_current_owner() {
    let mut machine = machine(vec![partition(1, 0)]);
    let live = okay(machine.prepare_fetch(Deadline::from_tick(30), Moment::from_tick(10)));
    let stale = super::ShareFetchAttempt::new(
        live.fence(),
        some(super::ShareFetchAssignmentGeneration::try_from_raw(2)),
        live.deadline(),
    );
    let error = rejected(machine.settle_acquired(
        stale,
        Moment::from_tick(11),
        vec![range(1, 1, 0, 0, 5, 50)],
    ));
    assert_eq!(error.kind(), ShareFetchSettlementErrorKind::StaleAttempt);
    assert_eq!(machine.in_flight(), Some(live));
    assert_eq!(machine.phase(), ShareFetchSessionPhase::InFlight);
    assert!(machine.ledger().is_empty());
}

#[test]
fn definitely_unsent_failure_reopens_the_same_session_without_advancing_epoch() {
    let mut machine = machine(vec![partition(1, 0)]);
    let attempt = okay(machine.prepare_fetch(Deadline::from_tick(30), Moment::from_tick(10)));
    okay(machine.settle_failure(attempt, DeliveryStatus::NotSent));

    assert_eq!(machine.phase(), ShareFetchSessionPhase::Ready);
    assert_eq!(machine.in_flight(), None);
    assert_eq!(machine.fence(), attempt.fence());
    let replacement = okay(machine.prepare_fetch(Deadline::from_tick(40), Moment::from_tick(11)));
    assert_eq!(replacement.fence(), attempt.fence());
}

#[test]
fn possibly_sent_failure_loses_the_session_and_stale_facts_cannot_reopen_it() {
    let mut machine = machine(vec![partition(1, 0)]);
    let attempt = okay(machine.prepare_fetch(Deadline::from_tick(30), Moment::from_tick(10)));
    okay(machine.settle_failure(attempt, DeliveryStatus::PossiblySent));

    assert_eq!(machine.phase(), ShareFetchSessionPhase::Lost);
    assert_eq!(machine.in_flight(), None);
    let error = rejected(machine.settle_failure(attempt, DeliveryStatus::NotSent));
    assert_eq!(
        error.kind(),
        super::ShareFetchSessionErrorKind::InvalidState
    );
    assert_eq!(machine.phase(), ShareFetchSessionPhase::Lost);
}

fn machine(assignment: Vec<AssignedTopicPartition>) -> ShareFetchSessionMachine {
    let policy = okay(ShareAcquisitionPolicy::try_new(4, 16, ByteCount::new(32)));
    okay(ShareFetchSessionMachine::try_open(
        fence(),
        assignment,
        policy,
    ))
}

fn partition(topic: u64, partition: u32) -> AssignedTopicPartition {
    AssignedTopicPartition::new(
        TopicId::from_raw(topic),
        PartitionIndex::from_raw(partition),
    )
}

fn range(uuid: u8, topic: u64, first: i64, last: i64, bytes: u64, lock: u64) -> ShareAcquiredRange {
    let mut topic_uuid = [0; 16];
    topic_uuid[0] = uuid;
    okay(ShareAcquiredRange::try_new(
        some(ShareTopicUuid::try_from_bytes(topic_uuid)),
        partition(topic, 0),
        okay(ShareAcquiredOffsets::try_new(first, last)),
        some(ShareDeliveryCount::try_from_raw(1)),
        ByteCount::new(bytes),
        Deadline::from_tick(lock),
        Moment::from_tick(0),
    ))
}

fn fence() -> ShareFetchSessionFence {
    ShareFetchSessionFence::new(
        some(ShareFetchBrokerId::try_from_raw(1)),
        some(GroupId::try_from_raw(1)),
        some(MemberId::try_from_raw(1)),
        some(ShareGroupMemberEpoch::try_from_raw(1)),
        ShareFetchSessionEpoch::initial(),
    )
}

fn okay<T, E: Debug>(result: Result<T, E>) -> T {
    match result {
        Ok(value) => value,
        Err(error) => panic!("unexpected error: {error:?}"),
    }
}

fn rejected<T, E: Debug>(result: Result<T, E>) -> E {
    match result {
        Ok(_value) => panic!("expected rejection"),
        Err(error) => error,
    }
}

fn some<T>(value: Option<T>) -> T {
    match value {
        Some(value) => value,
        None => panic!("expected validated value"),
    }
}
