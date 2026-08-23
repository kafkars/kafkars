//! Session-fenced acknowledgement admission and delivery-certainty evidence.

use crate::{
    AssignedTopicPartition, ByteCount, Deadline, DeliveryStatus, Moment, PartitionIndex, TopicId,
};

use super::{
    ShareAcknowledgeAttempt, ShareAcknowledgement, ShareAcknowledgementApplyErrorKind,
    ShareAcknowledgementFailureSettlement, ShareAcquisitionPolicy, ShareDisposition,
    ShareFetchSessionMachine, ShareFetchSessionPhase, ShareRecordDecision,
    test_support::{fence, ledger, okay, range, rejected},
};

#[test]
fn success_advances_session_and_retires_exact_acquisitions() {
    let (mut machine, acknowledgement) = acknowledging_batch();
    let admission = okay(machine.prepare_acknowledgement(
        acknowledgement,
        Deadline::from_tick(30),
        Moment::from_tick(3),
    ));
    let (attempt, acknowledgement) = admission.into_parts();

    assert_eq!(machine.phase(), ShareFetchSessionPhase::Acknowledging);
    assert_eq!(machine.acknowledging(), Some(attempt));
    assert_eq!(machine.ledger().next_reclaimable_deadline(), None);
    assert_eq!(
        machine.prepare_fetch(Deadline::from_tick(30), Moment::from_tick(3)),
        Err(super::ShareFetchSessionApplyError::new(
            super::ShareFetchSessionErrorKind::InvalidState
        ))
    );

    let releases = okay(machine.settle_acknowledged(attempt, acknowledgement));
    assert_eq!(releases.len(), 1);
    assert_eq!(releases[0].retained_bytes(), ByteCount::new(8));
    assert_eq!(machine.phase(), ShareFetchSessionPhase::Ready);
    assert_eq!(machine.fence().session_epoch().get(), 2);
    assert!(machine.ledger().is_empty());
}

#[test]
fn definitely_unsent_failure_restores_the_same_retry_capability() {
    let (mut machine, acknowledgement) = acknowledging_batch();
    let admission = okay(machine.prepare_acknowledgement(
        acknowledgement,
        Deadline::from_tick(30),
        Moment::from_tick(3),
    ));
    let (attempt, acknowledgement) = admission.into_parts();
    let settlement = okay(machine.settle_acknowledgement_failure(
        attempt,
        DeliveryStatus::NotSent,
        acknowledgement,
    ));
    let ShareAcknowledgementFailureSettlement::Retry(acknowledgement) = settlement else {
        panic!("definitely-unsent acknowledgement must return retry ownership");
    };

    assert_eq!(machine.phase(), ShareFetchSessionPhase::Ready);
    assert_eq!(machine.fence().session_epoch().get(), 1);
    assert_eq!(machine.ledger().retained_bytes(), ByteCount::new(8));
    let retry = okay(machine.prepare_acknowledgement(
        acknowledgement,
        Deadline::from_tick(40),
        Moment::from_tick(4),
    ));
    let (attempt, acknowledgement) = retry.into_parts();
    assert_eq!(
        okay(machine.settle_acknowledged(attempt, acknowledgement)).len(),
        1
    );
}

#[test]
fn possibly_sent_failure_consumes_capability_and_loses_session() {
    let (mut machine, acknowledgement) = acknowledging_batch();
    let admission = okay(machine.prepare_acknowledgement(
        acknowledgement,
        Deadline::from_tick(30),
        Moment::from_tick(3),
    ));
    let (attempt, acknowledgement) = admission.into_parts();
    let settlement = okay(machine.settle_acknowledgement_failure(
        attempt,
        DeliveryStatus::PossiblySent,
        acknowledgement,
    ));
    let ShareAcknowledgementFailureSettlement::Lost(releases) = settlement else {
        panic!("possibly-sent acknowledgement must not recreate retry ownership");
    };

    assert_eq!(releases.len(), 1);
    assert_eq!(machine.phase(), ShareFetchSessionPhase::Lost);
    assert!(machine.ledger().is_empty());
}

#[test]
fn stale_settlement_and_pre_admission_rejections_preserve_exact_owner() {
    let (mut machine, acknowledgement) = acknowledging_batch();
    let error = rejected(machine.prepare_acknowledgement(
        acknowledgement,
        Deadline::from_tick(3),
        Moment::from_tick(3),
    ));
    assert_eq!(
        error.kind(),
        ShareAcknowledgementApplyErrorKind::DeadlineElapsed
    );
    let acknowledgement = error.into_acknowledgement();
    let admission = okay(machine.prepare_acknowledgement(
        acknowledgement,
        Deadline::from_tick(30),
        Moment::from_tick(3),
    ));
    let (attempt, acknowledgement) = admission.into_parts();
    let stale = ShareAcknowledgeAttempt::new(
        attempt.fence(),
        attempt.acquisition_fence(),
        Deadline::from_tick(31),
    );
    let error = rejected(machine.settle_acknowledged(stale, acknowledgement));
    assert_eq!(
        error.kind(),
        ShareAcknowledgementApplyErrorKind::StaleAttempt
    );
    assert_eq!(machine.acknowledging(), Some(attempt));
    assert_eq!(
        okay(machine.settle_acknowledged(attempt, error.into_acknowledgement())).len(),
        1
    );
}

#[test]
fn foreign_and_expired_acquisitions_do_not_partially_mutate_session() {
    let (mut machine, _acknowledgement) = acknowledging_batch();
    let foreign = foreign_acknowledgement();
    let error = rejected(machine.prepare_acknowledgement(
        foreign,
        Deadline::from_tick(30),
        Moment::from_tick(3),
    ));
    assert_eq!(
        error.kind(),
        ShareAcknowledgementApplyErrorKind::SessionMismatch
    );
    assert_eq!(machine.phase(), ShareFetchSessionPhase::Ready);

    let mut expired = separate_machine();
    let acknowledgement = acknowledgement_for(&mut expired, 4, 8, 10);
    let error = rejected(expired.prepare_acknowledgement(
        acknowledgement,
        Deadline::from_tick(30),
        Moment::from_tick(10),
    ));
    assert_eq!(
        error.kind(),
        ShareAcknowledgementApplyErrorKind::Acquisition(
            super::ShareAcquisitionAdmissionErrorKind::ExpiredLock
        )
    );
    assert_eq!(expired.phase(), ShareFetchSessionPhase::Ready);
    assert_eq!(expired.ledger().retained_bytes(), ByteCount::new(8));
}

fn acknowledging_batch() -> (ShareFetchSessionMachine, ShareAcknowledgement) {
    let mut machine = separate_machine();
    let acknowledgement = acknowledgement_for(&mut machine, 0, 8, 50);
    (machine, acknowledgement)
}

fn separate_machine() -> ShareFetchSessionMachine {
    let policy = okay(ShareAcquisitionPolicy::try_new(2, 8, ByteCount::new(16)));
    okay(ShareFetchSessionMachine::try_open(
        fence(0),
        vec![partition()],
        policy,
    ))
}

fn acknowledgement_for(
    machine: &mut ShareFetchSessionMachine,
    first_offset: i64,
    bytes: u64,
    lock: u64,
) -> ShareAcknowledgement {
    let attempt = okay(machine.prepare_fetch(Deadline::from_tick(20), Moment::from_tick(1)));
    okay(machine.settle_acquired(
        attempt,
        Moment::from_tick(2),
        vec![range(1, 1, first_offset, first_offset, 1, bytes, lock)],
    ));
    let acquisitions = okay(machine.ledger_mut().claim_batch(
        attempt.fence(),
        1,
        Moment::from_tick(2),
    ));
    let generation = acquisitions[0].generation();
    okay(ShareAcknowledgement::try_new(
        acquisitions,
        vec![ShareRecordDecision::new(
            generation,
            first_offset,
            ShareDisposition::Accept,
        )],
    ))
}

fn foreign_acknowledgement() -> ShareAcknowledgement {
    let mut ledger = ledger(1, 1, 8);
    okay(ledger.try_admit(
        fence(2),
        Moment::from_tick(1),
        vec![range(1, 1, 2, 2, 1, 6, 40)],
    ));
    let acquisitions = okay(ledger.claim_batch(fence(2), 1, Moment::from_tick(2)));
    let generation = acquisitions[0].generation();
    okay(ShareAcknowledgement::try_new(
        acquisitions,
        vec![ShareRecordDecision::new(
            generation,
            2,
            ShareDisposition::Accept,
        )],
    ))
}

fn partition() -> AssignedTopicPartition {
    AssignedTopicPartition::new(TopicId::from_raw(1), PartitionIndex::from_raw(0))
}
