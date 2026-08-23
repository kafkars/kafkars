//! Core admission, generated request, and lossless preparation rollback evidence.

use std::time::Duration;

use kafka_client_core::{
    Moment, ShareAcknowledgement, ShareDisposition, ShareFetchSessionPhase, ShareRecordDecision,
};

use crate::clock::MonotonicClock;

use super::{
    fetch_acknowledgement::ShareAcknowledgementPreparationFailureKind,
    fetch_session::ShareFetchSessionOwner,
    fetch_session_settlement::settlement_test::{owner, stage, success},
};

#[test]
fn exact_delivery_prepares_one_acknowledgement_under_the_original_deadline() {
    let (mut owner, acknowledgement) = delivered_acknowledgement();
    let clock = MonotonicClock::new();
    let capture = clock
        .capture_deadline_after(Duration::from_secs(30))
        .unwrap_or_else(|error| panic!("deadline: {error:?}"));

    owner
        .prepare_acknowledgement(acknowledgement, capture, capture.now())
        .unwrap_or_else(|failure| panic!("preparation: {:?}", failure.kind));

    assert_eq!(
        owner.machine().phase(),
        ShareFetchSessionPhase::Acknowledging
    );
    assert!(owner.prepared_acknowledgement.is_some());
    assert_eq!(owner.next_deadline(), Some(capture.deadline()));
}

#[test]
fn elapsed_admission_returns_the_exact_capability_without_mutation() {
    let (mut owner, acknowledgement) = delivered_acknowledgement();
    let expected = acknowledgement.batches().len();
    let clock = MonotonicClock::new();
    let capture = clock
        .capture_deadline_after(Duration::from_nanos(1))
        .unwrap_or_else(|error| panic!("deadline: {error:?}"));
    let failure = owner
        .prepare_acknowledgement(
            acknowledgement,
            capture,
            Moment::from_tick(capture.deadline().tick()),
        )
        .err()
        .unwrap_or_else(|| panic!("elapsed admission must reject"));

    assert_eq!(
        failure.kind,
        ShareAcknowledgementPreparationFailureKind::Core(
            kafka_client_core::ShareAcknowledgementApplyErrorKind::DeadlineElapsed
        )
    );
    assert_eq!(failure.acknowledgement.batches().len(), expected);
    assert_eq!(owner.machine().phase(), ShareFetchSessionPhase::Ready);
    assert!(owner.prepared_acknowledgement.is_none());
}

pub(super) fn delivered_acknowledgement() -> (ShareFetchSessionOwner, ShareAcknowledgement) {
    let mut owner = owner();
    stage(&mut owner, success(Some(30_000)));
    owner
        .settle_terminal(Moment::from_tick(7))
        .unwrap_or_else(|error| panic!("fetch settlement: {error:?}"));
    let delivery = owner
        .take_delivery(Moment::from_tick(8))
        .unwrap_or_else(|error| panic!("take delivery: {error:?}"))
        .unwrap_or_else(|| panic!("delivery expected"));
    let (_fence, partitions, acquisitions) = delivery.into_parts();
    let mut decisions = Vec::new();
    for acquisition in &acquisitions {
        let range = acquisition.range();
        for offset in range.first_offset()..=range.last_offset() {
            decisions.push(ShareRecordDecision::new(
                acquisition.generation(),
                offset,
                ShareDisposition::Accept,
            ));
        }
    }
    drop(partitions);
    let acknowledgement = ShareAcknowledgement::try_new(acquisitions, decisions)
        .unwrap_or_else(|error| panic!("acknowledgement: {error:?}"));
    (owner, acknowledgement)
}
