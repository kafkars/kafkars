//! Exact acknowledgement validation, sorting, compression, and gap evidence.

use super::{
    ShareAcknowledgeType, ShareAcknowledgement, ShareAcknowledgementBuildErrorKind,
    ShareDisposition, ShareRecordDecision,
};
use crate::{
    Moment,
    consumer::share_fetch::test_support::{fence, ledger, okay, range, rejected},
};

#[test]
fn normalization_sorts_ranges_and_keeps_gap_values_internal() {
    let mut ledger = ledger(3, 12, 30);
    okay(ledger.try_admit(
        fence(1),
        Moment::from_tick(1),
        vec![range(2, 2, 4, 6, 1, 5, 50), range(1, 1, 8, 9, 1, 5, 50)],
    ));
    let acquisitions = okay(ledger.claim_batch(fence(1), 2, Moment::from_tick(2)));
    let first = acquisitions[0].generation();
    let second = acquisitions[1].generation();
    let acknowledgement = okay(ShareAcknowledgement::try_new(
        acquisitions,
        vec![
            ShareRecordDecision::new(first, 6, ShareDisposition::Reject),
            ShareRecordDecision::new(second, 9, ShareDisposition::Accept),
            ShareRecordDecision::new(first, 4, ShareDisposition::Release),
        ],
    ));

    assert_eq!(acknowledgement.batches().len(), 2);
    assert_eq!(acknowledgement.batches()[0].topic_uuid().bytes()[0], 1);
    assert_eq!(
        acknowledgement.batches()[1].acknowledge_types(),
        &[
            ShareAcknowledgeType::Release,
            ShareAcknowledgeType::Gap,
            ShareAcknowledgeType::Reject,
        ]
    );
}

#[test]
fn uniform_range_uses_one_wire_value() {
    let (acquisitions, generation) = delivered(0, 2);
    let acknowledgement = okay(ShareAcknowledgement::try_new(
        acquisitions,
        (0..=2)
            .map(|offset| ShareRecordDecision::new(generation, offset, ShareDisposition::Accept))
            .collect(),
    ));

    assert_eq!(
        acknowledgement.batches()[0].acknowledge_types(),
        &[ShareAcknowledgeType::Accept]
    );
    assert_eq!(ShareAcknowledgeType::Accept.wire_value(), 1);
    assert_eq!(ShareAcknowledgeType::Release.wire_value(), 2);
    assert_eq!(ShareAcknowledgeType::Reject.wire_value(), 3);
}

#[test]
fn structural_rejections_return_exact_linear_inputs_without_mutation() {
    for (decisions, expected) in [
        (
            Vec::new(),
            ShareAcknowledgementBuildErrorKind::EmptyDecisions,
        ),
        (
            vec![ShareRecordDecision::new(
                super::super::ShareAcquisitionGeneration::initial(),
                -1,
                ShareDisposition::Accept,
            )],
            ShareAcknowledgementBuildErrorKind::InvalidOffset,
        ),
    ] {
        let (acquisitions, _generation) = delivered(0, 0);
        let error = rejected(ShareAcknowledgement::try_new(acquisitions, decisions));
        assert_eq!(error.kind(), expected);
        let (acquisitions, _decisions) = error.into_parts();
        assert_eq!(acquisitions.len(), 1);
    }
}

#[test]
fn duplicate_foreign_out_of_range_and_missing_decisions_fail_closed() {
    let (acquisitions, generation) = delivered(4, 5);
    let duplicate = ShareRecordDecision::new(generation, 4, ShareDisposition::Accept);
    assert_eq!(
        rejected(ShareAcknowledgement::try_new(
            acquisitions,
            vec![duplicate, duplicate]
        ))
        .kind(),
        ShareAcknowledgementBuildErrorKind::DuplicateDecision
    );

    let (acquisitions, generation) = delivered(4, 5);
    assert_eq!(
        rejected(ShareAcknowledgement::try_new(
            acquisitions,
            vec![ShareRecordDecision::new(
                generation,
                6,
                ShareDisposition::Accept
            )],
        ))
        .kind(),
        ShareAcknowledgementBuildErrorKind::OffsetOutsideRange
    );

    let (acquisitions, _generation) = delivered(4, 5);
    let foreign = super::super::ShareAcquisitionGeneration::try_from_raw(99)
        .unwrap_or_else(|| panic!("generation"));
    assert_eq!(
        rejected(ShareAcknowledgement::try_new(
            acquisitions,
            vec![ShareRecordDecision::new(
                foreign,
                4,
                ShareDisposition::Accept
            )],
        ))
        .kind(),
        ShareAcknowledgementBuildErrorKind::UnknownAcquisition
    );

    let (mut acquisitions, generation) = delivered(4, 5);
    let (more, _other) = delivered_with_fence(2, 8, 8);
    acquisitions.extend(more);
    assert_eq!(
        rejected(ShareAcknowledgement::try_new(
            acquisitions,
            vec![ShareRecordDecision::new(
                generation,
                4,
                ShareDisposition::Accept
            )],
        ))
        .kind(),
        ShareAcknowledgementBuildErrorKind::MixedSession
    );

    let mut ledger = ledger(2, 4, 30);
    okay(ledger.try_admit(
        fence(1),
        Moment::from_tick(1),
        vec![range(1, 1, 0, 0, 1, 5, 50), range(1, 1, 1, 1, 1, 5, 50)],
    ));
    let acquisitions = okay(ledger.claim_batch(fence(1), 2, Moment::from_tick(2)));
    let generation = acquisitions[0].generation();
    assert_eq!(
        rejected(ShareAcknowledgement::try_new(
            acquisitions,
            vec![ShareRecordDecision::new(
                generation,
                0,
                ShareDisposition::Accept
            )],
        ))
        .kind(),
        ShareAcknowledgementBuildErrorKind::MissingDecision
    );
}

fn delivered(
    first: i64,
    last: i64,
) -> (
    Vec<super::super::ShareAcquisition>,
    super::super::ShareAcquisitionGeneration,
) {
    delivered_with_fence(1, first, last)
}

fn delivered_with_fence(
    session: i32,
    first: i64,
    last: i64,
) -> (
    Vec<super::super::ShareAcquisition>,
    super::super::ShareAcquisitionGeneration,
) {
    let mut ledger = ledger(1, 16, 30);
    okay(ledger.try_admit(
        fence(session),
        Moment::from_tick(1),
        vec![range(1, 1, first, last, 1, 5, 50)],
    ));
    let acquisitions = okay(ledger.claim_batch(fence(session), 1, Moment::from_tick(2)));
    let generation = acquisitions[0].generation();
    (acquisitions, generation)
}
