//! Atomic staged-batch claim and exact abandonment evidence.

use crate::{ByteCount, Moment};

use super::{
    ShareAcquisitionAdmissionErrorKind,
    test_support::{fence, ledger, okay, range, rejected},
};

#[test]
fn complete_staged_set_claims_and_abandons_atomically() {
    let mut ledger = ledger(3, 6, 30);
    okay(ledger.try_admit(
        fence(1),
        Moment::from_tick(10),
        vec![range(1, 1, 0, 1, 2, 10, 50), range(1, 1, 2, 3, 2, 10, 50)],
    ));

    let acquisitions = okay(ledger.claim_batch(fence(1), 2, Moment::from_tick(11)));
    assert_eq!(acquisitions.len(), 2);
    assert_eq!(acquisitions[0].generation().get(), 1);
    assert_eq!(acquisitions[1].generation().get(), 2);
    assert_eq!(
        ledger.claim_batch(fence(1), 1, Moment::from_tick(11)),
        Err(ShareAcquisitionAdmissionErrorKind::InvalidOwnership)
    );

    let releases = okay(ledger.abandon_batch(acquisitions));
    assert_eq!(releases.len(), 2);
    assert_eq!(ledger.retained_bytes(), ByteCount::new(0));
    assert_eq!(ledger.retained_records(), 4);
}

#[test]
fn count_expiry_and_duplicate_rejections_do_not_partially_mutate() {
    let mut ledger = ledger(3, 6, 30);
    okay(ledger.try_admit(
        fence(1),
        Moment::from_tick(10),
        vec![range(1, 1, 0, 0, 1, 7, 20), range(1, 1, 1, 1, 1, 8, 20)],
    ));
    assert_eq!(
        ledger.claim_batch(fence(1), 1, Moment::from_tick(11)),
        Err(ShareAcquisitionAdmissionErrorKind::InvalidOwnership)
    );
    assert_eq!(
        ledger.claim_batch(fence(1), 2, Moment::from_tick(20)),
        Err(ShareAcquisitionAdmissionErrorKind::ExpiredLock)
    );
    assert_eq!(ledger.retained_bytes(), ByteCount::new(15));

    let acquisitions = okay(ledger.claim_batch(fence(1), 2, Moment::from_tick(12)));
    let Some(first) = acquisitions.first() else {
        panic!("first acquisition");
    };
    let duplicate = vec![duplicate_of(first), duplicate_of(first)];
    let error = rejected(ledger.abandon_batch(duplicate));
    assert_eq!(
        error.kind(),
        ShareAcquisitionAdmissionErrorKind::InvalidOwnership
    );
    assert_eq!(error.into_acquisitions().len(), 2);
    assert_eq!(ledger.retained_bytes(), ByteCount::new(15));
    assert_eq!(okay(ledger.abandon_batch(acquisitions)).len(), 2);
}

#[test]
fn claim_is_scoped_to_one_exact_broker_session_fence() {
    let mut ledger = ledger(3, 6, 30);
    okay(ledger.try_admit(
        fence(1),
        Moment::from_tick(10),
        vec![range(1, 1, 0, 0, 1, 5, 50)],
    ));
    okay(ledger.try_admit(
        fence(2),
        Moment::from_tick(10),
        vec![range(2, 2, 0, 0, 1, 6, 50)],
    ));

    let first = okay(ledger.claim_batch(fence(1), 1, Moment::from_tick(11)));
    assert_eq!(first.len(), 1);
    let second = okay(ledger.claim_batch(fence(2), 1, Moment::from_tick(11)));
    assert_eq!(second.len(), 1);
    assert_eq!(ledger.retained_bytes(), ByteCount::new(11));

    let mut combined = first;
    combined.extend(second);
    let error = rejected(ledger.abandon_batch(combined));
    assert_eq!(
        error.kind(),
        ShareAcquisitionAdmissionErrorKind::InvalidOwnership
    );
    assert_eq!(error.into_acquisitions().len(), 2);
    assert_eq!(ledger.retained_bytes(), ByteCount::new(11));
}

fn duplicate_of(acquisition: &super::ShareAcquisition) -> super::ShareAcquisition {
    super::ShareAcquisition::delivered(
        acquisition.generation(),
        acquisition.fence(),
        acquisition.range(),
    )
}
