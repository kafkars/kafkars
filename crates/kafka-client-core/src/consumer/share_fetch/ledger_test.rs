//! Executable bounded-ledger and linear-delivery evidence for `ShareFetch`.

use crate::{ByteCount, Moment};

use super::{
    ShareAcquisitionAdmissionErrorKind,
    test_support::{fence, ledger, okay, range, rejected, some},
};

#[test]
fn admission_is_atomic_bounded_and_generation_fenced() {
    let mut ledger = ledger(3, 6, 30);
    let ranges = vec![range(1, 1, 0, 1, 2, 10, 50), range(1, 1, 2, 3, 2, 10, 50)];
    let admitted = okay(ledger.try_admit(fence(1), Moment::from_tick(10), ranges));
    assert_eq!(admitted, 2);
    let first = some(ledger.claim_next(Moment::from_tick(10)));
    let second = some(ledger.claim_next(Moment::from_tick(10)));
    assert_eq!(first.generation().get(), 1);
    assert_eq!(second.generation().get(), 2);
    assert_eq!(ledger.retained_records(), 4);
    assert_eq!(ledger.retained_bytes(), ByteCount::new(20));

    let overlapping = vec![range(1, 1, 3, 4, 2, 1, 50)];
    let error = rejected(ledger.try_admit(fence(1), Moment::from_tick(10), overlapping));
    assert_eq!(
        error.kind(),
        ShareAcquisitionAdmissionErrorKind::OverlappingRange
    );
    assert_eq!(error.into_ranges().len(), 1);
    assert_eq!(ledger.len(), 2);
    assert_eq!(ledger.retained_bytes(), ByteCount::new(20));

    let capacity = vec![range(1, 1, 4, 6, 3, 11, 50)];
    let error = rejected(ledger.try_admit(fence(1), Moment::from_tick(10), capacity));
    assert_eq!(
        error.kind(),
        ShareAcquisitionAdmissionErrorKind::RecordCapacity
    );
    assert_eq!(ledger.len(), 2);
}
