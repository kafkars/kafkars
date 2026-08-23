//! Exact drop, expiry, and session-loss reclamation evidence.

use crate::{ByteCount, Moment};

use super::{
    ShareAcquisitionAdmissionErrorKind,
    test_support::{fence, ledger, okay, range, rejected, some},
};

#[test]
fn drop_releases_payload_once_but_retains_lock_until_expiry() {
    let mut ledger = ledger(2, 4, 20);
    okay(ledger.try_admit(
        fence(1),
        Moment::from_tick(10),
        vec![range(1, 1, 0, 1, 2, 12, 20)],
    ));
    let delivery = some(ledger.claim_next(Moment::from_tick(11)));
    let generation = delivery.generation();
    let release = okay(ledger.abandon(delivery));
    assert_eq!(release.retained_bytes(), ByteCount::new(12));
    assert_eq!(ledger.retained_bytes(), ByteCount::new(0));
    assert_eq!(ledger.retained_records(), 2);
    assert_eq!(ledger.expire_one(Moment::from_tick(19)), Ok(None));
    let expired = some(okay(ledger.expire_one(Moment::from_tick(20))));
    assert_eq!(expired.generation(), generation);
    assert_eq!(expired.retained_bytes(), ByteCount::new(0));
    assert!(ledger.is_empty());
}

#[test]
fn uuid_aliases_and_session_reclamation_fail_closed() {
    let mut ledger = ledger(3, 6, 30);
    okay(ledger.try_admit(
        fence(1),
        Moment::from_tick(1),
        vec![range(1, 1, 0, 0, 1, 5, 30)],
    ));
    let alias = vec![range(1, 2, 1, 1, 1, 5, 30)];
    assert_eq!(
        rejected(ledger.try_admit(fence(1), Moment::from_tick(2), alias)).kind(),
        ShareAcquisitionAdmissionErrorKind::TopicIdentityMismatch
    );
    assert_eq!(ledger.retire_one_session(fence(2)), Ok(None));
    let release = some(okay(ledger.retire_one_session(fence(1))));
    assert_eq!(release.retained_bytes(), ByteCount::new(5));
    assert!(ledger.is_empty());
}
