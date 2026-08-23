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
    let delivery = okay(ledger.claim_batch(fence(1), 1, Moment::from_tick(11)));
    let Some(acquisition) = delivery.first() else {
        panic!("acquisition");
    };
    let generation = acquisition.generation();
    let releases = okay(ledger.abandon_batch(delivery));
    let release = releases
        .first()
        .unwrap_or_else(|| panic!("acquisition release"));
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

#[test]
fn application_owned_ranges_survive_lock_expiry_and_session_loss() {
    let mut ledger = ledger(2, 4, 20);
    okay(ledger.try_admit(
        fence(1),
        Moment::from_tick(10),
        vec![range(1, 1, 0, 1, 2, 12, 20)],
    ));
    let delivery = okay(ledger.claim_batch(fence(1), 1, Moment::from_tick(11)));

    assert_eq!(ledger.expire_one(Moment::from_tick(20)), Ok(None));
    assert_eq!(ledger.retire_one_session(fence(1)), Ok(None));
    assert_eq!(ledger.retained_bytes(), ByteCount::new(12));
    assert_eq!(ledger.retained_records(), 2);
    assert_eq!(ledger.next_reclaimable_deadline(), None);
    assert_eq!(ledger.retire_one_reclaimable(), Ok(None));

    assert_eq!(okay(ledger.abandon_batch(delivery)).len(), 1);
    assert_eq!(ledger.retained_bytes(), ByteCount::new(0));
    assert_eq!(
        ledger.next_reclaimable_deadline(),
        Some(crate::Deadline::from_tick(20))
    );
    assert!(ledger.retire_one_reclaimable().is_ok());
    assert!(ledger.is_empty());
}
