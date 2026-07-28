//! Nonreused transactional offset-transfer identity scenarios.

use super::TransactionOffsetCommitId;

#[test]
fn operation_identity_starts_nonzero_advances_and_exhausts_without_wraparound() {
    let first = TransactionOffsetCommitId::initial();
    let second = first
        .checked_next()
        .unwrap_or_else(|| panic!("second identity"));

    assert_eq!(first.get(), 1);
    assert_eq!(second.get(), 2);
    assert_eq!(
        TransactionOffsetCommitId::from_raw_for_test(u64::MAX).checked_next(),
        None
    );
}
