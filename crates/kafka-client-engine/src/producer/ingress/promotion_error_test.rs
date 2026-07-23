//! Dormant promotion progress-owner scenarios.

use super::promotion_error::PendingPromotionProgress;

#[test]
fn empty_progress_preserves_bounded_scan_facts_without_a_linear_owner() {
    let progress = PendingPromotionProgress::new(1, true, None);
    assert_eq!(progress.inspected(), 1);
    assert!(progress.remaining());
    assert!(progress.into_resolution().is_none());
}
