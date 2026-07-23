//! Bounded host-scheduling scenarios for concrete admin work.

#[test]
fn contended_admin_never_looks_quiescent_to_shutdown() {
    let progress = super::admin::AdminProgress::contended();
    assert_eq!(progress.unsettled, usize::MAX);
    assert!(!progress.driver_progress);
    assert_eq!(progress.next_deadline, None);
}
