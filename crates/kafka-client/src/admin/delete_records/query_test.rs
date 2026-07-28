//! Public Admin `DeleteRecordsTarget` value scenarios.

use super::DeleteRecordsTarget;

#[test]
fn target_preserves_identity_and_offset_without_early_validation() {
    let target = DeleteRecordsTarget::before_offset("orders", -1, -2);

    assert_eq!(target.topic(), "orders");
    assert_eq!(target.partition(), -1);
    assert_eq!(target.deletion_offset(), -2);
    assert_eq!(
        DeleteRecordsTarget::before_high_watermark("audit", 0).deletion_offset(),
        -1
    );
}
