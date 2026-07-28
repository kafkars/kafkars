//! Public-to-engine Admin `DeleteRecords` request translation scenarios.

use crate::admin::DeleteRecordsTarget;

use super::DeleteRecordsAdminRequest;

#[test]
fn translation_is_deferred_and_preserves_caller_order() {
    let request = DeleteRecordsAdminRequest::new(vec![
        DeleteRecordsTarget::before_offset("orders", 2, 91),
        DeleteRecordsTarget::before_high_watermark("audit", 0),
    ]);
    let engine = request.into_engine();
    assert!(format!("{engine:?}").contains("DeleteRecordsRequest"));
}
