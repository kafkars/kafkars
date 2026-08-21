//! Public Admin `DeleteRecordsResultInfo` value scenarios.

use super::DeleteRecordsResultInfo;

#[test]
fn result_preserves_low_watermark() {
    let value = DeleteRecordsResultInfo::new(91);

    assert_eq!(value.low_watermark(), 91);
}
