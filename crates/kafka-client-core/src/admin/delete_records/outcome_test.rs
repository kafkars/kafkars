//! Result scenarios for deterministic Admin `DeleteRecords` values.

use core::num::NonZeroI16;

use super::{
    DeleteRecordsBatch, DeleteRecordsBrokerError, DeleteRecordsOutcome, DeleteRecordsResult,
    DeletedRecords,
};

#[test]
fn result_preserves_low_watermark_and_exact_broker_code() {
    let success = DeleteRecordsOutcome::deleted("orders".to_owned(), 2, DeletedRecords::new(91));
    let DeleteRecordsResult::Deleted(value) = success.result() else {
        panic!("expected deletion result");
    };
    assert_eq!(value.low_watermark(), 91);

    let code = NonZeroI16::new(-31_999).unwrap_or_else(|| panic!("code is nonzero"));
    let failed =
        DeleteRecordsOutcome::failed("audit".to_owned(), 0, DeleteRecordsBrokerError::new(code));
    let DeleteRecordsResult::Failed(error) = failed.result() else {
        panic!("expected broker failure");
    };
    assert_eq!(error.code(), -31_999);
}

#[test]
fn batch_preserves_caller_order_and_maximum_throttle() {
    let batch = DeleteRecordsBatch::new(
        73,
        vec![
            DeleteRecordsOutcome::deleted("orders".to_owned(), 2, DeletedRecords::new(91)),
            DeleteRecordsOutcome::deleted("audit".to_owned(), 0, DeletedRecords::new(42)),
        ],
    );
    assert_eq!(batch.throttle_time_ms(), 73);
    assert_eq!(batch.outcomes()[0].topic(), "orders");
    assert_eq!(batch.outcomes()[1].topic(), "audit");
}
