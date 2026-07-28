//! Request construction scenarios for Admin `DeleteRecords`.

use kafka_client_core::DeleteRecordsTarget;

use super::{delete_records_request, request::DeleteRecordsRequestFailure};

#[test]
fn request_preserves_target_offset_and_timeout() {
    let target = DeleteRecordsTarget::new("orders".to_owned(), 2, -1);
    let request = delete_records_request(&target, 4_321)
        .unwrap_or_else(|error| panic!("request must construct: {error:?}"));

    assert_eq!(request.timeout_ms, 4_321);
    assert_eq!(request.topics[0].name.as_str(), "orders");
    assert_eq!(request.topics[0].partitions[0].partition_index, 2);
    assert_eq!(request.topics[0].partitions[0].offset, -1);
}

#[test]
fn negative_timeout_is_rejected_before_driver_admission() {
    let target = DeleteRecordsTarget::new("orders".to_owned(), 2, 91);
    assert_eq!(
        delete_records_request(&target, -1),
        Err(DeleteRecordsRequestFailure::NegativeTimeout { actual: -1 })
    );
}
