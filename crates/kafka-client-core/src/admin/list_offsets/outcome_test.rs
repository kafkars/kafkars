//! Scenarios for exact Admin `ListOffsets` result values.

use core::num::NonZeroI16;

use super::{
    AdminListOffset, AdminListOffsetBrokerError, AdminListOffsetOutcome, AdminListOffsetResult,
    AdminListOffsetsBatch,
};

#[test]
fn successful_value_preserves_optional_protocol_facts() {
    let value = AdminListOffset::new(Some(91), Some(1_700_000_000_123), Some(7));

    assert_eq!(value.offset(), Some(91));
    assert_eq!(value.timestamp_ms(), Some(1_700_000_000_123));
    assert_eq!(value.leader_epoch(), Some(7));
}

#[test]
fn partition_failure_retains_identity_and_exact_signed_code() {
    let code = NonZeroI16::new(-31_999).unwrap_or_else(|| panic!("code is nonzero"));
    let outcome = AdminListOffsetOutcome::failed(
        "audit".to_owned(),
        3,
        AdminListOffsetBrokerError::new(code),
    );

    assert_eq!(outcome.topic(), "audit");
    assert_eq!(outcome.partition(), 3);
    let AdminListOffsetResult::Failed(error) = outcome.result() else {
        panic!("partition must retain its broker failure");
    };
    assert_eq!(error.code(), -31_999);
}

#[test]
fn response_batch_retains_maximum_throttle_and_caller_order() {
    let batch = AdminListOffsetsBatch::new(
        73,
        vec![
            AdminListOffsetOutcome::listed(
                "orders".to_owned(),
                2,
                AdminListOffset::new(Some(91), None, None),
            ),
            AdminListOffsetOutcome::listed(
                "audit".to_owned(),
                0,
                AdminListOffset::new(None, None, None),
            ),
        ],
    );

    assert_eq!(batch.throttle_time_ms(), 73);
    assert_eq!(batch.outcomes()[0].topic(), "orders");
    assert_eq!(batch.outcomes()[1].topic(), "audit");
}
