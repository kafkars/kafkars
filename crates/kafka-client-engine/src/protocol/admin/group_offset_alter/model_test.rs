//! Borrowed offset-alteration fact access scenarios.

use core::num::NonZeroI16;

use super::{
    ValidatedOffsetCommitResponse,
    model::{OffsetCommitPartitionRef, OffsetCommitPartitionResult, OffsetCommitTargetRef},
};

#[test]
fn target_preserves_offset_epoch_and_nullable_metadata_scalars() {
    let target = OffsetCommitTargetRef::new("orders", 7, 91, Some(4), Some(""));
    assert_eq!(target.topic(), "orders");
    assert_eq!(target.partition(), 7);
    assert_eq!(target.next_offset(), 91);
    assert_eq!(target.leader_epoch(), Some(4));
    assert_eq!(target.metadata(), Some(""));

    let absent = OffsetCommitTargetRef::new("audit", 0, 3, None, None);
    assert_eq!(absent.leader_epoch(), None);
    assert_eq!(absent.metadata(), None);
}

#[test]
fn validated_response_transfers_only_its_charged_entry_vector() {
    let code = NonZeroI16::new(-31_000).unwrap_or_else(|| panic!("nonzero code"));
    let entry = OffsetCommitPartitionRef::new(
        "orders",
        7,
        OffsetCommitPartitionResult::Rejected { code },
        0,
    );
    let response = ValidatedOffsetCommitResponse::new(vec![entry], 9, 64);
    assert_eq!(response.throttle_time_ms(), 9);
    assert_eq!(response.retained_charge(), 64);
    assert_eq!(
        response.entries()[0].result(),
        OffsetCommitPartitionResult::Rejected { code }
    );
    assert_eq!(response.into_validated_alterations().len(), 1);
}
