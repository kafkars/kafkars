//! Borrowed offset-deletion fact access scenarios.

use core::num::NonZeroI16;

use super::{
    ValidatedOffsetDeleteResponse,
    model::{OffsetDeletePartitionRef, OffsetDeletePartitionResult, OffsetDeleteTargetRef},
};

#[test]
fn target_and_terminal_facts_preserve_exact_scalars() {
    let target = OffsetDeleteTargetRef::new("orders", 7);
    assert_eq!(target.topic(), "orders");
    assert_eq!(target.partition(), 7);

    let code = NonZeroI16::new(-31_000).unwrap_or_else(|| panic!("nonzero code"));
    let entry = OffsetDeletePartitionRef::new(
        "orders",
        7,
        OffsetDeletePartitionResult::Rejected { code },
        3,
    );
    assert_eq!(entry.topic(), "orders");
    assert_eq!(entry.partition(), 7);
    assert_eq!(
        entry.result(),
        OffsetDeletePartitionResult::Rejected { code }
    );
}

#[test]
fn validated_response_transfers_only_its_charged_entry_vector() {
    let response = ValidatedOffsetDeleteResponse::new(Vec::new(), 9, None, 64);
    assert_eq!(response.throttle_time_ms(), 9);
    assert_eq!(response.top_level_error(), None);
    assert_eq!(response.retained_charge(), 64);
    assert!(response.into_validated_deletions().is_empty());
}
