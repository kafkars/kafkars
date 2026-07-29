//! Exact error diagnostics and terminal-category scenarios.

use core::num::NonZeroI16;

use super::{
    DescribeMetadataQuorumBrokerError, DescribeMetadataQuorumPartitionError,
    DescribeMetadataQuorumTerminal,
};

#[test]
fn top_level_error_preserves_signed_code_and_bounded_diagnostic_facts() {
    let error = DescribeMetadataQuorumBrokerError::new(
        NonZeroI16::new(-41).unwrap_or_else(|| panic!("nonzero")),
        Some("quorum unavailable".to_owned()),
        true,
    );

    assert_eq!(error.code(), -41);
    assert_eq!(error.message(), Some("quorum unavailable"));
    assert!(error.message_truncated());
    assert_eq!(
        error.into_parts(),
        (-41, Some("quorum unavailable".to_owned()), true)
    );
}

#[test]
fn fixed_partition_rejection_is_distinct_from_top_level_rejection() {
    let partition = DescribeMetadataQuorumPartitionError::new(
        NonZeroI16::new(3).unwrap_or_else(|| panic!("nonzero")),
        None,
        false,
    );
    let terminal = DescribeMetadataQuorumTerminal::PartitionRejected(partition);

    assert!(matches!(
        terminal,
        DescribeMetadataQuorumTerminal::PartitionRejected(error)
            if error.code() == 3 && error.message().is_none()
    ));
}
