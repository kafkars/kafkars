//! Error translation and display scenarios for the private commit host.

use super::error::GroupOffsetCommitHostError;

#[test]
fn error_display_names_the_host_and_retains_the_variant() {
    let error = GroupOffsetCommitHostError::ByteAccounting;

    assert_eq!(
        error.to_string(),
        "group offset commit host invariant failed: ByteAccounting"
    );
}
