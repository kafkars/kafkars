//! Ownership-moving broker failure scenarios for legacy full-snapshot topic configuration.

use core::num::NonZeroI16;

use super::LegacyAlterConfigBrokerError;
use super::{LegacyAlterConfigOutcome, LegacyAlterConfigResult, LegacyAlterConfigsBatch};

#[test]
fn exact_signed_code_and_bounded_diagnostic_fact_move_without_reclassification() {
    let code = NonZeroI16::new(-32_123).unwrap_or_else(|| panic!("code is nonzero"));
    let error = LegacyAlterConfigBrokerError::new(code, Some("future error".to_owned()), true);

    assert_eq!(error.code(), -32_123);
    assert_eq!(error.message(), Some("future error"));
    assert!(error.message_truncated());
    assert_eq!(
        error.into_parts(),
        (-32_123, Some("future error".to_owned()), true)
    );
}

#[test]
fn exact_resource_identity_moves_without_topic_reclassification() {
    let batch = LegacyAlterConfigsBatch::new(
        17,
        vec![
            LegacyAlterConfigOutcome::resource_altered(4, "1"),
            LegacyAlterConfigOutcome::resource_failed(
                64,
                "future",
                LegacyAlterConfigBrokerError::new(
                    NonZeroI16::new(-30_001).unwrap_or_else(|| panic!("nonzero")),
                    None,
                    false,
                ),
            ),
        ],
    );

    assert_eq!(batch.resources()[0].resource_type(), 4);
    assert_eq!(batch.resources()[0].resource_name(), "1");
    assert_eq!(
        batch.resources()[0].result(),
        &LegacyAlterConfigResult::Altered
    );
    assert_eq!(batch.resources()[1].resource_type(), 64);
    assert_eq!(batch.resources()[1].resource_name(), "future");
}
