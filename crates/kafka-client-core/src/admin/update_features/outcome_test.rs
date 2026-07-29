//! Stable scalar accessors for feature outcomes and broker diagnostics.

use core::num::NonZeroI16;

use super::{
    UpdateFeatureOutcome, UpdateFeatureResult, UpdateFeaturesBatch, UpdateFeaturesBrokerError,
    UpdateFeaturesBrokerResponse,
};

#[test]
fn broker_error_retains_exact_signed_code_and_diagnostic_metadata() {
    let code = NonZeroI16::new(-32_111).unwrap_or_else(|| panic!("code is nonzero"));
    let error = UpdateFeaturesBrokerError::new(code, Some("rejected".to_owned()), true);
    assert_eq!(error.code(), -32_111);
    assert_eq!(error.message(), Some("rejected"));
    assert!(error.message_truncated());
    assert_eq!(
        error.into_parts(),
        (-32_111, Some("rejected".to_owned()), true)
    );
}

#[test]
fn feature_results_and_atomic_success_have_distinct_normalized_shapes() {
    let batch = UpdateFeaturesBatch::new(
        91,
        vec![UpdateFeatureOutcome::updated("metadata.version".to_owned())],
    );
    assert_eq!(batch.throttle_time_ms(), 91);
    assert_eq!(batch.outcomes()[0].feature(), "metadata.version");
    assert_eq!(batch.outcomes()[0].result(), &UpdateFeatureResult::Updated);
    let response = UpdateFeaturesBrokerResponse::FeatureResults(batch);
    let UpdateFeaturesBrokerResponse::FeatureResults(batch) = response else {
        panic!("old response must retain feature results");
    };
    let (throttle, outcomes) = batch.into_parts();
    assert_eq!(throttle, 91);
    assert_eq!(
        outcomes[0].clone().into_parts(),
        ("metadata.version".to_owned(), UpdateFeatureResult::Updated)
    );
    assert_eq!(
        UpdateFeaturesBrokerResponse::AtomicSuccess {
            throttle_time_ms: 7,
        },
        UpdateFeaturesBrokerResponse::AtomicSuccess {
            throttle_time_ms: 7,
        }
    );
}
