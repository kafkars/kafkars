//! Secret-free credential metadata and exact broker-error scenarios.

use core::num::NonZeroI16;

use super::{
    DescribeUserScramCredentialsBatch, DescribeUserScramCredentialsBrokerError,
    DescribeUserScramCredentialsUserOutcome, DescribeUserScramCredentialsUserResult,
    ScramCredentialInfo,
};

#[test]
fn credential_info_exposes_only_mechanism_and_iterations() {
    let info = ScramCredentialInfo::new(-7, 4096);

    assert_eq!(info.mechanism(), -7);
    assert_eq!(info.iterations(), 4096);
    assert_eq!(info.into_parts(), (-7, 4096));
}

#[test]
fn user_outcomes_and_batch_preserve_stable_scalar_facts() {
    let outcome = DescribeUserScramCredentialsUserOutcome::described(
        "alice".to_owned(),
        vec![ScramCredentialInfo::new(1, 4096)],
    );
    assert_eq!(outcome.user(), "alice");
    assert!(matches!(
        outcome.result(),
        DescribeUserScramCredentialsUserResult::Described(credentials)
            if credentials == &[ScramCredentialInfo::new(1, 4096)]
    ));

    let batch = DescribeUserScramCredentialsBatch::new(7, vec![outcome]);
    assert_eq!(batch.throttle_time_ms(), 7);
    assert_eq!(batch.outcomes().len(), 1);
}

#[test]
fn broker_error_preserves_exact_signed_code_and_bounded_diagnostic_facts() {
    let error = DescribeUserScramCredentialsBrokerError::new(
        NonZeroI16::new(-29).unwrap_or_else(|| panic!("nonzero")),
        Some("credential metadata unavailable".to_owned()),
        true,
    );

    assert_eq!(error.code(), -29);
    assert_eq!(error.message(), Some("credential metadata unavailable"));
    assert!(error.message_truncated());
}
