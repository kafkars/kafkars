//! Stable scalar accessors for SCRAM credential alteration outcomes.

use core::num::NonZeroI16;

use super::{
    AlterUserScramCredentialBrokerError, AlterUserScramCredentialOutcome,
    AlterUserScramCredentialResult, AlterUserScramCredentialsBatch,
};

#[test]
fn broker_error_retains_exact_signed_code_and_bounded_diagnostic_metadata() {
    let code = NonZeroI16::new(-32_111).unwrap_or_else(|| panic!("code is nonzero"));
    let error = AlterUserScramCredentialBrokerError::new(code, Some("rejected".to_owned()), true);
    assert_eq!(error.code(), -32_111);
    assert_eq!(error.message(), Some("rejected"));
    assert!(error.message_truncated());
    assert_eq!(
        error.into_parts(),
        (-32_111, Some("rejected".to_owned()), true)
    );
}

#[test]
fn batch_retains_throttle_and_user_result_without_credential_material() {
    let batch = AlterUserScramCredentialsBatch::new(
        91,
        vec![AlterUserScramCredentialOutcome::altered("alice".to_owned())],
    );
    assert_eq!(batch.throttle_time_ms(), 91);
    assert_eq!(batch.outcomes()[0].user(), "alice");
    assert_eq!(
        batch.outcomes()[0].result(),
        &AlterUserScramCredentialResult::Altered
    );
    let (throttle, outcomes) = batch.into_parts();
    assert_eq!(throttle, 91);
    assert_eq!(
        outcomes[0].clone().into_parts(),
        ("alice".to_owned(), AlterUserScramCredentialResult::Altered)
    );
}
