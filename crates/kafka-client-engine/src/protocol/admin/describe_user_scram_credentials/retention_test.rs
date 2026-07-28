//! Retained-capacity evidence for SCRAM response normalization.

use super::{
    DescribeUserScramCredentialsRequestRef, DescribeUserScramCredentialsResponseFailure,
    normalize_describe_user_scram_credentials_response,
    response_test::{described, response},
    retention::{normalized_retained_charge, response_peak_charge},
};

#[test]
fn response_charge_covers_correlation_scratch_and_normalized_storage() {
    let users = vec!["bob".to_owned(), "alice".to_owned()];
    let request = DescribeUserScramCredentialsRequestRef::selected(&users);
    let response = response(vec![
        described("alice", &[(1, 4096)]),
        described("bob", &[(2, 8192)]),
    ]);
    let required = response_peak_charge(request, &response)
        .unwrap_or_else(|| panic!("bounded response charge should fit"));
    assert_eq!(
        normalize_describe_user_scram_credentials_response(0, request, &response, required - 1,),
        Err(DescribeUserScramCredentialsResponseFailure::RetainedBytes {
            required,
            limit: required - 1,
        })
    );
    let normalized =
        normalize_describe_user_scram_credentials_response(0, request, &response, required)
            .unwrap_or_else(|error| panic!("exact charge should fit: {error:?}"));
    let actual = normalized_retained_charge(&normalized)
        .unwrap_or_else(|| panic!("normalized charge should fit"));
    assert!(actual <= required);
    assert_eq!(normalized.retained_bytes, required.max(actual));
}

#[test]
fn unicode_diagnostic_prefix_is_charged_at_the_retained_boundary() {
    let mut response = response(Vec::new());
    response.error_code = -41;
    response.error_message = Some(format!("{}é", "x".repeat(1023)).as_str().into());
    let request = DescribeUserScramCredentialsRequestRef::all();
    let required = response_peak_charge(request, &response)
        .unwrap_or_else(|| panic!("diagnostic response charge should fit"));
    let normalized =
        normalize_describe_user_scram_credentials_response(0, request, &response, required)
            .unwrap_or_else(|error| panic!("bounded diagnostic should fit: {error:?}"));
    assert_eq!(
        normalized
            .error_message
            .as_deref()
            .unwrap_or_else(|| panic!("expected diagnostic"))
            .len(),
        1023
    );
    assert!(normalized.error_message_truncated);
}
