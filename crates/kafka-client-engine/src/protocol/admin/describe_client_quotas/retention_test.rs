//! Focused evidence for conservative quota response byte accounting.

use super::{
    normalize_describe_client_quotas_response,
    response_test::{entity, entry, response, value},
    retention::{
        MAX_DIAGNOSTIC_BYTES, bounded_diagnostic_len, normalized_retained_charge,
        response_peak_charge,
    },
};

#[test]
fn response_peak_covers_the_normalized_result_while_scratch_is_live() {
    let response = response(Some(vec![entry(
        vec![entity("client-id", Some("orders")), entity("user", None)],
        vec![
            value("consumer_byte_rate", 1024.0),
            value("request_percentage", 50.0),
        ],
    )]));
    let peak = response_peak_charge(&response).expect("bounded peak");
    let normalized = normalize_describe_client_quotas_response(1, &response, peak)
        .expect("peak admits normalization");
    let retained = normalized_retained_charge(&normalized).expect("bounded retained result");

    assert_eq!(normalized.retained_bytes, peak);
    assert!(retained <= peak);
}

#[test]
fn diagnostic_charge_uses_a_utf8_safe_prefix() {
    let diagnostic = format!("{}é", "x".repeat(MAX_DIAGNOSTIC_BYTES - 1));

    assert_eq!(
        bounded_diagnostic_len(Some(&diagnostic)),
        MAX_DIAGNOSTIC_BYTES - 1
    );
    assert_eq!(bounded_diagnostic_len(None), 0);
}
