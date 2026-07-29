//! Version, scalar, diagnostic, and retained-capacity normalization evidence.

use kafka_wire::UnregisterBrokerResponse;
use kafka_wire_core::StrBytes;

use super::{
    UNREGISTER_BROKER_MAX_RETAINED_BYTES, UnregisterBrokerResponseFailure,
    normalize_unregister_broker_response, retention::UNREGISTER_BROKER_MAX_DIAGNOSTIC_BYTES,
};

#[test]
fn response_preserves_success_and_nullable_diagnostic() {
    let response = response(9, 0, None);
    let normalized =
        normalize(&response, UNREGISTER_BROKER_MAX_RETAINED_BYTES).expect("valid success");
    let retained = core::mem::size_of_val(&normalized);

    assert_eq!(normalized.into_parts(), (9, 0, None, false, retained));
}

#[test]
fn response_preserves_signed_code_and_utf8_safe_bounded_diagnostic() {
    let diagnostic = format!(
        "{}é",
        "x".repeat(UNREGISTER_BROKER_MAX_DIAGNOSTIC_BYTES - 1)
    );
    let response = response(3, -42, Some(&diagnostic));
    let normalized =
        normalize(&response, UNREGISTER_BROKER_MAX_RETAINED_BYTES).expect("valid broker failure");
    let (throttle, code, message, truncated, retained) = normalized.into_parts();

    assert_eq!(throttle, 3);
    assert_eq!(code, -42);
    assert_eq!(
        message.as_deref().map(str::len),
        Some(UNREGISTER_BROKER_MAX_DIAGNOSTIC_BYTES - 1)
    );
    assert!(truncated);
    assert!(retained <= UNREGISTER_BROKER_MAX_RETAINED_BYTES);
}

#[test]
fn response_rejects_missing_wrong_version_and_negative_throttle() {
    let valid_response = response(0, 0, None);
    assert_eq!(
        normalize_unregister_broker_response(
            None,
            &valid_response,
            UNREGISTER_BROKER_MAX_RETAINED_BYTES
        ),
        Err(UnregisterBrokerResponseFailure::MissingSelectedVersion)
    );
    assert_eq!(
        normalize_unregister_broker_response(
            Some(1),
            &valid_response,
            UNREGISTER_BROKER_MAX_RETAINED_BYTES
        ),
        Err(UnregisterBrokerResponseFailure::UnsupportedApiVersion { actual: 1 })
    );
    let response = response(-1, 0, None);
    assert_eq!(
        normalize(&response, UNREGISTER_BROKER_MAX_RETAINED_BYTES),
        Err(UnregisterBrokerResponseFailure::NegativeThrottleTime { actual: -1 })
    );
}

#[test]
fn response_honors_caller_capacity_below_the_absolute_ceiling() {
    let response = response(0, 0, Some("denied"));

    assert!(matches!(
        normalize(&response, 1),
        Err(UnregisterBrokerResponseFailure::RetainedBytes { limit: 1, .. })
    ));
}

fn normalize(
    response: &UnregisterBrokerResponse,
    limit: usize,
) -> Result<super::NormalizedUnregisterBrokerResponse, UnregisterBrokerResponseFailure> {
    normalize_unregister_broker_response(Some(0), response, limit)
}

fn response(
    throttle_time_ms: i32,
    error_code: i16,
    diagnostic: Option<&str>,
) -> UnregisterBrokerResponse {
    let mut response = UnregisterBrokerResponse::default();
    response.throttle_time_ms = throttle_time_ms;
    response.error_code = error_code;
    response.error_message = diagnostic.map(StrBytes::from);
    response
}
