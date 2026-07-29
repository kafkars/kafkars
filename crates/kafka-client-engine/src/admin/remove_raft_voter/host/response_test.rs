//! Exact success, broker-error, and contradictory response scenarios.

use kafka_client_core::RemoveRaftVoterInput;

use super::response::normalized_input;

#[test]
fn success_preserves_nonnegative_throttle() {
    let RemoveRaftVoterInput::BrokerResponded { success } = normalized_input(19, 0, None, false)
    else {
        panic!("broker success expected");
    };
    assert_eq!(success.throttle_time_ms(), 19);
}

#[test]
fn signed_nonzero_broker_error_preserves_diagnostic() {
    let RemoveRaftVoterInput::BrokerRejected { error } =
        normalized_input(23, -32_000, Some("controller said no".to_owned()), true)
    else {
        panic!("broker rejection expected");
    };
    assert_eq!(error.throttle_time_ms(), 23);
    assert_eq!(error.code(), -32_000);
    assert_eq!(error.message(), Some("controller said no"));
    assert!(error.message_truncated());
}

#[test]
fn a_success_code_cannot_be_bound_to_an_error_diagnostic() {
    assert!(matches!(
        normalized_input(0, 0, Some("contradiction".to_owned()), false),
        RemoveRaftVoterInput::InvalidResponse
    ));
}
