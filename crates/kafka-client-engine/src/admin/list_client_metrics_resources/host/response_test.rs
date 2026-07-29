//! Exact broker error and stable success response translation scenarios.

use kafka_client_core::ListClientMetricsResourcesInput;

use super::response::normalized_input;

#[test]
fn signed_nonzero_broker_error_preserves_throttle_and_code() {
    let ListClientMetricsResourcesInput::BrokerRejected { error } =
        normalized_input(23, -32_000, Vec::new())
    else {
        panic!("broker rejection expected");
    };

    assert_eq!(error.throttle_time_ms(), 23);
    assert_eq!(error.code(), -32_000);
}

#[test]
fn successful_names_cross_as_generated_free_core_input() {
    let ListClientMetricsResourcesInput::BrokerResponded {
        throttle_time_ms,
        resource_names,
    } = normalized_input(19, 0, vec!["alpha".to_owned(), "zeta".to_owned()])
    else {
        panic!("broker response expected");
    };

    assert_eq!(throttle_time_ms, 19);
    assert_eq!(resource_names, ["alpha", "zeta"]);
}

#[test]
fn an_error_cannot_be_bound_to_success_names() {
    assert!(matches!(
        normalized_input(0, 7, vec!["orders".to_owned()]),
        ListClientMetricsResourcesInput::InvalidResponse
    ));
}
