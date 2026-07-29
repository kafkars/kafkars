//! Exact broker error and generated-free successful response scenarios.

use kafka_client_core::ListConfigResourcesInput;

use super::response::normalized_iter_input;

#[test]
fn signed_nonzero_broker_error_preserves_throttle_and_code() {
    let ListConfigResourcesInput::BrokerRejected { error } =
        normalized_iter_input(23, -32_000, Vec::<(i8, String)>::new())
    else {
        panic!("broker rejection expected");
    };

    assert_eq!(error.throttle_time_ms(), 23);
    assert_eq!(error.code(), -32_000);
}

#[test]
fn successful_resources_cross_as_generated_free_core_input() {
    let ListConfigResourcesInput::BrokerResponded {
        throttle_time_ms,
        resources,
    } = normalized_iter_input(
        19,
        0,
        vec![(2, "orders".to_owned()), (64, "future".to_owned())],
    )
    else {
        panic!("broker response expected");
    };

    assert_eq!(throttle_time_ms, 19);
    assert_eq!(resources[0].resource_type().code(), 2);
    assert_eq!(resources[0].resource_name(), "orders");
    assert_eq!(resources[1].resource_type().code(), 64);
    assert_eq!(resources[1].resource_name(), "future");
}

#[test]
fn broker_error_cannot_be_bound_to_success_resources() {
    assert!(matches!(
        normalized_iter_input(0, 7, vec![(2, "orders".to_owned())]),
        ListConfigResourcesInput::InvalidResponse
    ));
}

#[test]
fn nonpositive_success_type_is_rejected_before_core() {
    assert!(matches!(
        normalized_iter_input(0, 0, vec![(0, "invalid".to_owned())]),
        ListConfigResourcesInput::InvalidResponse
    ));
}
