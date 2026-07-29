//! Exact broker error and complete feature-description translation scenarios.

use kafka_client_core::DescribeFeaturesInput;

use super::response::normalized_input;

#[test]
fn signed_nonzero_broker_error_preserves_throttle_and_code() {
    let DescribeFeaturesInput::BrokerRejected { error } =
        normalized_input(23, -32_000, Vec::new(), true, None, Vec::new(), false)
    else {
        panic!("broker rejection expected");
    };

    assert_eq!(error.throttle_time_ms(), 23);
    assert_eq!(error.code(), -32_000);
}

#[test]
fn successful_empty_description_preserves_version_completeness_and_migration_fact() {
    let DescribeFeaturesInput::BrokerResponded { description } =
        normalized_input(19, 0, Vec::new(), false, Some(7), Vec::new(), true)
    else {
        panic!("broker response expected");
    };

    assert_eq!(description.throttle_time_ms(), 19);
    assert!(!description.supported_features_complete());
    assert_eq!(description.finalized_features_epoch(), Some(7));
    assert!(description.zk_migration_ready());
}

#[test]
fn an_error_cannot_be_bound_to_feature_payload() {
    assert!(matches!(
        normalized_input(0, 7, Vec::new(), true, Some(1), Vec::new(), false),
        DescribeFeaturesInput::InvalidResponse
    ));
}
