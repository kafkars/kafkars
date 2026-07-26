//! Accepted receipt and lossless returned-request outcome scenarios.

use kafka_client_core::GroupPositionBootstrapInput;

use super::{
    admission::{
        GroupPositionOffsetFetchAccepted, GroupPositionOffsetFetchReturn,
        GroupPositionOffsetFetchReturnReason,
    },
    calls_test::{fence, key, request},
};

#[test]
fn accepted_receipt_produces_only_the_exact_driver_accepted_fact() {
    let accepted = GroupPositionOffsetFetchAccepted::from_fence_for_test(fence(13));
    assert_eq!(accepted.fence(), fence(13));
    assert_eq!(
        accepted.driver_accepted(),
        GroupPositionBootstrapInput::DriverAccepted { fence: fence(13) }
    );
    accepted.confirm_receipt();
}

#[test]
fn returned_request_preserves_key_request_charge_and_reason() {
    let expected_charge = request().retained_bytes();
    let returned = GroupPositionOffsetFetchReturn::new(
        key(14, 241),
        request(),
        GroupPositionOffsetFetchReturnReason::Capacity { limit: 8 },
    );
    let (returned_key, returned_request, reason) = returned.into_parts();
    assert_eq!(returned_key.fence(), fence(14));
    assert_eq!(returned_request.retained_bytes(), expected_charge);
    assert_eq!(
        reason,
        GroupPositionOffsetFetchReturnReason::Capacity { limit: 8 }
    );
}
