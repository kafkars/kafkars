//! Response correlation scenarios for Admin `DeleteConsumerGroups`.

use kafka_client_core::{DeleteConsumerGroupsResult, DeleteConsumerGroupsTarget};
use kafka_wire::{DeleteGroupsResponse, delete_groups_response::DeletableGroupResult};

use super::{DeleteConsumerGroupsResponseFailure, normalize_delete_consumer_groups_response};

#[test]
fn response_preserves_success_throttle_and_exact_broker_error() {
    let target = DeleteConsumerGroupsTarget::new("orders-workers".to_owned());
    let normalized = normalize_delete_consumer_groups_response(
        &target,
        3,
        &response(17, "orders-workers", 0, None),
        usize::MAX,
    )
    .unwrap_or_else(|error| panic!("response must normalize: {error:?}"));
    assert_eq!(normalized.throttle_time_ms(), 17);
    assert!(matches!(
        normalized.outcome().result(),
        DeleteConsumerGroupsResult::Deleted
    ));

    let normalized = normalize_delete_consumer_groups_response(
        &target,
        0,
        &response(
            0,
            "orders-workers",
            -31_999,
            Some("coordinator rejected deletion"),
        ),
        usize::MAX,
    )
    .unwrap_or_else(|error| panic!("broker error must normalize: {error:?}"));
    let DeleteConsumerGroupsResult::Failed(error) = normalized.outcome().result() else {
        panic!("expected broker failure");
    };
    assert_eq!(error.code(), -31_999);
    assert_eq!(error.message(), Some("coordinator rejected deletion"));
    assert!(!error.message_truncated());
}

#[test]
fn response_bounds_diagnostic_at_a_utf8_boundary() {
    let target = DeleteConsumerGroupsTarget::new("orders-workers".to_owned());
    let diagnostic = format!("{}é-tail", "x".repeat(1023));
    let normalized = normalize_delete_consumer_groups_response(
        &target,
        3,
        &response(0, "orders-workers", -31_998, Some(&diagnostic)),
        usize::MAX,
    )
    .unwrap_or_else(|error| panic!("bounded diagnostic must normalize: {error:?}"));
    let DeleteConsumerGroupsResult::Failed(error) = normalized.outcome().result() else {
        panic!("expected broker failure");
    };
    assert_eq!(error.message(), Some("x".repeat(1023).as_str()));
    assert!(error.message_truncated());
}

#[test]
fn response_rejects_uncorrelated_group() {
    let target = DeleteConsumerGroupsTarget::new("orders-workers".to_owned());
    assert_eq!(
        normalize_delete_consumer_groups_response(
            &target,
            2,
            &response(0, "other-workers", 0, None),
            usize::MAX,
        ),
        Err(DeleteConsumerGroupsResponseFailure::UnexpectedGroup)
    );
}

#[test]
fn response_rejects_a_correlated_result_beyond_its_capacity() {
    let target = DeleteConsumerGroupsTarget::new("orders-workers".to_owned());
    assert_eq!(
        normalize_delete_consumer_groups_response(
            &target,
            3,
            &response(0, "orders-workers", 0, None),
            target.group_id().len() - 1,
        ),
        Err(DeleteConsumerGroupsResponseFailure::RetainedBytes)
    );
}

fn response(
    throttle_time_ms: i32,
    group_id: &str,
    error_code: i16,
    error_message: Option<&str>,
) -> DeleteGroupsResponse {
    let mut result = DeletableGroupResult::default();
    result.group_id = group_id.into();
    result.error_code = error_code;
    result.error_message = error_message.map(Into::into);
    let mut response = DeleteGroupsResponse::default();
    response.throttle_time_ms = throttle_time_ms;
    response.results = vec![result];
    response
}
