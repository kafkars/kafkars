//! Raw selected-version, response, and exact-key retention scenarios.

use kafka_client_core::Deadline;

use super::{
    calls::TrackedGroupPositionOffsetFetchCalls,
    calls_test::{fence, key},
};

#[test]
fn raw_terminal_preserves_key_selected_version_and_uninterpreted_response() {
    let mut calls = TrackedGroupPositionOffsetFetchCalls::new(8);
    let mut response = kafka_wire::OffsetFetchResponse::default();
    response.throttle_time_ms = 17;
    calls.install_terminal_for_test(key(5, 151), Some(8), Ok(response));
    let accepted =
        super::admission::GroupPositionOffsetFetchAccepted::from_fence_for_test(fence(5));
    let terminal = calls
        .begin_group_position_offset_fetch_settlement(&accepted)
        .unwrap_or_else(|error| panic!("begin terminal: {error:?}"));

    assert_eq!(terminal.key().fence(), fence(5));
    assert_eq!(
        terminal.key().operation_deadline().core(),
        Deadline::from_tick(151)
    );
    assert_eq!(terminal.selected_version(), Some(8));
    let response = terminal
        .result()
        .as_ref()
        .unwrap_or_else(|error| panic!("raw response: {error}"));
    assert_eq!(response.throttle_time_ms, 17);
    let (returned_key, selected_version, returned_result) = terminal.into_parts();
    assert_eq!(returned_key.fence(), fence(5));
    assert_eq!(selected_version, Some(8));
    assert_eq!(
        returned_result
            .unwrap_or_else(|error| panic!("owned raw response: {error}"))
            .throttle_time_ms,
        17
    );
}
