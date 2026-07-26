//! Settled and pending route-token owner identity scenarios.

use super::{
    calls::TrackedGroupPositionOffsetFetchCalls,
    calls_test::{fence, key},
    settlement::GroupPositionOffsetFetchPoll,
};

#[test]
fn poll_reports_the_exact_settled_and_pending_fence_without_moving_owners() {
    let mut calls = TrackedGroupPositionOffsetFetchCalls::new(8);
    calls.install_terminal_for_test(
        key(6, 100),
        Some(9),
        Ok(kafka_wire::OffsetFetchResponse::default()),
    );
    assert_eq!(
        calls.poll_group_position_offset_fetch(),
        Ok(GroupPositionOffsetFetchPoll::TerminalReady { fence: fence(6) })
    );
    let accepted =
        super::admission::GroupPositionOffsetFetchAccepted::from_fence_for_test(fence(6));
    let terminal = calls
        .begin_group_position_offset_fetch_settlement(&accepted)
        .unwrap_or_else(|error| panic!("begin settlement: {error:?}"));
    assert_eq!(
        calls.poll_group_position_offset_fetch(),
        Ok(GroupPositionOffsetFetchPoll::ConfirmationPending { fence: fence(6) })
    );
    calls
        .restore_group_position_offset_fetch_settlement(terminal)
        .unwrap_or_else(|failure| {
            let (_terminal, error) = failure.into_parts();
            panic!("restore settlement: {error:?}");
        });
    assert_eq!(
        calls.poll_group_position_offset_fetch(),
        Ok(GroupPositionOffsetFetchPoll::TerminalReady { fence: fence(6) })
    );
}
