//! Linear normalized-input transfer between settled and pending values.

use core::num::NonZeroI16;

use kafka_client_core::{
    GroupOffsetCommitBrokerError, GroupOffsetCommitInput, GroupOffsetCommitPartitionOutcome,
    OperationId, PartitionIndex, TopicId,
};

use super::group_offset_commit_settlement::{
    RouteTokenDestination, SettledGroupOffsetCommitCall, needs_coordinator_refresh,
    route_token_destination,
};

#[test]
fn settled_input_moves_with_its_exact_pending_confirmation() {
    let operation_id = OperationId::from_raw(31);
    let settled = SettledGroupOffsetCommitCall::new(
        operation_id,
        GroupOffsetCommitInput::InvalidResponse,
        None,
    );
    assert_eq!(settled.operation_id(), operation_id);
    let (input, pending) = settled.into_parts();
    assert_eq!(input, GroupOffsetCommitInput::InvalidResponse);
    assert_eq!(pending.operation_id(), operation_id);
    let settled = pending.into_settled(input);
    assert_eq!(settled.operation_id(), operation_id);
}

#[test]
fn only_coordinator_invalidating_commit_codes_require_route_refresh() {
    for code in [15, 16] {
        assert!(needs_coordinator_refresh(&broker_rejection(code)));
        assert_eq!(
            route_token_destination(&broker_rejection(code)),
            RouteTokenDestination::Refresh
        );
    }
    for code in [14, 22, 25, 27] {
        assert!(!needs_coordinator_refresh(&broker_rejection(code)));
        assert_eq!(
            route_token_destination(&broker_rejection(code)),
            RouteTokenDestination::Confirm
        );
    }
    assert!(!needs_coordinator_refresh(
        &GroupOffsetCommitInput::InvalidResponse
    ));
    assert_eq!(
        route_token_destination(&GroupOffsetCommitInput::InvalidResponse),
        RouteTokenDestination::Confirm
    );
}

pub(super) fn broker_rejection(code: i16) -> GroupOffsetCommitInput {
    let error = NonZeroI16::new(code).unwrap_or_else(|| panic!("nonzero broker error"));
    GroupOffsetCommitInput::BrokerResponded {
        throttle_time_ms: 0,
        outcomes: vec![GroupOffsetCommitPartitionOutcome::rejected(
            TopicId::from_raw(1),
            PartitionIndex::from_raw(0),
            GroupOffsetCommitBrokerError::new(error),
        )],
    }
}
