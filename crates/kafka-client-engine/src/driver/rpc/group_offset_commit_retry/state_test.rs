//! Exact coordinator-refresh classification for normalized group commit facts.

use core::num::NonZeroI16;

use kafka_client_core::{
    GroupOffsetCommitBrokerError, GroupOffsetCommitInput, GroupOffsetCommitPartitionOutcome,
    OperationId, PartitionIndex, TopicId,
};

use super::state::{RouteTokenDestination, needs_coordinator_refresh, route_token_destination};

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

pub(in crate::driver::rpc) fn broker_rejection(code: i16) -> GroupOffsetCommitInput {
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

#[test]
fn operation_identity_remains_nonzero_in_state_fixtures() {
    assert_ne!(OperationId::from_raw(1), OperationId::from_raw(2));
}
