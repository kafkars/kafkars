//! Linear controller-route refresh preparation scenarios for reassignment calls.

use super::reassignment_controller_refresh::{
    ReassignmentControllerRefresh, ReassignmentControllerRefreshPrepareError,
};

#[test]
fn ordinary_terminals_retain_optional_route_evidence_until_settlement() {
    let mut refresh = ReassignmentControllerRefresh::unclassified(None);
    assert_eq!(refresh.prepare(false), Ok(()));
    assert!(!refresh.is_pending());
    assert_eq!(
        refresh.prepare(false),
        Err(ReassignmentControllerRefreshPrepareError::AlreadyPrepared)
    );
}

#[test]
fn causal_refresh_requires_the_exact_controller_route_token() {
    let mut refresh = ReassignmentControllerRefresh::unclassified(None);
    assert_eq!(
        refresh.prepare(true),
        Err(ReassignmentControllerRefreshPrepareError::MissingRouteToken)
    );
    assert!(!refresh.is_pending());
    assert_eq!(
        refresh.prepare(false),
        Ok(()),
        "a failed required preparation must retain the unclassified state"
    );
}
