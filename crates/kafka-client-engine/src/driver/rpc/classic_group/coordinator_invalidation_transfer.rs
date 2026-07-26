//! Exact transfer of pending membership route tokens into rediscovery ownership.

use kafka_driver::RouteKind;

use super::{
    coordinator_invalidation::PendingClassicCoordinatorInvalidation,
    heartbeat_calls::{AcceptedClassicHeartbeatCall, TrackedClassicHeartbeatCalls},
    heartbeat_settlement::{
        ClassicHeartbeatConfirmationError, ClassicHeartbeatConfirmationFailure,
    },
    join_group_calls::{AcceptedJoinGroupCall, TrackedJoinGroupCalls},
    join_group_settlement::{JoinGroupConfirmationError, JoinGroupConfirmationFailure},
    sync_group_calls::{AcceptedSyncGroupCall, TrackedSyncGroupCalls},
    sync_group_settlement::{SyncGroupConfirmationError, SyncGroupConfirmationFailure},
};

impl TrackedJoinGroupCalls {
    pub(crate) fn extract_join_group_rediscovery(
        &mut self,
        accepted: AcceptedJoinGroupCall,
    ) -> Result<PendingClassicCoordinatorInvalidation, JoinGroupConfirmationFailure> {
        let supplied = accepted.key();
        let Some(pending) = self.pending_confirmation.as_ref() else {
            return Err(JoinGroupConfirmationFailure::new(
                accepted,
                JoinGroupConfirmationError::NoPending { supplied },
            ));
        };
        if pending.key() != supplied {
            return Err(JoinGroupConfirmationFailure::new(
                accepted,
                JoinGroupConfirmationError::KeyMismatch {
                    pending: pending.key(),
                    supplied,
                },
            ));
        }
        match pending.route_token_kind() {
            None => {
                return Err(JoinGroupConfirmationFailure::new(
                    accepted,
                    JoinGroupConfirmationError::RouteTokenUnavailable {
                        pending: pending.key(),
                    },
                ));
            }
            Some(RouteKind::Coordinator) => {}
            Some(observed) => {
                return Err(JoinGroupConfirmationFailure::new(
                    accepted,
                    JoinGroupConfirmationError::RouteTokenKind {
                        pending: pending.key(),
                        observed,
                    },
                ));
            }
        }
        let Some(pending) = self.pending_confirmation.take() else {
            return Err(JoinGroupConfirmationFailure::new(
                accepted,
                JoinGroupConfirmationError::NoPending { supplied },
            ));
        };
        let route_token = match pending.into_rediscovery_route_token() {
            Ok(route_token) => route_token,
            Err(pending) => {
                self.pending_confirmation = Some(pending);
                return Err(JoinGroupConfirmationFailure::new(
                    accepted,
                    JoinGroupConfirmationError::RouteTokenUnavailable { pending: supplied },
                ));
            }
        };
        accepted.confirm_join_group_call_receipt();
        Ok(PendingClassicCoordinatorInvalidation::new(
            supplied.group_id(),
            route_token,
        ))
    }
}

impl TrackedSyncGroupCalls {
    pub(crate) fn extract_sync_group_rediscovery(
        &mut self,
        accepted: AcceptedSyncGroupCall,
    ) -> Result<PendingClassicCoordinatorInvalidation, SyncGroupConfirmationFailure> {
        let supplied = accepted.key();
        let Some(pending) = self.pending_confirmation.as_ref() else {
            return Err(SyncGroupConfirmationFailure::new(
                accepted,
                SyncGroupConfirmationError::NoPending { supplied },
            ));
        };
        if pending.key() != supplied {
            return Err(SyncGroupConfirmationFailure::new(
                accepted,
                SyncGroupConfirmationError::KeyMismatch {
                    pending: pending.key(),
                    supplied,
                },
            ));
        }
        match pending.route_token_kind() {
            None => {
                return Err(SyncGroupConfirmationFailure::new(
                    accepted,
                    SyncGroupConfirmationError::RouteTokenUnavailable {
                        pending: pending.key(),
                    },
                ));
            }
            Some(RouteKind::Coordinator) => {}
            Some(observed) => {
                return Err(SyncGroupConfirmationFailure::new(
                    accepted,
                    SyncGroupConfirmationError::RouteTokenKind {
                        pending: pending.key(),
                        observed,
                    },
                ));
            }
        }
        let Some(pending) = self.pending_confirmation.take() else {
            return Err(SyncGroupConfirmationFailure::new(
                accepted,
                SyncGroupConfirmationError::NoPending { supplied },
            ));
        };
        let route_token = match pending.into_rediscovery_route_token() {
            Ok(route_token) => route_token,
            Err(pending) => {
                self.pending_confirmation = Some(pending);
                return Err(SyncGroupConfirmationFailure::new(
                    accepted,
                    SyncGroupConfirmationError::RouteTokenUnavailable { pending: supplied },
                ));
            }
        };
        accepted.confirm_sync_group_call_receipt();
        Ok(PendingClassicCoordinatorInvalidation::new(
            supplied.group_id(),
            route_token,
        ))
    }
}

impl TrackedClassicHeartbeatCalls {
    #[expect(
        clippy::result_large_err,
        reason = "failure restores the exact accepted linear call without another allocation"
    )]
    pub(crate) fn extract_classic_heartbeat_rediscovery(
        &mut self,
        accepted: AcceptedClassicHeartbeatCall,
    ) -> Result<PendingClassicCoordinatorInvalidation, ClassicHeartbeatConfirmationFailure> {
        let supplied = accepted.key();
        let Some(pending) = self.pending_confirmation.as_ref() else {
            return Err(ClassicHeartbeatConfirmationFailure::new(
                accepted,
                ClassicHeartbeatConfirmationError::NoPending { supplied },
            ));
        };
        if pending.key() != supplied {
            return Err(ClassicHeartbeatConfirmationFailure::new(
                accepted,
                ClassicHeartbeatConfirmationError::KeyMismatch {
                    pending: pending.key(),
                    supplied,
                },
            ));
        }
        match pending.route_token_kind() {
            None => {
                return Err(ClassicHeartbeatConfirmationFailure::new(
                    accepted,
                    ClassicHeartbeatConfirmationError::RouteTokenUnavailable {
                        pending: pending.key(),
                    },
                ));
            }
            Some(RouteKind::Coordinator) => {}
            Some(observed) => {
                return Err(ClassicHeartbeatConfirmationFailure::new(
                    accepted,
                    ClassicHeartbeatConfirmationError::RouteTokenKind {
                        pending: pending.key(),
                        observed,
                    },
                ));
            }
        }
        let Some(pending) = self.pending_confirmation.take() else {
            return Err(ClassicHeartbeatConfirmationFailure::new(
                accepted,
                ClassicHeartbeatConfirmationError::NoPending { supplied },
            ));
        };
        let route_token = match pending.into_rediscovery_route_token() {
            Ok(route_token) => route_token,
            Err(pending) => {
                self.pending_confirmation = Some(pending);
                return Err(ClassicHeartbeatConfirmationFailure::new(
                    accepted,
                    ClassicHeartbeatConfirmationError::RouteTokenUnavailable { pending: supplied },
                ));
            }
        };
        accepted.confirm_classic_heartbeat_call_receipt();
        Ok(PendingClassicCoordinatorInvalidation::new(
            supplied.group_id(),
            route_token,
        ))
    }
}
