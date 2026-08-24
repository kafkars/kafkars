//! Route-loss and driver-shutdown recovery for every exact session-set owner.

use kafka_client_core::Moment;

use crate::driver::{DriverOwner, ShareFetchRouteRefresh, ShareFetchRouteRefreshPoll};

use super::{
    super::fetch_session_execution::ShareFetchExecutionError,
    owner::{ShareFetchSessionSet, release_unsubmitted},
};

impl ShareFetchSessionSet {
    pub(in crate::consumer::share) fn retain_recovery(
        &mut self,
        recovery: ShareFetchSessionRecovery,
    ) -> Result<(), ShareFetchExecutionError> {
        if self.recovery.is_some() {
            return Err(ShareFetchExecutionError::Occupied);
        }
        self.recovery = Some(recovery);
        Ok(())
    }

    pub(in crate::consumer::share) const fn is_recovering(&self) -> bool {
        matches!(
            self.recovery.as_ref(),
            Some(ShareFetchSessionRecovery::Route(_) | ShareFetchSessionRecovery::Ready)
        )
    }

    pub(in crate::consumer::share) fn poll_recovery(
        &mut self,
        driver: &DriverOwner,
        now: Moment,
    ) -> ShareFetchSessionRecoveryPoll {
        self.recovery
            .as_mut()
            .map_or(ShareFetchSessionRecoveryPoll::Inactive, |recovery| {
                recovery.poll(driver, now)
            })
    }

    pub(in crate::consumer::share) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), ShareFetchExecutionError> {
        if let Some(recovery) = &mut self.recovery {
            recovery.discard_after_driver_shutdown();
        }
        for session in &mut self.sessions {
            let _recovered_acknowledgement =
                session.recover_acknowledgement_after_driver_shutdown()?;
            if session.acknowledgement_terminal.is_some() {
                let outcome = session.settle_acknowledgement_terminal()?;
                session
                    .retain_settled_acknowledgement(outcome)
                    .map_err(|_outcome| ShareFetchExecutionError::Occupied)?;
            }
            let _recovered_prepared =
                session.recover_prepared_acknowledgement_after_driver_shutdown()?;
            let _recovered = session.recover_call_after_driver_shutdown()?;
            let _discarded = session.discard_terminal()?;
            while session
                .retire_one_reclaimable()
                .map_err(ShareFetchExecutionError::Acquisition)?
            {}
        }
        Ok(())
    }

    pub(in crate::consumer::share) fn release_after_driver_shutdown(
        self,
    ) -> Result<(), ShareFetchExecutionError> {
        release_unsubmitted(self.sessions)
    }
}

pub(in crate::consumer::share) enum ShareFetchSessionRecovery {
    Route(ShareFetchRouteRefresh),
    Ready,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::share) enum ShareFetchSessionRecoveryPoll {
    Inactive,
    Progress,
    Pending,
    Ready,
    Failed,
}

impl ShareFetchSessionRecovery {
    pub(in crate::consumer::share) const fn route(refresh: ShareFetchRouteRefresh) -> Self {
        Self::Route(refresh)
    }

    pub(in crate::consumer::share) const fn session() -> Self {
        Self::Ready
    }

    fn poll(&mut self, driver: &DriverOwner, now: Moment) -> ShareFetchSessionRecoveryPoll {
        match self {
            Self::Route(refresh) => match refresh.poll(driver, now) {
                ShareFetchRouteRefreshPoll::Progress => ShareFetchSessionRecoveryPoll::Progress,
                ShareFetchRouteRefreshPoll::Pending => ShareFetchSessionRecoveryPoll::Pending,
                ShareFetchRouteRefreshPoll::Ready => {
                    *self = Self::Ready;
                    ShareFetchSessionRecoveryPoll::Ready
                }
                ShareFetchRouteRefreshPoll::Failed => {
                    *self = Self::Failed;
                    ShareFetchSessionRecoveryPoll::Failed
                }
            },
            Self::Ready => ShareFetchSessionRecoveryPoll::Ready,
            Self::Failed => ShareFetchSessionRecoveryPoll::Failed,
        }
    }

    fn discard_after_driver_shutdown(&mut self) {
        if let Self::Route(refresh) = self {
            refresh.discard_after_driver_shutdown();
        }
        *self = Self::Ready;
    }
}
