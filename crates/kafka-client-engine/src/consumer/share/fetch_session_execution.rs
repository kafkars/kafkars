//! Exact prepared-to-driver handoff and retained `ShareFetch` terminal ownership.

use kafka_client_core::{DeliveryStatus, ShareFetchAttempt};

use crate::driver::{
    DriverOwner, ShareFetchCall, ShareFetchCompletionErrorKind, ShareFetchDriverSubmitErrorKind,
    ShareFetchResolution, ShareFetchRoute, ShareFetchTerminalContext,
};

use super::fetch_session::{ShareFetchSessionOwner, ShareFetchSessionOwnerError};

#[must_use = "an active ShareFetch call must settle or recover after driver shutdown"]
pub(super) struct ActiveShareFetchCall {
    attempt: ShareFetchAttempt,
    call: ShareFetchCall,
}

#[must_use = "a ShareFetch terminal must enter session policy exactly once"]
pub(super) struct ShareFetchSessionTerminal {
    pub(super) attempt: ShareFetchAttempt,
    pub(super) resolution: ShareFetchResolution,
    pub(super) route: ShareFetchRoute,
    pub(super) context: ShareFetchTerminalContext,
}

impl ShareFetchSessionOwner {
    pub(super) const fn has_active_call(&self) -> bool {
        self.active.is_some()
    }

    pub(super) fn submit_prepared(
        &mut self,
        driver: &DriverOwner,
        now: kafka_client_core::Moment,
    ) -> Result<ShareFetchSubmissionTurn, ShareFetchExecutionError> {
        if self.active.is_some() || self.terminal.is_some() {
            return Err(ShareFetchExecutionError::Occupied);
        }
        let prepared = self
            .take_prepared()
            .ok_or(ShareFetchExecutionError::NotPrepared)?;
        let (attempt, request, capture) = prepared.into_parts();
        match ShareFetchCall::submit(
            driver,
            attempt.fence().broker_id(),
            request,
            capture.now(),
            capture.operation_deadline(),
        ) {
            Ok(call) => {
                self.active = Some(ActiveShareFetchCall { attempt, call });
                Ok(ShareFetchSubmissionTurn::Submitted)
            }
            Err(failure) => {
                let kind = failure.kind();
                drop(failure.into_evidence());
                self.settle_attempt_failure(attempt, DeliveryStatus::NotSent)
                    .map_err(ShareFetchExecutionError::Session)?;
                if kind == ShareFetchDriverSubmitErrorKind::Full {
                    self.prepare_next_at(capture, now)
                        .map_err(ShareFetchExecutionError::Session)?;
                    Ok(ShareFetchSubmissionTurn::Backpressured)
                } else {
                    Err(ShareFetchExecutionError::Submit(kind))
                }
            }
        }
    }

    pub(super) fn poll_execution(
        &mut self,
    ) -> Result<ShareFetchExecutionPoll, ShareFetchExecutionError> {
        if self.terminal.is_some() {
            return Ok(ShareFetchExecutionPoll::Terminal);
        }
        let Some(active) = self.active.as_mut() else {
            return Err(ShareFetchExecutionError::NotActive);
        };
        let Some(terminal) = active.call.try_terminal() else {
            return Ok(ShareFetchExecutionPoll::Pending);
        };
        let active = self
            .active
            .take()
            .unwrap_or_else(|| unreachable!("polled ShareFetch call remains active"));
        match terminal {
            Ok(raw) => {
                let (resolution, route, context) = raw.into_resolution(self.response_limits());
                if route.broker_id() != active.attempt.fence().broker_id()
                    || context.broker_id != active.attempt.fence().broker_id()
                {
                    route.accept();
                    self.settle_attempt_failure(active.attempt, DeliveryStatus::PossiblySent)
                        .map_err(ShareFetchExecutionError::Session)?;
                    return Err(ShareFetchExecutionError::BrokerMismatch);
                }
                self.terminal = Some(ShareFetchSessionTerminal {
                    attempt: active.attempt,
                    resolution,
                    route,
                    context,
                });
                Ok(ShareFetchExecutionPoll::Terminal)
            }
            Err(failure) => {
                let (evidence, kind) = failure.into_parts();
                drop(evidence);
                self.settle_attempt_failure(active.attempt, DeliveryStatus::PossiblySent)
                    .map_err(ShareFetchExecutionError::Session)?;
                Err(ShareFetchExecutionError::Completion(kind))
            }
        }
    }

    pub(super) fn take_terminal(&mut self) -> Option<ShareFetchSessionTerminal> {
        self.terminal.take()
    }

    pub(super) fn recover_call_after_driver_shutdown(
        &mut self,
    ) -> Result<bool, ShareFetchExecutionError> {
        let Some(active) = self.active.take() else {
            return Ok(false);
        };
        drop(active.call.recover_after_driver_shutdown().into_evidence());
        self.settle_attempt_failure(active.attempt, DeliveryStatus::PossiblySent)
            .map_err(ShareFetchExecutionError::Session)?;
        Ok(true)
    }

    pub(super) fn release_unsubmitted(mut self) -> Result<(), ShareFetchExecutionError> {
        if self.active.is_some() || self.terminal.is_some() || self.staged.is_some() {
            return Err(ShareFetchExecutionError::Occupied);
        }
        if let Some(prepared) = self.take_prepared() {
            self.settle_unsubmitted(prepared)
                .map_err(ShareFetchExecutionError::Session)?;
        }
        Ok(())
    }

    pub(super) fn discard_terminal(&mut self) -> Result<bool, ShareFetchExecutionError> {
        let Some(terminal) = self.take_terminal() else {
            return Ok(false);
        };
        terminal.route.accept();
        self.settle_attempt_failure(terminal.attempt, DeliveryStatus::PossiblySent)
            .map_err(ShareFetchExecutionError::Session)?;
        Ok(true)
    }
}

impl ActiveShareFetchCall {
    pub(super) const fn deadline(&self) -> kafka_client_core::Deadline {
        self.attempt.deadline()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ShareFetchSubmissionTurn {
    Submitted,
    Backpressured,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ShareFetchExecutionPoll {
    Pending,
    Terminal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ShareFetchExecutionError {
    Occupied,
    NotPrepared,
    NotActive,
    BrokerMismatch,
    Submit(ShareFetchDriverSubmitErrorKind),
    Completion(ShareFetchCompletionErrorKind),
    Session(ShareFetchSessionOwnerError),
}
