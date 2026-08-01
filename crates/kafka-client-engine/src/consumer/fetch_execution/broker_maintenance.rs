//! Deadline-bound execution of one broker session's forgotten-only epoch.

use kafka_client_core::Moment;

use crate::{
    clock::MonotonicClock,
    driver::{
        DriverOwner, ForgottenFetchRequest, ForgottenFetchSubmitFailureKind,
        TrackedForgottenFetchCall,
    },
    protocol::fetch::OwnedForgottenFetchPartition,
};

use super::{
    broker_maintenance_state::BrokerSessionMaintenance,
    broker_session::BrokerSessionPlan,
    executor::DirectFetchExecutor,
    fault::{FetchExecutionError, RetainedFetchFault},
};

impl DirectFetchExecutor {
    pub(super) fn drive_forgotten_maintenance(
        &mut self,
        driver: &DriverOwner,
        clock: &MonotonicClock,
        now: Moment,
    ) -> Result<bool, FetchExecutionError> {
        if self.fault.is_some() {
            return Err(FetchExecutionError::Faulted);
        }
        if let Some(maintenance) = self.broker_maintenance.take() {
            return self.drive_retained_maintenance(driver, maintenance, now);
        }
        if self.broker_maintenance_deferred {
            return Ok(false);
        }
        if self.broker_close_requested {
            return Ok(false);
        }
        if self.broker_calls.retained_count() != 0 || !self.broker_calls.has_admission_capacity() {
            return Ok(false);
        }
        let sessions = self
            .broker_sessions
            .as_ref()
            .unwrap_or_else(|| unreachable!("broker maintenance requires session owner"));
        if !sessions.has_forgotten_ready() {
            return Ok(false);
        }
        let policy = self
            .broker_session_policy
            .ok_or(FetchExecutionError::BrokerSession)?;
        let deadline = clock
            .capture_deadline_after(policy.timeout)
            .map_err(|_error| FetchExecutionError::BrokerSession)?
            .operation_deadline();
        let plan = self
            .broker_sessions
            .as_mut()
            .unwrap_or_else(|| unreachable!("ready maintenance requires session owner"))
            .try_begin_forgotten()
            .map_err(|_error| FetchExecutionError::BrokerSession)?
            .unwrap_or_else(|| unreachable!("ready forgotten session must produce a plan"));
        let request = match request_from_plan(&plan, policy.settings, deadline) {
            Ok(request) => request,
            Err(()) => {
                self.broker_sessions
                    .as_mut()
                    .unwrap_or_else(|| unreachable!("maintenance plan requires session owner"))
                    .abort(plan, false)
                    .map_err(|_error| FetchExecutionError::BrokerSession)?;
                return Err(FetchExecutionError::BrokerSession);
            }
        };
        self.submit_forgotten_maintenance(driver, plan, request, now)
    }

    fn drive_retained_maintenance(
        &mut self,
        driver: &DriverOwner,
        maintenance: BrokerSessionMaintenance,
        now: Moment,
    ) -> Result<bool, FetchExecutionError> {
        match maintenance {
            BrokerSessionMaintenance::Prepared { plan, request } => {
                if self.broker_close_requested {
                    self.abort_maintenance_plan(plan, false)?;
                    return Ok(true);
                }
                self.submit_forgotten_maintenance(driver, plan, request, now)
            }
            BrokerSessionMaintenance::Active { plan, mut call } => {
                let Some(terminal) = call.try_terminal(now) else {
                    self.broker_maintenance = Some(BrokerSessionMaintenance::Active { plan, call });
                    return Ok(false);
                };
                match terminal {
                    Ok(terminal) => self.settle_forgotten_terminal(plan, terminal),
                    Err(failure) => {
                        self.broker_maintenance = Some(BrokerSessionMaintenance::CompletionFault {
                            _plan: plan,
                            failure,
                        });
                        self.fault = Some(RetainedFetchFault::Registry);
                        Err(FetchExecutionError::BrokerSession)
                    }
                }
            }
            maintenance @ (BrokerSessionMaintenance::CompletionFault { .. }
            | BrokerSessionMaintenance::ConfirmationFault { .. }
            | BrokerSessionMaintenance::RequestFault { .. }) => {
                self.broker_maintenance = Some(maintenance);
                self.fault = Some(RetainedFetchFault::Registry);
                Err(FetchExecutionError::Faulted)
            }
        }
    }

    fn submit_forgotten_maintenance(
        &mut self,
        driver: &DriverOwner,
        plan: BrokerSessionPlan,
        request: ForgottenFetchRequest,
        now: Moment,
    ) -> Result<bool, FetchExecutionError> {
        match TrackedForgottenFetchCall::submit(driver, plan.broker_id(), request, now) {
            Ok(call) => {
                self.broker_maintenance = Some(BrokerSessionMaintenance::Active { plan, call });
                Ok(true)
            }
            Err(failure) => {
                let (request, kind) = failure.into_parts();
                match kind {
                    ForgottenFetchSubmitFailureKind::Backpressured => {
                        self.broker_maintenance =
                            Some(BrokerSessionMaintenance::Prepared { plan, request });
                        Ok(false)
                    }
                    ForgottenFetchSubmitFailureKind::DeadlineElapsed => {
                        self.abort_maintenance_plan(plan, false)?;
                        Ok(true)
                    }
                    ForgottenFetchSubmitFailureKind::DriverRejected
                    | ForgottenFetchSubmitFailureKind::Request => {
                        self.abort_maintenance_plan(plan, true)?;
                        Ok(true)
                    }
                    ForgottenFetchSubmitFailureKind::EmptyForgotten
                    | ForgottenFetchSubmitFailureKind::InvalidSession => {
                        self.abort_maintenance_plan(plan, false)?;
                        self.broker_maintenance =
                            Some(BrokerSessionMaintenance::RequestFault { request });
                        self.fault = Some(RetainedFetchFault::Registry);
                        Err(FetchExecutionError::BrokerSession)
                    }
                }
            }
        }
    }
}

pub(super) fn request_from_plan(
    plan: &BrokerSessionPlan,
    settings: crate::protocol::fetch::FetchRequestSettings,
    deadline: crate::clock::OperationDeadline,
) -> Result<ForgottenFetchRequest, ()> {
    let mut forgotten = Vec::new();
    forgotten
        .try_reserve_exact(plan.forgotten().len())
        .map_err(|_error| ())?;
    forgotten.extend(plan.forgotten().iter().map(|member| {
        OwnedForgottenFetchPartition::new(
            member.topic_owner(),
            member.position().partition().partition().get(),
        )
    }));
    Ok(ForgottenFetchRequest::new(
        settings,
        plan.session(),
        deadline,
        forgotten,
    ))
}
