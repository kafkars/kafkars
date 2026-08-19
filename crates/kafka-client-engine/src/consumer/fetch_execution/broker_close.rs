//! Bounded final-epoch execution for every broker-owned Fetch session.

use kafka_client_core::Moment;

use crate::{
    clock::MonotonicClock,
    driver::{BrokerFetchCloseCall, DriverOwner},
};

use super::{
    broker_session::BrokerSessionPlan,
    executor::DirectFetchExecutor,
    fault::{FetchExecutionError, RetainedFetchFault},
};

#[derive(Clone, Copy)]
pub(super) struct BrokerSessionPolicy {
    pub(super) settings: crate::protocol::fetch::FetchRequestSettings,
    pub(super) timeout: std::time::Duration,
}

pub(super) struct ActiveBrokerSessionClose {
    pub(super) plan: BrokerSessionPlan,
    pub(super) call: BrokerFetchCloseCall,
}

impl DirectFetchExecutor {
    pub(crate) fn request_broker_session_close(&mut self) {
        self.broker_maintenance_deferred = false;
        self.broker_close_requested = true;
    }

    pub(crate) const fn broker_session_close_requested(&self) -> bool {
        self.broker_close_requested
    }

    pub(crate) fn broker_session_close_deadline(&self) -> Option<kafka_client_core::Deadline> {
        self.broker_close_deadline
            .map(crate::clock::OperationDeadline::core)
    }

    pub(crate) fn drive_broker_session_close(
        &mut self,
        driver: &DriverOwner,
        clock: &MonotonicClock,
        now: Moment,
    ) -> Result<bool, FetchExecutionError> {
        if self.fault.is_some() {
            return Err(FetchExecutionError::Faulted);
        }
        if let Some(active) = &mut self.active_broker_close {
            return match active.call.poll() {
                Ok(false) => Ok(false),
                Ok(true) => self.complete_active_broker_close(),
                Err(_error) => {
                    self.fault = Some(RetainedFetchFault::Registry);
                    Err(FetchExecutionError::BrokerSession)
                }
            };
        }
        if !self.ordinary_fetch_work_is_idle() {
            return Ok(false);
        }
        let Some(broker_id) = self
            .broker_sessions
            .as_ref()
            .and_then(super::broker_session::BrokerFetchSessions::first_broker_id)
        else {
            self.broker_close_requested = false;
            self.broker_close_deadline = None;
            return Ok(false);
        };
        let Some(policy) = self.broker_session_policy else {
            return Err(FetchExecutionError::BrokerSession);
        };
        if self.broker_close_deadline.is_none() {
            let capture = clock
                .capture_deadline_after(policy.timeout)
                .map_err(|_error| FetchExecutionError::BrokerSession)?;
            self.broker_close_deadline = Some(capture.operation_deadline());
        }
        let deadline = self
            .broker_close_deadline
            .unwrap_or_else(|| unreachable!("close deadline was installed"));
        if deadline.core().is_elapsed_at(now) {
            self.broker_sessions
                .as_mut()
                .unwrap_or_else(|| unreachable!("broker ID requires session owner"))
                .discard_all();
            self.broker_close_requested = false;
            self.broker_close_deadline = None;
            return Ok(true);
        }
        let plan = self
            .broker_sessions
            .as_mut()
            .unwrap_or_else(|| unreachable!("broker ID requires session owner"))
            .try_begin_close(broker_id)
            .map_err(|_error| FetchExecutionError::BrokerSession)?;
        let Some(plan) = plan else {
            if self.broker_sessions_are_closed() {
                self.broker_close_requested = false;
                self.broker_close_deadline = None;
            }
            return Ok(true);
        };
        match BrokerFetchCloseCall::submit(
            driver,
            broker_id,
            policy.settings,
            plan.session(),
            deadline,
        ) {
            Ok(call) => {
                self.active_broker_close = Some(ActiveBrokerSessionClose { plan, call });
                Ok(true)
            }
            Err(error) if error.is_backpressured() => {
                self.broker_sessions
                    .as_mut()
                    .unwrap_or_else(|| unreachable!("close plan requires session owner"))
                    .abort(plan, false)
                    .map_err(|_error| FetchExecutionError::BrokerSession)?;
                Ok(false)
            }
            Err(_error) => {
                self.broker_sessions
                    .as_mut()
                    .unwrap_or_else(|| unreachable!("close plan requires session owner"))
                    .complete_close(plan)
                    .map_err(|_error| FetchExecutionError::BrokerSession)?;
                Ok(true)
            }
        }
    }

    fn complete_active_broker_close(&mut self) -> Result<bool, FetchExecutionError> {
        let active = self
            .active_broker_close
            .take()
            .unwrap_or_else(|| unreachable!("polled close remains active"));
        self.broker_sessions
            .as_mut()
            .unwrap_or_else(|| unreachable!("active close requires session owner"))
            .complete_close(active.plan)
            .map_err(|_error| FetchExecutionError::BrokerSession)?;
        if self.broker_sessions_are_closed() {
            self.broker_close_requested = false;
            self.broker_close_deadline = None;
        }
        Ok(true)
    }

    fn ordinary_fetch_work_is_idle(&self) -> bool {
        self.calls.retained_count() == 0
            && self.broker_calls.retained_count() == 0
            && self.route_calls.is_empty()
            && self.routed.is_empty()
            && self.active_broker_sessions.is_empty()
            && self.broker_maintenance.is_none()
            && self.store.retained() == (0, 0)
    }
}
