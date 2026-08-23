//! Broker-local core session and exact prepared `ShareFetch` ownership.

use std::sync::Arc;

use kafka_client_core::{
    DeliveryStatus, ShareAcquisitionPolicy, ShareFetchAttempt, ShareFetchSessionApplyError,
    ShareFetchSessionFence, ShareFetchSessionMachine, ShareFetchSessionOpenError,
};

use crate::{
    clock::{DeadlineCapture, OperationDeadline},
    protocol::consumer::share_fetch::{
        PreparedShareFetchRequest, ShareFetchRequestFailure, ShareFetchRequestSettings,
        ShareFetchResponseLimits,
    },
};

use super::fetch_plan::{ShareBrokerSessionPlan, ShareFetchSessionRequestPlan};

/// Exact core attempt paired with its generated request and unchanged deadline.
#[must_use = "a prepared ShareFetch session attempt must be submitted or settled"]
pub(super) struct PreparedShareFetchSession {
    attempt: ShareFetchAttempt,
    request: PreparedShareFetchRequest,
    submitted_at: kafka_client_core::Moment,
    deadline: OperationDeadline,
}

impl PreparedShareFetchSession {
    pub(super) fn into_parts(
        self,
    ) -> (
        ShareFetchAttempt,
        PreparedShareFetchRequest,
        kafka_client_core::Moment,
        OperationDeadline,
    ) {
        (self.attempt, self.request, self.submitted_at, self.deadline)
    }
}

/// Closed preparation owner for one broker-local share session.
#[must_use = "a share fetch session must remain hosted until it is closed or lost"]
pub(super) struct ShareFetchSessionOwner {
    machine: ShareFetchSessionMachine,
    request_plan: ShareFetchSessionRequestPlan,
    group: Arc<str>,
    member: Arc<str>,
    settings: ShareFetchRequestSettings,
    response_limits: ShareFetchResponseLimits,
    prepared: Option<PreparedShareFetchSession>,
}

/// Immutable bounded settings captured before one broker session opens.
pub(super) struct ShareFetchSessionConfig {
    group: Arc<str>,
    member: Arc<str>,
    policy: ShareAcquisitionPolicy,
    settings: ShareFetchRequestSettings,
    response_limits: ShareFetchResponseLimits,
}

impl ShareFetchSessionConfig {
    pub(super) const fn new(
        group: Arc<str>,
        member: Arc<str>,
        policy: ShareAcquisitionPolicy,
        settings: ShareFetchRequestSettings,
        response_limits: ShareFetchResponseLimits,
    ) -> Self {
        Self {
            group,
            member,
            policy,
            settings,
            response_limits,
        }
    }
}

impl ShareFetchSessionOwner {
    pub(super) fn try_open(
        plan: ShareBrokerSessionPlan,
        fence: ShareFetchSessionFence,
        config: ShareFetchSessionConfig,
        capture: DeadlineCapture,
    ) -> Result<Self, ShareFetchSessionOwnerError> {
        let (broker_id, assignment, request_plan) = plan.into_parts();
        if broker_id != fence.broker_id() {
            return Err(ShareFetchSessionOwnerError::BrokerMismatch);
        }
        let machine = ShareFetchSessionMachine::try_open(fence, assignment, config.policy)
            .map_err(ShareFetchSessionOwnerError::CoreOpen)?;
        let mut owner = Self {
            machine,
            request_plan,
            group: config.group,
            member: config.member,
            settings: config.settings,
            response_limits: config.response_limits,
            prepared: None,
        };
        owner.prepare_next(capture)?;
        Ok(owner)
    }

    pub(super) fn prepare_next(
        &mut self,
        capture: DeadlineCapture,
    ) -> Result<(), ShareFetchSessionOwnerError> {
        if self.prepared.is_some() {
            return Err(ShareFetchSessionOwnerError::Occupied);
        }
        let request = self
            .request_plan
            .prepare(
                &self.group,
                &self.member,
                self.machine.fence().session_epoch().get(),
                self.settings,
            )
            .map_err(ShareFetchSessionOwnerError::Protocol)?;
        let attempt = self
            .machine
            .prepare_fetch(capture.deadline(), capture.now())
            .map_err(ShareFetchSessionOwnerError::CoreApply)?;
        self.prepared = Some(PreparedShareFetchSession {
            attempt,
            request,
            submitted_at: capture.now(),
            deadline: capture.operation_deadline(),
        });
        Ok(())
    }

    pub(super) fn take_prepared(&mut self) -> Option<PreparedShareFetchSession> {
        self.prepared.take()
    }

    pub(super) fn settle_unsubmitted(
        &mut self,
        prepared: PreparedShareFetchSession,
    ) -> Result<(), ShareFetchSessionOwnerError> {
        let (attempt, request, _submitted_at, _deadline) = prepared.into_parts();
        drop(request);
        self.machine
            .settle_failure(attempt, DeliveryStatus::NotSent)
            .map_err(ShareFetchSessionOwnerError::CoreApply)
    }

    pub(super) const fn machine(&self) -> &ShareFetchSessionMachine {
        &self.machine
    }

    pub(super) const fn response_limits(&self) -> ShareFetchResponseLimits {
        self.response_limits
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ShareFetchSessionOwnerError {
    BrokerMismatch,
    Occupied,
    Protocol(ShareFetchRequestFailure),
    CoreOpen(ShareFetchSessionOpenError),
    CoreApply(ShareFetchSessionApplyError),
}
