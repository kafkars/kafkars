//! Broker-local core session and exact prepared `ShareFetch` ownership.

use kafka_client_core::{
    DeliveryStatus, Moment, ShareAcquiredRange, ShareAcquisitionPolicy, ShareFetchAttempt,
    ShareFetchSessionApplyError, ShareFetchSessionFence, ShareFetchSessionMachine,
    ShareFetchSessionOpenError, ShareFetchSettlementError,
};
use std::sync::Arc;

use crate::{
    clock::DeadlineCapture,
    protocol::consumer::share_fetch::{
        PreparedShareFetchRequest, ShareFetchRequestFailure, ShareFetchRequestSettings,
        ShareFetchResponseLimits,
    },
    protocol::fetch::FetchDecodeLimits,
};

use super::fetch_plan::{ShareBrokerSessionPlan, ShareFetchSessionRequestPlan};
use super::fetch_session_execution::{ActiveShareFetchCall, ShareFetchSessionTerminal};
use super::fetch_session_settlement::StagedShareFetchDelivery;

/// Exact core attempt paired with its generated request and unchanged deadline.
#[must_use = "a prepared ShareFetch session attempt must be submitted or settled"]
pub(super) struct PreparedShareFetchSession {
    attempt: ShareFetchAttempt,
    request: PreparedShareFetchRequest,
    capture: DeadlineCapture,
}

impl PreparedShareFetchSession {
    pub(super) fn into_parts(
        self,
    ) -> (
        ShareFetchAttempt,
        PreparedShareFetchRequest,
        DeadlineCapture,
    ) {
        (self.attempt, self.request, self.capture)
    }

    pub(super) const fn deadline(&self) -> kafka_client_core::Deadline {
        self.capture.deadline()
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
    decode_limits: FetchDecodeLimits,
    lock_timeout_ms: Option<u32>,
    prepared: Option<PreparedShareFetchSession>,
    pub(super) active: Option<ActiveShareFetchCall>,
    pub(super) terminal: Option<ShareFetchSessionTerminal>,
    pub(super) staged: Option<StagedShareFetchDelivery>,
}

/// Immutable bounded settings captured before one broker session opens.
pub(super) struct ShareFetchSessionConfig {
    group: Arc<str>,
    member: Arc<str>,
    policy: ShareAcquisitionPolicy,
    settings: ShareFetchRequestSettings,
    response_limits: ShareFetchResponseLimits,
    decode_limits: FetchDecodeLimits,
}

impl ShareFetchSessionConfig {
    pub(super) const fn new(
        group: Arc<str>,
        member: Arc<str>,
        policy: ShareAcquisitionPolicy,
        settings: ShareFetchRequestSettings,
        response_limits: ShareFetchResponseLimits,
        decode_limits: FetchDecodeLimits,
    ) -> Self {
        Self {
            group,
            member,
            policy,
            settings,
            response_limits,
            decode_limits,
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
            decode_limits: config.decode_limits,
            lock_timeout_ms: None,
            prepared: None,
            active: None,
            terminal: None,
            staged: None,
        };
        owner.prepare_next_at(capture, capture.now())?;
        Ok(owner)
    }

    pub(super) fn prepare_next(
        &mut self,
        capture: DeadlineCapture,
    ) -> Result<(), ShareFetchSessionOwnerError> {
        self.prepare_next_at(capture, capture.now())
    }

    pub(super) fn prepare_next_at(
        &mut self,
        capture: DeadlineCapture,
        now: Moment,
    ) -> Result<(), ShareFetchSessionOwnerError> {
        if self.prepared.is_some()
            || self.active.is_some()
            || self.terminal.is_some()
            || self.staged.is_some()
        {
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
            .prepare_fetch(capture.deadline(), now)
            .map_err(ShareFetchSessionOwnerError::CoreApply)?;
        self.prepared = Some(PreparedShareFetchSession {
            attempt,
            request,
            capture,
        });
        Ok(())
    }

    pub(super) fn take_prepared(&mut self) -> Option<PreparedShareFetchSession> {
        self.prepared.take()
    }

    pub(super) const fn has_prepared(&self) -> bool {
        self.prepared.is_some()
    }

    pub(super) fn settle_unsubmitted(
        &mut self,
        prepared: PreparedShareFetchSession,
    ) -> Result<(), ShareFetchSessionOwnerError> {
        let (attempt, request, _capture) = prepared.into_parts();
        drop(request);
        self.settle_attempt_failure(attempt, DeliveryStatus::NotSent)
    }

    pub(super) fn settle_attempt_failure(
        &mut self,
        attempt: ShareFetchAttempt,
        delivery: DeliveryStatus,
    ) -> Result<(), ShareFetchSessionOwnerError> {
        self.machine
            .settle_failure(attempt, delivery)
            .map_err(ShareFetchSessionOwnerError::CoreApply)
    }

    pub(super) fn settle_acquired(
        &mut self,
        attempt: ShareFetchAttempt,
        now: Moment,
        ranges: Vec<ShareAcquiredRange>,
    ) -> Result<usize, ShareFetchSettlementError> {
        self.machine.settle_acquired(attempt, now, ranges)
    }

    pub(super) const fn machine(&self) -> &ShareFetchSessionMachine {
        &self.machine
    }

    pub(super) const fn response_limits(&self) -> ShareFetchResponseLimits {
        self.response_limits
    }

    pub(super) const fn decode_limits(&self) -> FetchDecodeLimits {
        self.decode_limits
    }

    pub(super) const fn lock_timeout_ms(&self) -> Option<u32> {
        self.lock_timeout_ms
    }

    pub(super) fn commit_lock_timeout_ms(&mut self, value: u32) {
        self.lock_timeout_ms = Some(value);
    }

    pub(super) const fn request_plan(&self) -> &ShareFetchSessionRequestPlan {
        &self.request_plan
    }

    pub(super) fn next_deadline(&self) -> Option<kafka_client_core::Deadline> {
        self.prepared
            .as_ref()
            .map(PreparedShareFetchSession::deadline)
            .or_else(|| self.active.as_ref().map(ActiveShareFetchCall::deadline))
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
