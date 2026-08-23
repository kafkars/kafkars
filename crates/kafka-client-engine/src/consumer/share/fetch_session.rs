//! Broker-local core session and exact prepared `ShareFetch` ownership.

use kafka_client_core::{
    Deadline, DeliveryStatus, Moment, ShareAcquiredRange, ShareFetchAttempt,
    ShareFetchSessionApplyError, ShareFetchSessionFence, ShareFetchSessionMachine,
    ShareFetchSessionOpenError, ShareFetchSessionPhase, ShareFetchSettlementError,
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

use super::fetch_acknowledgement::{PreparedShareAcknowledgement, ShareAcknowledgementTerminal};
use super::fetch_acknowledgement_execution::{
    ActiveShareAcknowledgementCall, ShareAcknowledgementExecutionOutcome,
    ShareAcknowledgementOwnershipFault,
};
use super::fetch_plan::{ShareBrokerSessionPlan, ShareFetchSessionRequestPlan};
use super::fetch_session_execution::{ActiveShareFetchCall, ShareFetchSessionTerminal};
use super::fetch_session_set::ShareFetchSessionConfig;
use super::fetch_session_settlement::StagedShareFetchDelivery;

/// Exact core attempt paired with its generated request and unchanged deadline.
#[must_use = "a prepared ShareFetch session attempt must be submitted or settled"]
pub(super) struct PreparedShareFetchSession {
    pub(super) attempt: ShareFetchAttempt,
    pub(super) request: PreparedShareFetchRequest,
    pub(super) capture: DeadlineCapture,
}

/// Closed preparation owner for one broker-local share session.
#[must_use = "a share fetch session must remain hosted until it is closed or lost"]
pub(super) struct ShareFetchSessionOwner {
    pub(super) machine: ShareFetchSessionMachine,
    request_plan: ShareFetchSessionRequestPlan,
    pub(super) group: Arc<str>,
    pub(super) member: Arc<str>,
    settings: ShareFetchRequestSettings,
    response_limits: ShareFetchResponseLimits,
    decode_limits: FetchDecodeLimits,
    lock_timeout_ms: Option<u32>,
    pub(super) throttle_until: Option<Deadline>,
    pub(super) prepared: Option<PreparedShareFetchSession>,
    pub(super) active: Option<ActiveShareFetchCall>,
    pub(super) terminal: Option<ShareFetchSessionTerminal>,
    pub(super) staged: Option<StagedShareFetchDelivery>,
    pub(super) prepared_acknowledgement: Option<PreparedShareAcknowledgement>,
    pub(super) active_acknowledgement: Option<ActiveShareAcknowledgementCall>,
    pub(super) acknowledgement_terminal: Option<ShareAcknowledgementTerminal>,
    pub(super) acknowledgement_outcome: Option<ShareAcknowledgementExecutionOutcome>,
    pub(super) acknowledgement_faults: Vec<ShareAcknowledgementOwnershipFault>,
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
            throttle_until: None,
            prepared: None,
            active: None,
            terminal: None,
            staged: None,
            prepared_acknowledgement: None,
            active_acknowledgement: None,
            acknowledgement_terminal: None,
            acknowledgement_outcome: None,
            acknowledgement_faults: Vec::new(),
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
            || self.prepared_acknowledgement.is_some()
            || self.active_acknowledgement.is_some()
            || self.acknowledgement_terminal.is_some()
            || self.acknowledgement_outcome.is_some()
            || !self.acknowledgement_faults.is_empty()
        {
            return Err(ShareFetchSessionOwnerError::Occupied);
        }
        if self
            .throttle_until
            .is_some_and(|deadline| !deadline.is_elapsed_at(now))
        {
            return Err(ShareFetchSessionOwnerError::Throttled);
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
        self.throttle_until = None;
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

    pub(super) fn commit_throttle_until(&mut self, value: Deadline) {
        self.throttle_until = Some(value);
    }

    pub(super) const fn request_plan(&self) -> &ShareFetchSessionRequestPlan {
        &self.request_plan
    }

    pub(super) fn ready_for_preparation(&self, now: Moment) -> bool {
        self.prepared.is_none()
            && self.active.is_none()
            && self.terminal.is_none()
            && self.staged.is_none()
            && self.prepared_acknowledgement.is_none()
            && self.active_acknowledgement.is_none()
            && self.acknowledgement_terminal.is_none()
            && self.acknowledgement_outcome.is_none()
            && self.acknowledgement_faults.is_empty()
            && self.machine.ledger().is_empty()
            && self.machine.phase() == ShareFetchSessionPhase::Ready
            && self
                .throttle_until
                .is_none_or(|deadline| deadline.is_elapsed_at(now))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ShareFetchSessionOwnerError {
    BrokerMismatch,
    Occupied,
    Throttled,
    Protocol(ShareFetchRequestFailure),
    CoreOpen(ShareFetchSessionOpenError),
    CoreApply(ShareFetchSessionApplyError),
}
