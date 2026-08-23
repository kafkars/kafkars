//! Nonoverlapping `ShareFetch` session, assignment, and response settlement.

use crate::{AssignedTopicPartition, Deadline, DeliveryStatus, Moment};

use super::{
    ShareAcquiredRange, ShareAcquisitionLedger, ShareAcquisitionPolicy,
    ShareFetchAssignmentGeneration, ShareFetchAttempt, ShareFetchSessionApplyError,
    ShareFetchSessionEpoch, ShareFetchSessionErrorKind, ShareFetchSessionFence,
    ShareFetchSessionOpenError, ShareFetchSettlementError, ShareFetchSettlementErrorKind,
};

/// Maximum partitions retained in one broker-local `ShareFetch` session.
pub const SHARE_FETCH_MAX_PARTITIONS_PER_BROKER: usize = 64;

/// Lifecycle of one broker-local `ShareFetch` session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShareFetchSessionPhase {
    /// The session can admit one new fetch attempt.
    Ready,
    /// One exact fetch attempt owns driver execution.
    InFlight,
    /// Session authority was lost and must be drained before reopening.
    Lost,
}

/// Deterministic owner of one broker-local `ShareFetch` session and acquisition ledger.
#[derive(Debug, Eq, PartialEq)]
pub struct ShareFetchSessionMachine {
    phase: ShareFetchSessionPhase,
    fence: ShareFetchSessionFence,
    assignment_generation: ShareFetchAssignmentGeneration,
    assignment: Vec<AssignedTopicPartition>,
    in_flight: Option<ShareFetchAttempt>,
    ledger: ShareAcquisitionLedger,
}

impl ShareFetchSessionMachine {
    /// Opens epoch zero after reserving the complete acquisition ledger.
    pub fn try_open(
        fence: ShareFetchSessionFence,
        assignment: Vec<AssignedTopicPartition>,
        policy: ShareAcquisitionPolicy,
    ) -> Result<Self, ShareFetchSessionOpenError> {
        if fence.session_epoch() != ShareFetchSessionEpoch::initial() {
            return Err(ShareFetchSessionOpenError::NoninitialEpoch);
        }
        validate_assignment(&assignment).map_err(ShareFetchSessionOpenError::Assignment)?;
        let ledger = ShareAcquisitionLedger::try_new(policy)
            .map_err(ShareFetchSessionOpenError::Acquisition)?;
        Ok(Self {
            phase: ShareFetchSessionPhase::Ready,
            fence,
            assignment_generation: ShareFetchAssignmentGeneration::initial(),
            assignment,
            in_flight: None,
            ledger,
        })
    }

    /// Returns the current lifecycle phase.
    pub const fn phase(&self) -> ShareFetchSessionPhase {
        self.phase
    }

    /// Returns the current complete session fence.
    pub const fn fence(&self) -> ShareFetchSessionFence {
        self.fence
    }

    /// Returns the assignment snapshot generation.
    pub const fn assignment_generation(&self) -> ShareFetchAssignmentGeneration {
        self.assignment_generation
    }

    /// Borrows the current partitions eligible for acquisition.
    pub fn assignment(&self) -> &[AssignedTopicPartition] {
        &self.assignment
    }

    /// Returns the sole in-flight attempt, if any.
    pub const fn in_flight(&self) -> Option<ShareFetchAttempt> {
        self.in_flight
    }

    /// Borrows the bounded acquisition ledger.
    pub const fn ledger(&self) -> &ShareAcquisitionLedger {
        &self.ledger
    }

    /// Mutably borrows the bounded ledger for delivery and retirement turns.
    pub fn ledger_mut(&mut self) -> &mut ShareAcquisitionLedger {
        &mut self.ledger
    }

    /// Replaces future acquisition eligibility without retiring ledger entries.
    pub fn replace_assignment(
        &mut self,
        assignment: Vec<AssignedTopicPartition>,
    ) -> Result<(), ShareFetchSessionApplyError> {
        validate_assignment(&assignment).map_err(ShareFetchSessionApplyError::new)?;
        let next = self.assignment_generation.checked_next().ok_or_else(|| {
            ShareFetchSessionApplyError::new(ShareFetchSessionErrorKind::Exhausted)
        })?;
        if self.phase == ShareFetchSessionPhase::Lost {
            return Err(ShareFetchSessionApplyError::new(
                ShareFetchSessionErrorKind::InvalidState,
            ));
        }
        self.assignment = assignment;
        self.assignment_generation = next;
        Ok(())
    }

    /// Admits one exact, deadline-bounded `ShareFetch` attempt.
    pub fn prepare_fetch(
        &mut self,
        deadline: Deadline,
        now: Moment,
    ) -> Result<ShareFetchAttempt, ShareFetchSessionApplyError> {
        if self.phase != ShareFetchSessionPhase::Ready || self.assignment.is_empty() {
            return Err(ShareFetchSessionApplyError::new(
                ShareFetchSessionErrorKind::InvalidState,
            ));
        }
        if deadline.is_elapsed_at(now) {
            return Err(ShareFetchSessionApplyError::new(
                ShareFetchSessionErrorKind::DeadlineElapsed,
            ));
        }
        let attempt = ShareFetchAttempt::new(self.fence, self.assignment_generation, deadline);
        self.in_flight = Some(attempt);
        self.phase = ShareFetchSessionPhase::InFlight;
        Ok(attempt)
    }

    /// Atomically settles one correlated response into staged acquisitions.
    pub fn settle_acquired(
        &mut self,
        attempt: ShareFetchAttempt,
        now: Moment,
        ranges: Vec<ShareAcquiredRange>,
    ) -> Result<usize, ShareFetchSettlementError> {
        if self.phase != ShareFetchSessionPhase::InFlight || self.in_flight != Some(attempt) {
            return Err(ShareFetchSettlementError::new(
                ShareFetchSettlementErrorKind::StaleAttempt,
                ranges,
            ));
        }
        if attempt.deadline().is_elapsed_at(now) {
            self.lose();
            return Err(ShareFetchSettlementError::new(
                ShareFetchSettlementErrorKind::DeadlineElapsed,
                ranges,
            ));
        }
        let Some(next_fence) = self.fence.next_session() else {
            self.lose();
            return Err(ShareFetchSettlementError::new(
                ShareFetchSettlementErrorKind::SessionEpochExhausted,
                ranges,
            ));
        };
        if attempt.assignment_generation() != self.assignment_generation {
            self.fence = next_fence;
            self.in_flight = None;
            self.phase = ShareFetchSessionPhase::Ready;
            return Err(ShareFetchSettlementError::new(
                ShareFetchSettlementErrorKind::AssignmentChanged,
                ranges,
            ));
        }
        if ranges
            .iter()
            .any(|range| !self.assignment.contains(&range.partition()))
        {
            self.lose();
            return Err(ShareFetchSettlementError::new(
                ShareFetchSettlementErrorKind::UnassignedPartition,
                ranges,
            ));
        }
        match self.ledger.try_admit(attempt.fence(), now, ranges) {
            Ok(acquisitions) => {
                self.fence = next_fence;
                self.in_flight = None;
                self.phase = ShareFetchSessionPhase::Ready;
                Ok(acquisitions)
            }
            Err(error) => {
                let kind = ShareFetchSettlementErrorKind::Acquisition(error.kind());
                let ranges = error.into_ranges();
                self.lose();
                Err(ShareFetchSettlementError::new(kind, ranges))
            }
        }
    }

    /// Settles one exact failed attempt without inventing broker session state.
    pub fn settle_failure(
        &mut self,
        attempt: ShareFetchAttempt,
        delivery: DeliveryStatus,
    ) -> Result<(), ShareFetchSessionApplyError> {
        if self.phase != ShareFetchSessionPhase::InFlight || self.in_flight != Some(attempt) {
            return Err(ShareFetchSessionApplyError::new(
                ShareFetchSessionErrorKind::InvalidState,
            ));
        }
        match delivery {
            DeliveryStatus::NotSent => {
                self.in_flight = None;
                self.phase = ShareFetchSessionPhase::Ready;
            }
            DeliveryStatus::PossiblySent => self.lose(),
        }
        Ok(())
    }

    fn lose(&mut self) {
        self.in_flight = None;
        self.phase = ShareFetchSessionPhase::Lost;
    }
}

fn validate_assignment(
    assignment: &[AssignedTopicPartition],
) -> Result<(), ShareFetchSessionErrorKind> {
    if assignment.len() > SHARE_FETCH_MAX_PARTITIONS_PER_BROKER {
        return Err(ShareFetchSessionErrorKind::AssignmentCapacity);
    }
    for (index, partition) in assignment.iter().enumerate() {
        if assignment[..index].contains(partition) {
            return Err(ShareFetchSessionErrorKind::DuplicatePartition);
        }
    }
    Ok(())
}
