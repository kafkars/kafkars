//! Exact engine ownership of KIP-848 effects between core and mechanism turns.

use kafka_client_core::{
    AssignmentGeneration, ConsumerGroupHeartbeatApplyError, ConsumerGroupHeartbeatAttempt,
    ConsumerGroupHeartbeatEffect, ConsumerGroupHeartbeatErrorKind, ConsumerGroupHeartbeatInput,
    ConsumerGroupHeartbeatMachine, ConsumerGroupHeartbeatPolicy, ConsumerGroupHeartbeatRequestKind,
    ConsumerGroupMemberEpoch, GroupId, MemberId, MembershipCycle,
};

use crate::clock::{DeadlineCapture, OperationDeadline};

const CONSUMER_GROUP_ATTEMPT_TIMEOUT_TICKS: u64 = 10_000_000_000;

/// One exact core-authorized API 68 submission awaiting mechanism ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct PreparedConsumerGroupHeartbeat {
    attempt: ConsumerGroupHeartbeatAttempt,
    kind: ConsumerGroupHeartbeatRequestKind,
    member_id: Option<MemberId>,
    member_epoch: Option<ConsumerGroupMemberEpoch>,
    assignment_generation: Option<AssignmentGeneration>,
    deadline: OperationDeadline,
}

impl PreparedConsumerGroupHeartbeat {
    pub(super) const fn attempt(self) -> ConsumerGroupHeartbeatAttempt {
        self.attempt
    }

    pub(super) const fn kind(self) -> ConsumerGroupHeartbeatRequestKind {
        self.kind
    }

    pub(super) const fn member_id(self) -> Option<MemberId> {
        self.member_id
    }

    pub(super) const fn member_epoch(self) -> Option<ConsumerGroupMemberEpoch> {
        self.member_epoch
    }

    pub(super) const fn assignment_generation(self) -> Option<AssignmentGeneration> {
        self.assignment_generation
    }

    pub(super) const fn deadline(self) -> OperationDeadline {
        self.deadline
    }
}

/// Pre-core or post-core start failure for one modern group entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ConsumerGroupExecutionAdmissionError {
    Occupied,
    Core(ConsumerGroupHeartbeatErrorKind),
    DeadlineMismatch,
    EffectShape,
}

/// One modern membership lifetime, its core owner, and pending effect transfer.
pub(super) struct ConsumerGroupExecution {
    machine: ConsumerGroupHeartbeatMachine,
    cycle: Option<MembershipCycle>,
    prepared: Option<PreparedConsumerGroupHeartbeat>,
}

impl ConsumerGroupExecution {
    pub(super) fn new(group_id: GroupId) -> Self {
        let policy = ConsumerGroupHeartbeatPolicy::try_new(CONSUMER_GROUP_ATTEMPT_TIMEOUT_TICKS)
            .unwrap_or_else(|_error| unreachable!("fixed attempt timeout is positive"));
        Self {
            machine: ConsumerGroupHeartbeatMachine::new(group_id, policy),
            cycle: None,
            prepared: None,
        }
    }

    pub(super) fn begin(
        &mut self,
        capture: DeadlineCapture,
    ) -> Result<(), ConsumerGroupExecutionAdmissionError> {
        if self.prepared.is_some() || self.cycle.is_some() {
            return Err(ConsumerGroupExecutionAdmissionError::Occupied);
        }
        let transition = self
            .machine
            .apply(ConsumerGroupHeartbeatInput::Begin {
                now: capture.now(),
                deadline: capture.deadline(),
            })
            .map_err(map_core)?;
        let mut effects = transition.into_effects();
        let Some(ConsumerGroupHeartbeatEffect::Submit {
            group_id,
            attempt,
            kind,
            member_id,
            member_epoch,
            assignment_generation,
            deadline,
        }) = effects.next()
        else {
            return Err(ConsumerGroupExecutionAdmissionError::EffectShape);
        };
        if effects.next().is_some()
            || group_id != self.machine.group_id()
            || kind != ConsumerGroupHeartbeatRequestKind::Join
        {
            return Err(ConsumerGroupExecutionAdmissionError::EffectShape);
        }
        if deadline != capture.deadline() {
            return Err(ConsumerGroupExecutionAdmissionError::DeadlineMismatch);
        }
        self.prepared = Some(PreparedConsumerGroupHeartbeat {
            attempt,
            kind,
            member_id,
            member_epoch,
            assignment_generation,
            deadline: capture.operation_deadline(),
        });
        self.cycle = Some(MembershipCycle::initial());
        Ok(())
    }

    pub(super) const fn machine(&self) -> &ConsumerGroupHeartbeatMachine {
        &self.machine
    }

    pub(super) const fn machine_mut(&mut self) -> &mut ConsumerGroupHeartbeatMachine {
        &mut self.machine
    }

    pub(super) const fn cycle(&self) -> Option<MembershipCycle> {
        self.cycle
    }

    pub(super) const fn prepared(&self) -> Option<PreparedConsumerGroupHeartbeat> {
        self.prepared
    }

    pub(super) fn take_prepared(&mut self) -> Option<PreparedConsumerGroupHeartbeat> {
        self.prepared.take()
    }

    pub(super) fn restore_prepared(&mut self, prepared: PreparedConsumerGroupHeartbeat) -> bool {
        if self.prepared.is_some() {
            return false;
        }
        self.prepared = Some(prepared);
        true
    }

    pub(super) fn unsettled(&self) -> usize {
        usize::from(self.machine.in_flight().is_some())
    }
}

fn map_core(error: ConsumerGroupHeartbeatApplyError) -> ConsumerGroupExecutionAdmissionError {
    match error.kind() {
        ConsumerGroupHeartbeatErrorKind::InvalidPhase => {
            ConsumerGroupExecutionAdmissionError::Occupied
        }
        kind => ConsumerGroupExecutionAdmissionError::Core(kind),
    }
}
