//! Exact engine ownership of KIP-848 effects between core and mechanism turns.

use kafka_client_core::{
    AssignmentGeneration, ConsumerGroupHeartbeatApplyError, ConsumerGroupHeartbeatAttempt,
    ConsumerGroupHeartbeatEffect, ConsumerGroupHeartbeatErrorKind, ConsumerGroupHeartbeatInput,
    ConsumerGroupHeartbeatMachine, ConsumerGroupHeartbeatPolicy, ConsumerGroupHeartbeatRequestKind,
    ConsumerGroupMemberEpoch, GroupId, MemberId, MembershipCycle,
};

use crate::{
    clock::{DeadlineCapture, OperationDeadline},
    driver::ConsumerGroupHeartbeatCall,
};

use super::consumer_group_topic_identity::{
    ConsumerGroupTopicIdentityBuildError, ConsumerGroupTopicIdentityOwner,
};
use super::consumer_group_topic_identity_call::ConsumerGroupTopicIdentityCall;

pub(super) const CONSUMER_GROUP_ATTEMPT_TIMEOUT_TICKS: u64 = 10_000_000_000;

/// One exact core-authorized API 68 submission awaiting mechanism ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct PreparedConsumerGroupHeartbeat {
    pub(super) attempt: ConsumerGroupHeartbeatAttempt,
    pub(super) kind: ConsumerGroupHeartbeatRequestKind,
    pub(super) member_id: Option<MemberId>,
    pub(super) member_epoch: Option<ConsumerGroupMemberEpoch>,
    pub(super) assignment_generation: Option<AssignmentGeneration>,
    pub(super) deadline: OperationDeadline,
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

/// Registration-time reservation failure before modern membership exists.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ConsumerGroupExecutionBuildError {
    TopicIdentity(ConsumerGroupTopicIdentityBuildError),
}

/// Mechanism or core ownership failure after one modern start was accepted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ConsumerGroupExecutionError {
    MissingPrepared,
    TopicIdentityCallOccupied,
    TopicIdentityCallMissing,
    HeartbeatCallOccupied,
    HeartbeatCallMissing,
    Core(ConsumerGroupHeartbeatErrorKind),
    EffectShape,
}

/// One modern membership lifetime, its core owner, and pending effect transfer.
pub(super) struct ConsumerGroupExecution {
    pub(super) machine: ConsumerGroupHeartbeatMachine,
    pub(super) cycle: Option<MembershipCycle>,
    pub(super) prepared: Option<PreparedConsumerGroupHeartbeat>,
    pub(super) rebalance_timeout_ms: u32,
    pub(super) topic_identities: ConsumerGroupTopicIdentityOwner,
    pub(super) topic_identity_call: Option<ConsumerGroupTopicIdentityCall>,
    pub(super) heartbeat_call: Option<ConsumerGroupHeartbeatCall>,
}

impl ConsumerGroupExecution {
    pub(super) fn try_new(
        group_id: GroupId,
        topic_count: usize,
        rebalance_timeout_ms: u32,
    ) -> Result<Self, ConsumerGroupExecutionBuildError> {
        let policy = ConsumerGroupHeartbeatPolicy::try_new(CONSUMER_GROUP_ATTEMPT_TIMEOUT_TICKS)
            .unwrap_or_else(|_error| unreachable!("fixed attempt timeout is positive"));
        Ok(Self {
            machine: ConsumerGroupHeartbeatMachine::new(group_id, policy),
            cycle: None,
            prepared: None,
            rebalance_timeout_ms,
            topic_identities: ConsumerGroupTopicIdentityOwner::try_new(topic_count)
                .map_err(ConsumerGroupExecutionBuildError::TopicIdentity)?,
            topic_identity_call: None,
            heartbeat_call: None,
        })
    }

    #[cfg(test)]
    pub(super) fn new(group_id: GroupId) -> Self {
        Self::try_new(group_id, 0, 30_000)
            .unwrap_or_else(|error| panic!("test execution: {error:?}"))
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

    pub(super) const fn rebalance_timeout_ms(&self) -> u32 {
        self.rebalance_timeout_ms
    }

    pub(super) const fn topic_identities(&self) -> &ConsumerGroupTopicIdentityOwner {
        &self.topic_identities
    }

    pub(super) const fn topic_identities_mut(&mut self) -> &mut ConsumerGroupTopicIdentityOwner {
        &mut self.topic_identities
    }

    pub(super) const fn prepared(&self) -> Option<PreparedConsumerGroupHeartbeat> {
        self.prepared
    }

    pub(super) fn take_prepared(&mut self) -> Option<PreparedConsumerGroupHeartbeat> {
        self.prepared.take()
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
