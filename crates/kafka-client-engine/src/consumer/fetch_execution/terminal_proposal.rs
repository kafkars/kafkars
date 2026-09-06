//! Linear Fetch-terminal proposal before deterministic core application.

use kafka_client_core::FetchFence;

use crate::protocol::fetch::{FetchBrokerFailure, FetchBrokerLevel, FetchLeader};

use super::terminal::FetchTerminalFact;

/// One normalized terminal that still owns core application and route settlement.
#[must_use = "a Fetch terminal proposal must be applied or retained"]
pub(in crate::consumer) struct FetchTerminalProposal {
    fact: FetchTerminalFact,
    broker_failure: Option<FetchBrokerFailure>,
}

/// Proof that one linearly owned terminal is Kafka partition `OFFSET_OUT_OF_RANGE`.
#[must_use = "an offset-out-of-range proposal must be applied or returned to generic settlement"]
pub(in crate::consumer) struct PartitionOffsetOutOfRangeProposal {
    proposal: FetchTerminalProposal,
}

/// Exact KIP-951 broker response eligible for one fenced Fetch replacement.
#[must_use = "a leader-movement proposal must be retried or terminally applied"]
pub(in crate::consumer) struct LeaderMovementFetchProposal {
    proposal: FetchTerminalProposal,
    leader: Option<FetchLeader>,
}

impl FetchTerminalProposal {
    pub(super) const fn new(
        fact: FetchTerminalFact,
        broker_failure: Option<FetchBrokerFailure>,
    ) -> Self {
        Self {
            fact,
            broker_failure,
        }
    }

    #[allow(
        clippy::result_large_err,
        reason = "a failed narrowing returns the exact linear terminal proposal intact"
    )]
    pub(in crate::consumer) fn into_partition_offset_out_of_range(
        self,
    ) -> Result<PartitionOffsetOutOfRangeProposal, Self> {
        if matches!(
            self.broker_failure,
            Some(failure)
                if failure.level() == FetchBrokerLevel::Partition && failure.code().get() == 1
        ) {
            Ok(PartitionOffsetOutOfRangeProposal { proposal: self })
        } else {
            Err(self)
        }
    }

    #[allow(
        clippy::result_large_err,
        reason = "a failed narrowing returns the exact linear terminal proposal intact"
    )]
    pub(in crate::consumer) fn into_leader_movement_retry(
        self,
    ) -> Result<LeaderMovementFetchProposal, Self> {
        let leader_movement = matches!(
            self.broker_failure,
            Some(failure)
                if failure.level() == FetchBrokerLevel::Partition
                    && matches!(failure.code().get(), 6 | 74 | 75)
        );
        let transport = matches!(
            &self.fact.action,
            super::terminal::FetchTerminalAction::Apply(
                kafka_client_core::AssignedConsumerInput::FetchFailed {
                    failure: kafka_client_core::FetchFailure::Transport,
                    ..
                }
            )
        );
        if self.fact.request.topic_route().is_none() || (!leader_movement && !transport) {
            return Err(self);
        }
        let leader = self
            .broker_failure
            .and_then(FetchBrokerFailure::leader)
            .filter(|leader| {
                self.fact
                    .request
                    .leader_epoch()
                    .is_none_or(|epoch| leader.epoch > epoch)
            });
        Ok(LeaderMovementFetchProposal {
            proposal: self,
            leader,
        })
    }

    pub(super) fn into_fact(self) -> FetchTerminalFact {
        self.fact
    }
}

impl LeaderMovementFetchProposal {
    pub(super) fn is_transport(&self) -> bool {
        matches!(
            &self.proposal.fact.action,
            super::terminal::FetchTerminalAction::Apply(
                kafka_client_core::AssignedConsumerInput::FetchFailed {
                    failure: kafka_client_core::FetchFailure::Transport,
                    ..
                }
            )
        )
    }

    pub(super) const fn leader(&self) -> Option<FetchLeader> {
        self.leader
    }

    pub(super) const fn clears_leader_epoch(&self) -> bool {
        matches!(self.proposal.broker_failure, Some(failure) if failure.code().get() == 75)
    }

    pub(super) fn into_proposal(self) -> FetchTerminalProposal {
        self.proposal
    }

    pub(super) fn into_parts(self) -> (FetchTerminalFact, Option<FetchLeader>) {
        (self.proposal.fact, self.leader)
    }
}

impl PartitionOffsetOutOfRangeProposal {
    pub(in crate::consumer) fn fence(&self) -> FetchFence {
        self.proposal.fact.request.fence()
    }

    pub(in crate::consumer) fn into_proposal(self) -> FetchTerminalProposal {
        self.proposal
    }
}
