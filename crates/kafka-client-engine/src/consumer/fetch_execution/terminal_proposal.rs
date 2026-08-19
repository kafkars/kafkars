//! Linear Fetch-terminal proposal before deterministic core application.

use kafka_client_core::FetchFence;

use crate::protocol::fetch::{FetchBrokerFailure, FetchBrokerLevel};

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

    pub(super) fn into_fact(self) -> FetchTerminalFact {
        self.fact
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
