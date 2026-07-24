//! Closed settlement observation and error-domain scenarios.

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine, AssignedPartition,
    AssignedTopicPartition, Deadline, FetchFence, Moment, NextFetchOffset, PartitionIndex,
    StartPosition, TopicId,
};

use super::settlement::{
    FetchBeginSettlementError, FetchConfirmationError, FetchPoll, StaleFetchConfirmationError,
};

#[test]
fn settlement_states_keep_live_stale_and_pending_domains_distinct() {
    let fence = fence();
    assert_ne!(
        FetchPoll::TerminalReady { fence },
        FetchPoll::StaleConfirmationReady { fence }
    );
    assert_ne!(
        FetchBeginSettlementError::StaleSettledCall { supplied: fence },
        FetchBeginSettlementError::ConfirmationPending { pending: fence }
    );
    assert_ne!(
        FetchConfirmationError::NoPendingConfirmation { supplied: fence },
        FetchConfirmationError::FenceMismatch {
            pending: fence,
            supplied: fence,
        }
    );
    assert_ne!(
        StaleFetchConfirmationError::NoSettledCall { supplied: fence },
        StaleFetchConfirmationError::LiveSettledCall { supplied: fence }
    );
}

fn fence() -> FetchFence {
    let mut machine = AssignedConsumerMachine::new();
    machine
        .apply(AssignedConsumerInput::Assign {
            partitions: vec![AssignedPartition::new(
                AssignedTopicPartition::new(TopicId::from_raw(2), PartitionIndex::from_raw(3)),
                StartPosition::Offset(
                    NextFetchOffset::try_from_raw(5).unwrap_or_else(|| panic!("valid offset")),
                ),
            )],
            now: Moment::from_tick(0),
            resolution_deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("assign: {error}"))
        .effects()
        .iter()
        .find_map(|effect| match effect {
            AssignedConsumerEffect::FetchReady { fence, .. } => Some(*fence),
            _ => None,
        })
        .unwrap_or_else(|| panic!("FetchReady effect"))
}
