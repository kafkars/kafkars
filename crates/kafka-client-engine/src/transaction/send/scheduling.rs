//! Scheduling interest and unsettled ownership for one transactional send.

use super::{owner::TransactionSendOwner, turn::TransactionSendSlot};

impl TransactionSendOwner {
    pub(crate) fn unsettled(&self) -> usize {
        usize::from(!matches!(
            self.slot,
            TransactionSendSlot::Vacant | TransactionSendSlot::Published
        ))
    }

    pub(crate) fn next_deadline(&self) -> Option<kafka_client_core::Deadline> {
        match &self.slot {
            TransactionSendSlot::Reserved(request, _) => Some(request.deadline().core()),
            TransactionSendSlot::AwaitingPartition(pending) => {
                Some(pending.request.deadline().core())
            }
            TransactionSendSlot::Enrolling(pending)
            | TransactionSendSlot::Ready(pending, _)
            | TransactionSendSlot::Materialized(pending, _)
            | TransactionSendSlot::Invalidating(pending, _, _, _) => Some(pending.deadline.core()),
            TransactionSendSlot::RetryBackoff(pending, _, _, replacement) => {
                Some(pending.deadline.core().min(replacement.not_before))
            }
            TransactionSendSlot::Vacant
            | TransactionSendSlot::Partitioning(_, _)
            | TransactionSendSlot::Producing(_, _, _)
            | TransactionSendSlot::Settling(_, _, _)
            | TransactionSendSlot::Terminal(_, _)
            | TransactionSendSlot::Published => None,
        }
    }
}
