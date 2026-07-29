//! Per-group settlement and caller-ordered batch aggregation transitions.

use core::{mem::take, num::NonZeroI16};

use crate::DeliveryStatus;

use super::validation::correlate_outcomes;
use crate::admin::group_offsets::{
    ListConsumerGroupBatchOutcome, ListConsumerGroupOffsetsBatch, ListConsumerGroupOffsetsFailure,
    ListConsumerGroupOffsetsFailureKind, ListConsumerGroupOffsetsMachine,
    ListConsumerGroupOffsetsMachineError, ListConsumerGroupOffsetsState,
    ListConsumerGroupOffsetsTerminal, ListConsumerGroupOffsetsTransition,
    ListConsumerGroupsOffsetsBatch, model::ListConsumerGroupOffsetsPlanShape,
};

impl ListConsumerGroupOffsetsMachine {
    pub(super) fn broker_responded(
        &mut self,
        batch: ListConsumerGroupOffsetsBatch,
    ) -> Result<ListConsumerGroupOffsetsTransition, ListConsumerGroupOffsetsMachineError> {
        if self.state != ListConsumerGroupOffsetsState::Submitted {
            return Err(ListConsumerGroupOffsetsMachineError::InvalidState);
        }
        let Some(selection) = self.plan.selections().get(self.next_group) else {
            return Err(ListConsumerGroupOffsetsMachineError::InvalidState);
        };
        let Some(batch) = correlate_outcomes(selection, batch) else {
            return Ok(self.finish(ListConsumerGroupOffsetsTerminal::Failed(
                ListConsumerGroupOffsetsFailure::new(
                    ListConsumerGroupOffsetsFailureKind::InvalidResponse,
                    DeliveryStatus::PossiblySent,
                ),
            )));
        };
        let Some(group_id) = self.current_group_id().map(str::to_owned) else {
            return Err(ListConsumerGroupOffsetsMachineError::InvalidState);
        };
        self.record_outcome(ListConsumerGroupBatchOutcome::offsets(group_id, batch))
    }

    pub(super) fn broker_rejected(
        &mut self,
        code: NonZeroI16,
        throttle_time_ms: u32,
    ) -> Result<ListConsumerGroupOffsetsTransition, ListConsumerGroupOffsetsMachineError> {
        if self.state != ListConsumerGroupOffsetsState::Submitted {
            return Err(ListConsumerGroupOffsetsMachineError::InvalidState);
        }
        if self.plan.shape() == ListConsumerGroupOffsetsPlanShape::Singular {
            return Ok(self.finish_failure(
                ListConsumerGroupOffsetsFailureKind::Broker(code),
                DeliveryStatus::PossiblySent,
            ));
        }
        let Some(group_id) = self.current_group_id().map(str::to_owned) else {
            return Err(ListConsumerGroupOffsetsMachineError::InvalidState);
        };
        self.record_outcome(ListConsumerGroupBatchOutcome::broker_rejected(
            group_id,
            code,
            throttle_time_ms,
        ))
    }

    fn record_outcome(
        &mut self,
        outcome: ListConsumerGroupBatchOutcome,
    ) -> Result<ListConsumerGroupOffsetsTransition, ListConsumerGroupOffsetsMachineError> {
        let Some(expected_group_id) = self.current_group_id() else {
            return Err(ListConsumerGroupOffsetsMachineError::InvalidState);
        };
        if outcome.group_id() != expected_group_id {
            return Ok(self.finish_failure(
                ListConsumerGroupOffsetsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ));
        }
        self.maximum_throttle_time_ms = self
            .maximum_throttle_time_ms
            .max(outcome.throttle_time_ms());
        if self.plan.shape() == ListConsumerGroupOffsetsPlanShape::Singular {
            if self.next_group != 0 || self.plan.group_ids().len() != 1 {
                return Err(ListConsumerGroupOffsetsMachineError::InvalidState);
            }
            self.next_group = 1;
            let ListConsumerGroupBatchOutcome::Offsets { offsets, .. } = outcome else {
                return Err(ListConsumerGroupOffsetsMachineError::InvalidState);
            };
            return Ok(self.finish(ListConsumerGroupOffsetsTerminal::Offsets(offsets)));
        }
        self.outcomes.push(outcome);
        self.next_group += 1;
        if self.next_group < self.plan.group_ids().len() {
            return self.submit_current();
        }
        let batch = ListConsumerGroupsOffsetsBatch::new(
            self.maximum_throttle_time_ms,
            take(&mut self.outcomes),
        );
        Ok(self.finish(ListConsumerGroupOffsetsTerminal::Batch(batch)))
    }
}
