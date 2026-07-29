//! Per-group correlation and bounded aggregation for caller-ordered API-90 batches.

use core::mem::{size_of, take};

use crate::DeliveryStatus;

use super::super::{
    LIST_SHARE_GROUP_OFFSETS_MAX_RESPONSE_TEXT_BYTES, LIST_SHARE_GROUP_OFFSETS_MAX_RETAINED_BYTES,
    ListShareGroupOffsetsBatch, ListShareGroupOffsetsBatchOutcome,
    ListShareGroupOffsetsBrokerError, ListShareGroupOffsetsFailureKind,
    ListShareGroupOffsetsMachine, ListShareGroupOffsetsMachineError,
    ListShareGroupOffsetsPlanShape, ListShareGroupOffsetsState, ListShareGroupOffsetsTerminal,
    ListShareGroupOffsetsTransition, ListShareGroupsOffsetsBatch,
    correlation::{ResponseValidation, broker_error_is_valid, correlate},
};

impl ListShareGroupOffsetsMachine {
    pub(super) fn broker_responded(
        &mut self,
        batch: ListShareGroupOffsetsBatch,
    ) -> Result<ListShareGroupOffsetsTransition, ListShareGroupOffsetsMachineError> {
        if self.state != ListShareGroupOffsetsState::Submitted {
            return Err(ListShareGroupOffsetsMachineError::InvalidState);
        }
        let Some(plan) = self.plan.singleton_at(self.next_group) else {
            return Err(ListShareGroupOffsetsMachineError::InvalidState);
        };
        match correlate(&plan, batch) {
            ResponseValidation::Valid {
                batch,
                text_bytes,
                retained_bytes,
            } => {
                let Some(group_id) = self.current_group_id().map(str::to_owned) else {
                    return Err(ListShareGroupOffsetsMachineError::InvalidState);
                };
                if self.plan.shape() == ListShareGroupOffsetsPlanShape::Batch {
                    let nested_retained_bytes =
                        retained_bytes.saturating_sub(size_of::<ListShareGroupOffsetsBatch>());
                    let Some(total_text_bytes) = group_id.len().checked_add(text_bytes) else {
                        return Ok(self.finish_failure(
                            ListShareGroupOffsetsFailureKind::ResponseTooLarge,
                            DeliveryStatus::PossiblySent,
                        ));
                    };
                    let Some(total_retained_bytes) =
                        group_id.len().checked_add(nested_retained_bytes)
                    else {
                        return Ok(self.finish_failure(
                            ListShareGroupOffsetsFailureKind::ResponseTooLarge,
                            DeliveryStatus::PossiblySent,
                        ));
                    };
                    if !self.charge_response(total_text_bytes, total_retained_bytes) {
                        return Ok(self.finish_failure(
                            ListShareGroupOffsetsFailureKind::ResponseTooLarge,
                            DeliveryStatus::PossiblySent,
                        ));
                    }
                }
                self.record_outcome(ListShareGroupOffsetsBatchOutcome::offsets(group_id, batch))
            }
            ResponseValidation::TooLarge => Ok(self.finish_failure(
                ListShareGroupOffsetsFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            )),
            ResponseValidation::Invalid => Ok(self.finish_failure(
                ListShareGroupOffsetsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            )),
        }
    }

    pub(super) fn broker_rejected(
        &mut self,
        error: ListShareGroupOffsetsBrokerError,
    ) -> Result<ListShareGroupOffsetsTransition, ListShareGroupOffsetsMachineError> {
        if self.state != ListShareGroupOffsetsState::Submitted {
            return Err(ListShareGroupOffsetsMachineError::InvalidState);
        }
        if !broker_error_is_valid(&error) {
            return Ok(self.finish_failure(
                ListShareGroupOffsetsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ));
        }
        let Some(group_id) = self.current_group_id().map(str::to_owned) else {
            return Err(ListShareGroupOffsetsMachineError::InvalidState);
        };
        if self.plan.shape() == ListShareGroupOffsetsPlanShape::Batch {
            let diagnostic_bytes = error.message().map_or(0, str::len);
            let Some(text_bytes) = group_id.len().checked_add(diagnostic_bytes) else {
                return Ok(self.finish_failure(
                    ListShareGroupOffsetsFailureKind::ResponseTooLarge,
                    DeliveryStatus::PossiblySent,
                ));
            };
            if !self.charge_response(text_bytes, text_bytes) {
                return Ok(self.finish_failure(
                    ListShareGroupOffsetsFailureKind::ResponseTooLarge,
                    DeliveryStatus::PossiblySent,
                ));
            }
        }
        self.record_outcome(ListShareGroupOffsetsBatchOutcome::broker_rejected(
            group_id, error,
        ))
    }

    fn charge_response(&mut self, text_bytes: usize, retained_bytes: usize) -> bool {
        let Some(total_text) = self.response_text_bytes.checked_add(text_bytes) else {
            return false;
        };
        let Some(total_retained) = self.response_retained_bytes.checked_add(retained_bytes) else {
            return false;
        };
        if total_text > LIST_SHARE_GROUP_OFFSETS_MAX_RESPONSE_TEXT_BYTES
            || total_retained > LIST_SHARE_GROUP_OFFSETS_MAX_RETAINED_BYTES
        {
            return false;
        }
        self.response_text_bytes = total_text;
        self.response_retained_bytes = total_retained;
        true
    }

    fn record_outcome(
        &mut self,
        outcome: ListShareGroupOffsetsBatchOutcome,
    ) -> Result<ListShareGroupOffsetsTransition, ListShareGroupOffsetsMachineError> {
        let Some(expected_group_id) = self.current_group_id() else {
            return Err(ListShareGroupOffsetsMachineError::InvalidState);
        };
        if outcome.group_id() != expected_group_id {
            return Ok(self.finish_failure(
                ListShareGroupOffsetsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ));
        }
        self.maximum_throttle_time_ms = self
            .maximum_throttle_time_ms
            .max(outcome.throttle_time_ms());
        if self.plan.shape() == ListShareGroupOffsetsPlanShape::Singular {
            if self.next_group != 0 || self.plan.queries().len() != 1 {
                return Err(ListShareGroupOffsetsMachineError::InvalidState);
            }
            self.next_group = 1;
            let terminal = match outcome {
                ListShareGroupOffsetsBatchOutcome::Offsets { offsets, .. } => {
                    ListShareGroupOffsetsTerminal::Offsets(offsets)
                }
                ListShareGroupOffsetsBatchOutcome::BrokerRejected { error, .. } => {
                    ListShareGroupOffsetsTerminal::BrokerRejected(error)
                }
            };
            return Ok(self.finish(terminal));
        }
        self.outcomes.push(outcome);
        self.next_group += 1;
        if self.next_group < self.plan.queries().len() {
            return self.submit_current();
        }
        let batch = ListShareGroupsOffsetsBatch::new(
            self.maximum_throttle_time_ms,
            take(&mut self.outcomes),
        );
        Ok(self.finish(ListShareGroupOffsetsTerminal::Batch(batch)))
    }
}
