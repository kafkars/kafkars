//! Atomic API-92 transitions, bounded correlation, and terminal assignment.

use std::collections::BTreeMap;

use crate::DeliveryStatus;

use super::{
    DELETE_SHARE_GROUP_OFFSETS_DIAGNOSTIC_BYTES,
    DELETE_SHARE_GROUP_OFFSETS_MAX_RESPONSE_TEXT_BYTES,
    DELETE_SHARE_GROUP_OFFSETS_MAX_RETAINED_BYTES, DELETE_SHARE_GROUP_OFFSETS_MAX_TOPICS,
    DeleteShareGroupOffsetsBatch, DeleteShareGroupOffsetsBrokerError,
    DeleteShareGroupOffsetsEffect, DeleteShareGroupOffsetsFailure,
    DeleteShareGroupOffsetsFailureKind, DeleteShareGroupOffsetsInput,
    DeleteShareGroupOffsetsMachine, DeleteShareGroupOffsetsMachineError,
    DeleteShareGroupOffsetsState, DeleteShareGroupOffsetsTerminal,
    DeleteShareGroupOffsetsTopicOutcome, DeleteShareGroupOffsetsTopicResult,
    DeleteShareGroupOffsetsTransition,
};

impl DeleteShareGroupOffsetsMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: DeleteShareGroupOffsetsInput,
    ) -> Result<DeleteShareGroupOffsetsTransition, DeleteShareGroupOffsetsMachineError> {
        if self.state == DeleteShareGroupOffsetsState::Completed {
            return Err(DeleteShareGroupOffsetsMachineError::AlreadyCompleted);
        }
        match input {
            DeleteShareGroupOffsetsInput::Start { now } => self.start(now),
            DeleteShareGroupOffsetsInput::DriverAccepted => self.driver_accepted(),
            DeleteShareGroupOffsetsInput::DriverRejected => self.finish_awaiting(
                DeleteShareGroupOffsetsFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            DeleteShareGroupOffsetsInput::DeadlineElapsed => self.finish_awaiting(
                DeleteShareGroupOffsetsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            DeleteShareGroupOffsetsInput::DriverDeadlineElapsed { delivery } => self
                .finish_submitted(
                    DeleteShareGroupOffsetsFailureKind::DeadlineElapsed,
                    delivery,
                ),
            DeleteShareGroupOffsetsInput::BrokerResponded { batch } => self.broker_responded(batch),
            DeleteShareGroupOffsetsInput::BrokerRejected { error } => self.broker_rejected(error),
            DeleteShareGroupOffsetsInput::ResponseTooLarge => self.finish_submitted(
                DeleteShareGroupOffsetsFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            DeleteShareGroupOffsetsInput::ProtocolIncompatible { delivery } => {
                self.finish_submitted(DeleteShareGroupOffsetsFailureKind::Compatibility, delivery)
            }
            DeleteShareGroupOffsetsInput::TransportFailed { delivery } => {
                self.finish_submitted(DeleteShareGroupOffsetsFailureKind::Transport, delivery)
            }
            DeleteShareGroupOffsetsInput::InvalidResponse => self.finish_submitted(
                DeleteShareGroupOffsetsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<DeleteShareGroupOffsetsTransition, DeleteShareGroupOffsetsMachineError> {
        if self.state != DeleteShareGroupOffsetsState::Ready {
            return Err(DeleteShareGroupOffsetsMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                DeleteShareGroupOffsetsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        self.state = DeleteShareGroupOffsetsState::AwaitingDriver;
        Ok(DeleteShareGroupOffsetsTransition::one(
            DeleteShareGroupOffsetsEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                plan: self.plan.clone(),
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<DeleteShareGroupOffsetsTransition, DeleteShareGroupOffsetsMachineError> {
        if self.state != DeleteShareGroupOffsetsState::AwaitingDriver {
            return Err(DeleteShareGroupOffsetsMachineError::InvalidState);
        }
        self.state = DeleteShareGroupOffsetsState::Submitted;
        Ok(DeleteShareGroupOffsetsTransition::none())
    }

    fn broker_responded(
        &mut self,
        batch: DeleteShareGroupOffsetsBatch,
    ) -> Result<DeleteShareGroupOffsetsTransition, DeleteShareGroupOffsetsMachineError> {
        if self.state != DeleteShareGroupOffsetsState::Submitted {
            return Err(DeleteShareGroupOffsetsMachineError::InvalidState);
        }
        match self.correlate_batch(batch) {
            ResponseValidation::Valid(batch) => {
                Ok(self.finish(DeleteShareGroupOffsetsTerminal::Deleted(batch)))
            }
            ResponseValidation::TooLarge => Ok(self.finish_failure(
                DeleteShareGroupOffsetsFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            )),
            ResponseValidation::Invalid => Ok(self.finish_failure(
                DeleteShareGroupOffsetsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            )),
        }
    }

    fn broker_rejected(
        &mut self,
        error: DeleteShareGroupOffsetsBrokerError,
    ) -> Result<DeleteShareGroupOffsetsTransition, DeleteShareGroupOffsetsMachineError> {
        if self.state != DeleteShareGroupOffsetsState::Submitted {
            return Err(DeleteShareGroupOffsetsMachineError::InvalidState);
        }
        if !diagnostic_is_valid(error.message(), error.message_truncated()) {
            return Ok(self.finish_failure(
                DeleteShareGroupOffsetsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ));
        }
        Ok(self.finish(DeleteShareGroupOffsetsTerminal::BrokerRejected(error)))
    }

    fn correlate_batch(&self, batch: DeleteShareGroupOffsetsBatch) -> ResponseValidation {
        let (throttle_time_ms, outcomes) = batch.into_parts();
        if outcomes.len() > DELETE_SHARE_GROUP_OFFSETS_MAX_TOPICS {
            return ResponseValidation::TooLarge;
        }
        if outcomes.len() != self.plan.topics().len() {
            return ResponseValidation::Invalid;
        }

        let mut text_bytes = 0usize;
        let mut by_topic = BTreeMap::new();
        for outcome in outcomes {
            let (topic, result) = outcome.into_parts();
            if topic.is_empty() {
                return ResponseValidation::Invalid;
            }
            let diagnostic = match &result {
                DeleteShareGroupOffsetsTopicResult::Deleted(topic_id) => {
                    if *topic_id == [0; 16] {
                        return ResponseValidation::Invalid;
                    }
                    None
                }
                DeleteShareGroupOffsetsTopicResult::Failed(error) => {
                    if !diagnostic_is_valid(error.message(), error.message_truncated()) {
                        return ResponseValidation::Invalid;
                    }
                    error.message()
                }
            };
            let Some(total) = text_bytes
                .checked_add(topic.len())
                .and_then(|total| total.checked_add(diagnostic.map_or(0, str::len)))
            else {
                return ResponseValidation::TooLarge;
            };
            text_bytes = total;
            if text_bytes > DELETE_SHARE_GROUP_OFFSETS_MAX_RESPONSE_TEXT_BYTES {
                return ResponseValidation::TooLarge;
            }
            if by_topic.insert(topic, result).is_some() {
                return ResponseValidation::Invalid;
            }
        }

        let Some(outcome_bytes) = self
            .plan
            .topics()
            .len()
            .checked_mul(core::mem::size_of::<DeleteShareGroupOffsetsTopicOutcome>())
        else {
            return ResponseValidation::TooLarge;
        };
        let Some(retained_bytes) = text_bytes.checked_add(outcome_bytes) else {
            return ResponseValidation::TooLarge;
        };
        if retained_bytes > DELETE_SHARE_GROUP_OFFSETS_MAX_RETAINED_BYTES {
            return ResponseValidation::TooLarge;
        }

        let mut ordered = Vec::with_capacity(self.plan.topics().len());
        for topic in self.plan.topics() {
            let Some(result) = by_topic.remove(topic) else {
                return ResponseValidation::Invalid;
            };
            ordered.push(match result {
                DeleteShareGroupOffsetsTopicResult::Deleted(topic_id) => {
                    DeleteShareGroupOffsetsTopicOutcome::deleted(topic.clone(), topic_id)
                }
                DeleteShareGroupOffsetsTopicResult::Failed(error) => {
                    DeleteShareGroupOffsetsTopicOutcome::failed(topic.clone(), error)
                }
            });
        }
        if !by_topic.is_empty() {
            return ResponseValidation::Invalid;
        }
        ResponseValidation::Valid(DeleteShareGroupOffsetsBatch::new(throttle_time_ms, ordered))
    }

    fn finish_awaiting(
        &mut self,
        kind: DeleteShareGroupOffsetsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<DeleteShareGroupOffsetsTransition, DeleteShareGroupOffsetsMachineError> {
        if self.state != DeleteShareGroupOffsetsState::AwaitingDriver {
            return Err(DeleteShareGroupOffsetsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted(
        &mut self,
        kind: DeleteShareGroupOffsetsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<DeleteShareGroupOffsetsTransition, DeleteShareGroupOffsetsMachineError> {
        if self.state != DeleteShareGroupOffsetsState::Submitted {
            return Err(DeleteShareGroupOffsetsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_failure(
        &mut self,
        kind: DeleteShareGroupOffsetsFailureKind,
        delivery: DeliveryStatus,
    ) -> DeleteShareGroupOffsetsTransition {
        self.finish(DeleteShareGroupOffsetsTerminal::Failed(
            DeleteShareGroupOffsetsFailure::new(kind, delivery),
        ))
    }

    fn finish(
        &mut self,
        terminal: DeleteShareGroupOffsetsTerminal,
    ) -> DeleteShareGroupOffsetsTransition {
        self.state = DeleteShareGroupOffsetsState::Completed;
        DeleteShareGroupOffsetsTransition::one(DeleteShareGroupOffsetsEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}

enum ResponseValidation {
    Valid(DeleteShareGroupOffsetsBatch),
    TooLarge,
    Invalid,
}

fn diagnostic_is_valid(message: Option<&str>, message_truncated: bool) -> bool {
    message.is_none_or(|message| message.len() <= DELETE_SHARE_GROUP_OFFSETS_DIAGNOSTIC_BYTES)
        && (message.is_some() || !message_truncated)
}
