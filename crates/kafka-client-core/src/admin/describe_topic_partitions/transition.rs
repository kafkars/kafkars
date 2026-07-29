//! Atomic single-submit transitions and request-correlated page completion.

mod cursor;

use crate::DeliveryStatus;

use super::{
    DescribeTopicPartitionsEffect, DescribeTopicPartitionsFailure,
    DescribeTopicPartitionsFailureKind, DescribeTopicPartitionsInput,
    DescribeTopicPartitionsMachine, DescribeTopicPartitionsMachineError,
    DescribeTopicPartitionsPage, DescribeTopicPartitionsState, DescribeTopicPartitionsTerminal,
    DescribeTopicPartitionsTransition,
};

use cursor::{next_cursor_advances, request_cursor_allows_page};

impl DescribeTopicPartitionsMachine {
    /// Applies one fact without hidden pagination, retry, cache, or cancellation.
    pub fn apply(
        &mut self,
        input: DescribeTopicPartitionsInput,
    ) -> Result<DescribeTopicPartitionsTransition, DescribeTopicPartitionsMachineError> {
        if self.state == DescribeTopicPartitionsState::Completed {
            return Err(DescribeTopicPartitionsMachineError::AlreadyCompleted);
        }
        match input {
            DescribeTopicPartitionsInput::Start { now } => self.start(now),
            DescribeTopicPartitionsInput::DriverAccepted => self.driver_accepted(),
            DescribeTopicPartitionsInput::DriverRejected => self.finish_awaiting(
                DescribeTopicPartitionsFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            DescribeTopicPartitionsInput::DeadlineElapsed => self.finish_awaiting(
                DescribeTopicPartitionsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            DescribeTopicPartitionsInput::DriverDeadlineElapsed { delivery } => self
                .finish_submitted(
                    DescribeTopicPartitionsFailureKind::DeadlineElapsed,
                    delivery,
                ),
            DescribeTopicPartitionsInput::BrokerResponded { page } => self.broker_responded(page),
            DescribeTopicPartitionsInput::ResponseTooLarge => self.finish_submitted(
                DescribeTopicPartitionsFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            DescribeTopicPartitionsInput::ProtocolIncompatible { delivery } => {
                self.finish_submitted(DescribeTopicPartitionsFailureKind::Compatibility, delivery)
            }
            DescribeTopicPartitionsInput::TransportFailed { delivery } => {
                self.finish_submitted(DescribeTopicPartitionsFailureKind::Transport, delivery)
            }
            DescribeTopicPartitionsInput::InvalidResponse => self.finish_submitted(
                DescribeTopicPartitionsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<DescribeTopicPartitionsTransition, DescribeTopicPartitionsMachineError> {
        if self.state != DescribeTopicPartitionsState::Ready {
            return Err(DescribeTopicPartitionsMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                DescribeTopicPartitionsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        self.state = DescribeTopicPartitionsState::AwaitingDriver;
        Ok(DescribeTopicPartitionsTransition::one(
            DescribeTopicPartitionsEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                plan: self.plan.clone(),
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<DescribeTopicPartitionsTransition, DescribeTopicPartitionsMachineError> {
        if self.state != DescribeTopicPartitionsState::AwaitingDriver {
            return Err(DescribeTopicPartitionsMachineError::InvalidState);
        }
        self.state = DescribeTopicPartitionsState::Submitted;
        Ok(DescribeTopicPartitionsTransition::none())
    }

    fn broker_responded(
        &mut self,
        mut page: DescribeTopicPartitionsPage,
    ) -> Result<DescribeTopicPartitionsTransition, DescribeTopicPartitionsMachineError> {
        if self.state != DescribeTopicPartitionsState::Submitted {
            return Err(DescribeTopicPartitionsMachineError::InvalidState);
        }
        if !page_matches_plan(&self.plan, &page) {
            return Ok(self.finish_failure(
                DescribeTopicPartitionsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ));
        }
        page.topics_mut().sort_unstable_by_key(|topic| {
            self.plan
                .topics()
                .iter()
                .position(|requested| requested.as_bytes() == topic.name().as_bytes())
                .unwrap_or(usize::MAX)
        });
        Ok(self.finish(DescribeTopicPartitionsTerminal::Page(page)))
    }

    fn finish_awaiting(
        &mut self,
        kind: DescribeTopicPartitionsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<DescribeTopicPartitionsTransition, DescribeTopicPartitionsMachineError> {
        if self.state != DescribeTopicPartitionsState::AwaitingDriver {
            return Err(DescribeTopicPartitionsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted(
        &mut self,
        kind: DescribeTopicPartitionsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<DescribeTopicPartitionsTransition, DescribeTopicPartitionsMachineError> {
        if self.state != DescribeTopicPartitionsState::Submitted {
            return Err(DescribeTopicPartitionsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_failure(
        &mut self,
        kind: DescribeTopicPartitionsFailureKind,
        delivery: DeliveryStatus,
    ) -> DescribeTopicPartitionsTransition {
        self.finish(DescribeTopicPartitionsTerminal::Failed(
            DescribeTopicPartitionsFailure::new(kind, delivery),
        ))
    }

    fn finish(
        &mut self,
        terminal: DescribeTopicPartitionsTerminal,
    ) -> DescribeTopicPartitionsTransition {
        self.state = DescribeTopicPartitionsState::Completed;
        DescribeTopicPartitionsTransition::one(DescribeTopicPartitionsEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}

fn page_matches_plan(
    plan: &super::DescribeTopicPartitionsPlan,
    page: &DescribeTopicPartitionsPage,
) -> bool {
    if page.partition_count() > plan.response_partition_limit() as usize {
        return false;
    }
    if page.topics().iter().any(|topic| {
        !plan
            .topics()
            .iter()
            .any(|requested| requested.as_bytes() == topic.name().as_bytes())
    }) {
        return false;
    }
    if page.next_cursor().is_some_and(|cursor| {
        !plan
            .topics()
            .iter()
            .any(|requested| requested.as_bytes() == cursor.topic_name().as_bytes())
    }) {
        return false;
    }
    request_cursor_allows_page(plan, page) && next_cursor_advances(plan, page)
}
