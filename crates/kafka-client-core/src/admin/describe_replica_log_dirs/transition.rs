//! Exact-broker grouping, caller-order restoration, and terminal assignment.

use crate::DeliveryStatus;

use super::{
    DescribeReplicaLogDirsEffect, DescribeReplicaLogDirsFailureKind, DescribeReplicaLogDirsInput,
    DescribeReplicaLogDirsMachine, DescribeReplicaLogDirsMachineError,
    DescribeReplicaLogDirsReplicaOutcome, DescribeReplicaLogDirsReplicaPlacement,
    DescribeReplicaLogDirsState, DescribeReplicaLogDirsTransition,
};

impl DescribeReplicaLogDirsMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: DescribeReplicaLogDirsInput,
    ) -> Result<DescribeReplicaLogDirsTransition, DescribeReplicaLogDirsMachineError> {
        if self.state == DescribeReplicaLogDirsState::Completed {
            return Err(DescribeReplicaLogDirsMachineError::AlreadyCompleted);
        }
        match input {
            DescribeReplicaLogDirsInput::Start { now } => self.start(now),
            DescribeReplicaLogDirsInput::DriverAccepted => self.driver_accepted(),
            DescribeReplicaLogDirsInput::DriverRejected => self.finish_awaiting(
                DescribeReplicaLogDirsFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            DescribeReplicaLogDirsInput::DeadlineElapsed => self.finish_awaiting(
                DescribeReplicaLogDirsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            DescribeReplicaLogDirsInput::DriverDeadlineElapsed { delivery } => {
                self.finish_submitted(DescribeReplicaLogDirsFailureKind::DeadlineElapsed, delivery)
            }
            DescribeReplicaLogDirsInput::BrokerResponded {
                broker_id,
                throttle_time_ms,
                result,
            } => self.broker_responded(broker_id, throttle_time_ms, result),
            DescribeReplicaLogDirsInput::ResponseTooLarge => self.finish_submitted(
                DescribeReplicaLogDirsFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            DescribeReplicaLogDirsInput::ProtocolIncompatible { delivery } => {
                self.finish_submitted(DescribeReplicaLogDirsFailureKind::Compatibility, delivery)
            }
            DescribeReplicaLogDirsInput::TransportFailed { delivery } => {
                self.finish_submitted(DescribeReplicaLogDirsFailureKind::Transport, delivery)
            }
            DescribeReplicaLogDirsInput::InvalidResponse => self.finish_submitted(
                DescribeReplicaLogDirsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<DescribeReplicaLogDirsTransition, DescribeReplicaLogDirsMachineError> {
        if self.state != DescribeReplicaLogDirsState::Ready {
            return Err(DescribeReplicaLogDirsMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return self.finish_current_and_remaining(
                DescribeReplicaLogDirsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            );
        }
        self.submit_current()
    }

    fn submit_current(
        &mut self,
    ) -> Result<DescribeReplicaLogDirsTransition, DescribeReplicaLogDirsMachineError> {
        let broker_id = self
            .current_broker()
            .ok_or(DescribeReplicaLogDirsMachineError::InvalidState)?;
        let replicas = self.plan.replicas_for_broker(broker_id).cloned().collect();
        self.state = DescribeReplicaLogDirsState::AwaitingDriver;
        Ok(DescribeReplicaLogDirsTransition::one(
            DescribeReplicaLogDirsEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                broker_id,
                replicas,
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<DescribeReplicaLogDirsTransition, DescribeReplicaLogDirsMachineError> {
        if self.state != DescribeReplicaLogDirsState::AwaitingDriver {
            return Err(DescribeReplicaLogDirsMachineError::InvalidState);
        }
        self.state = DescribeReplicaLogDirsState::Submitted;
        Ok(DescribeReplicaLogDirsTransition::none())
    }

    fn broker_responded(
        &mut self,
        broker_id: i32,
        throttle_time_ms: u32,
        result: Result<
            Vec<DescribeReplicaLogDirsReplicaPlacement>,
            super::DescribeReplicaLogDirsBrokerError,
        >,
    ) -> Result<DescribeReplicaLogDirsTransition, DescribeReplicaLogDirsMachineError> {
        if self.state != DescribeReplicaLogDirsState::Submitted {
            return Err(DescribeReplicaLogDirsMachineError::InvalidState);
        }
        if self.current_broker() != Some(broker_id) {
            return self.finish_current_and_remaining(
                DescribeReplicaLogDirsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            );
        }
        match result {
            Ok(placements) => {
                if !self.placements_match(broker_id, &placements) {
                    return self.finish_current_and_remaining(
                        DescribeReplicaLogDirsFailureKind::InvalidResponse,
                        DeliveryStatus::PossiblySent,
                    );
                }
                let mut placements = placements.into_iter();
                for (index, replica) in self.plan.replicas().iter().enumerate() {
                    if replica.broker_id() == broker_id {
                        let (_, info) = placements
                            .next()
                            .ok_or(DescribeReplicaLogDirsMachineError::IncompleteOutcome)?
                            .into_parts();
                        self.outcomes[index] = Some(
                            DescribeReplicaLogDirsReplicaOutcome::described(replica.clone(), info),
                        );
                    }
                }
            }
            Err(error) => {
                for (index, replica) in self.plan.replicas().iter().enumerate() {
                    if replica.broker_id() == broker_id {
                        self.outcomes[index] =
                            Some(DescribeReplicaLogDirsReplicaOutcome::broker_failed(
                                replica.clone(),
                                error,
                            ));
                    }
                }
            }
        }
        self.maximum_throttle_time_ms = self.maximum_throttle_time_ms.max(throttle_time_ms);
        self.next_broker += 1;
        if self.next_broker == self.plan.broker_ids().len() {
            return self.completed_transition();
        }
        self.submit_current()
    }

    fn placements_match(
        &self,
        broker_id: i32,
        placements: &[DescribeReplicaLogDirsReplicaPlacement],
    ) -> bool {
        let mut expected = self.plan.replicas_for_broker(broker_id);
        for placement in placements {
            if expected.next() != Some(placement.replica()) {
                return false;
            }
        }
        expected.next().is_none()
    }

    fn finish_awaiting(
        &mut self,
        kind: DescribeReplicaLogDirsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<DescribeReplicaLogDirsTransition, DescribeReplicaLogDirsMachineError> {
        if self.state != DescribeReplicaLogDirsState::AwaitingDriver {
            return Err(DescribeReplicaLogDirsMachineError::InvalidState);
        }
        self.finish_current_and_remaining(kind, delivery)
    }

    fn finish_submitted(
        &mut self,
        kind: DescribeReplicaLogDirsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<DescribeReplicaLogDirsTransition, DescribeReplicaLogDirsMachineError> {
        if self.state != DescribeReplicaLogDirsState::Submitted {
            return Err(DescribeReplicaLogDirsMachineError::InvalidState);
        }
        self.finish_current_and_remaining(kind, delivery)
    }

    fn finish_current_and_remaining(
        &mut self,
        kind: DescribeReplicaLogDirsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<DescribeReplicaLogDirsTransition, DescribeReplicaLogDirsMachineError> {
        let current = self
            .current_broker()
            .ok_or(DescribeReplicaLogDirsMachineError::InvalidState)?;
        for (index, replica) in self.plan.replicas().iter().enumerate() {
            if self.outcomes[index].is_some() {
                continue;
            }
            let failure = if replica.broker_id() == current {
                Self::failure(kind, delivery)
            } else {
                Self::failure(
                    DescribeReplicaLogDirsFailureKind::NotAttempted,
                    DeliveryStatus::NotSent,
                )
            };
            self.outcomes[index] = Some(DescribeReplicaLogDirsReplicaOutcome::operation_failed(
                replica.clone(),
                failure,
            ));
        }
        self.completed_transition()
    }
}
