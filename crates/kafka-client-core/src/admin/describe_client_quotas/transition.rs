//! Atomic client-quota description transitions and sole terminal assignment.

use crate::DeliveryStatus;

use super::{
    DESCRIBE_CLIENT_QUOTAS_DIAGNOSTIC_BYTES, DescribeClientQuotaEntity,
    DescribeClientQuotaEntityComponent, DescribeClientQuotaValue, DescribeClientQuotasBatch,
    DescribeClientQuotasEffect, DescribeClientQuotasFailure, DescribeClientQuotasFailureKind,
    DescribeClientQuotasInput, DescribeClientQuotasMachine, DescribeClientQuotasMachineError,
    DescribeClientQuotasState, DescribeClientQuotasTerminal, DescribeClientQuotasTransition,
};

const MAX_RESULT_STRING_BYTES: usize = i16::MAX as usize;

impl DescribeClientQuotasMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: DescribeClientQuotasInput,
    ) -> Result<DescribeClientQuotasTransition, DescribeClientQuotasMachineError> {
        if self.state == DescribeClientQuotasState::Completed {
            return Err(DescribeClientQuotasMachineError::AlreadyCompleted);
        }
        match input {
            DescribeClientQuotasInput::Start { now } => self.start(now),
            DescribeClientQuotasInput::DriverAccepted => self.driver_accepted(),
            DescribeClientQuotasInput::DriverRejected => self.finish_awaiting(
                DescribeClientQuotasFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            DescribeClientQuotasInput::DeadlineElapsed => self.finish_awaiting(
                DescribeClientQuotasFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            DescribeClientQuotasInput::DriverDeadlineElapsed { delivery } => {
                self.finish_submitted(DescribeClientQuotasFailureKind::DeadlineElapsed, delivery)
            }
            DescribeClientQuotasInput::BrokerResponded { batch } => self.broker_responded(batch),
            DescribeClientQuotasInput::BrokerRejected { error } => {
                if error
                    .message()
                    .is_some_and(|message| message.len() > DESCRIBE_CLIENT_QUOTAS_DIAGNOSTIC_BYTES)
                {
                    return self.finish_submitted(
                        DescribeClientQuotasFailureKind::InvalidResponse,
                        DeliveryStatus::PossiblySent,
                    );
                }
                self.finish_submitted(
                    DescribeClientQuotasFailureKind::Broker(error),
                    DeliveryStatus::PossiblySent,
                )
            }
            DescribeClientQuotasInput::ResponseTooLarge => self.finish_submitted(
                DescribeClientQuotasFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            DescribeClientQuotasInput::ProtocolIncompatible { delivery } => {
                self.finish_submitted(DescribeClientQuotasFailureKind::Compatibility, delivery)
            }
            DescribeClientQuotasInput::TransportFailed { delivery } => {
                self.finish_submitted(DescribeClientQuotasFailureKind::Transport, delivery)
            }
            DescribeClientQuotasInput::InvalidResponse => self.finish_submitted(
                DescribeClientQuotasFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<DescribeClientQuotasTransition, DescribeClientQuotasMachineError> {
        if self.state != DescribeClientQuotasState::Ready {
            return Err(DescribeClientQuotasMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                DescribeClientQuotasFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        self.state = DescribeClientQuotasState::AwaitingDriver;
        Ok(DescribeClientQuotasTransition::one(
            DescribeClientQuotasEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                plan: self.plan.clone(),
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<DescribeClientQuotasTransition, DescribeClientQuotasMachineError> {
        if self.state != DescribeClientQuotasState::AwaitingDriver {
            return Err(DescribeClientQuotasMachineError::InvalidState);
        }
        self.state = DescribeClientQuotasState::Submitted;
        Ok(DescribeClientQuotasTransition::none())
    }

    fn broker_responded(
        &mut self,
        mut batch: DescribeClientQuotasBatch,
    ) -> Result<DescribeClientQuotasTransition, DescribeClientQuotasMachineError> {
        if self.state != DescribeClientQuotasState::Submitted {
            return Err(DescribeClientQuotasMachineError::InvalidState);
        }
        if batch.entities().iter().any(entity_is_malformed) {
            return Ok(self.finish_failure(
                DescribeClientQuotasFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ));
        }
        batch.canonicalize();
        if batch.entities().iter().any(entity_contains_duplicates)
            || batch
                .entities()
                .windows(2)
                .any(|pair| pair[0].same_identity(&pair[1]))
        {
            return Ok(self.finish_failure(
                DescribeClientQuotasFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ));
        }
        Ok(self.finish(DescribeClientQuotasTerminal::Described(batch)))
    }

    fn finish_awaiting(
        &mut self,
        kind: DescribeClientQuotasFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<DescribeClientQuotasTransition, DescribeClientQuotasMachineError> {
        if self.state != DescribeClientQuotasState::AwaitingDriver {
            return Err(DescribeClientQuotasMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted(
        &mut self,
        kind: DescribeClientQuotasFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<DescribeClientQuotasTransition, DescribeClientQuotasMachineError> {
        if self.state != DescribeClientQuotasState::Submitted {
            return Err(DescribeClientQuotasMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_failure(
        &mut self,
        kind: DescribeClientQuotasFailureKind,
        delivery: DeliveryStatus,
    ) -> DescribeClientQuotasTransition {
        self.finish(DescribeClientQuotasTerminal::Failed(
            DescribeClientQuotasFailure::new(kind, delivery),
        ))
    }

    fn finish(&mut self, terminal: DescribeClientQuotasTerminal) -> DescribeClientQuotasTransition {
        self.state = DescribeClientQuotasState::Completed;
        DescribeClientQuotasTransition::one(DescribeClientQuotasEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}

fn entity_is_malformed(entity: &DescribeClientQuotaEntity) -> bool {
    entity.components().is_empty()
        || entity.values().is_empty()
        || entity.components().iter().any(component_is_malformed)
        || entity.values().iter().any(value_is_malformed)
}

fn component_is_malformed(component: &DescribeClientQuotaEntityComponent) -> bool {
    invalid_string(component.entity_type()) || component.entity_name().is_some_and(invalid_string)
}

fn value_is_malformed(value: &DescribeClientQuotaValue) -> bool {
    invalid_string(value.key()) || !value.value().is_finite()
}

fn entity_contains_duplicates(entity: &DescribeClientQuotaEntity) -> bool {
    entity
        .components()
        .windows(2)
        .any(|pair| pair[0].entity_type() == pair[1].entity_type())
        || entity
            .values()
            .windows(2)
            .any(|pair| pair[0].key() == pair[1].key())
}

fn invalid_string(value: &str) -> bool {
    value.is_empty() || value.len() > MAX_RESULT_STRING_BYTES
}
