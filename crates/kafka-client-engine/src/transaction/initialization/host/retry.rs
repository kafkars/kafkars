//! Pure scheduling for bounded `InitProducerId` replacement attempts.

use kafka_client_core::{
    Deadline, DeliveryStatus, Moment, ProducerRetryPolicy, TransactionInitializationEffect,
    TransactionInitializationInput,
};

use super::TransactionInitializationHost;
use crate::transaction::initialization::TransactionInitializationHostError;

const COORDINATOR_LOAD_BACKOFF_TICKS: u64 = 100_000_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct TransactionInitializationRetrySchedule {
    pub(super) not_before: Deadline,
    pub(super) retries_started: u32,
}

pub(super) fn plan_retry(
    policy: ProducerRetryPolicy,
    retries_started: u32,
    now: Moment,
    deadline: Deadline,
) -> Option<TransactionInitializationRetrySchedule> {
    if retries_started >= policy.max_retries() || deadline.is_elapsed_at(now) {
        return None;
    }
    let not_before = now.checked_deadline_after(policy.backoff_ticks())?;
    if not_before >= deadline {
        return None;
    }
    Some(TransactionInitializationRetrySchedule {
        not_before,
        retries_started: retries_started.checked_add(1)?,
    })
}

pub(super) fn plan_coordinator_load_retry(
    retries_started: u32,
    now: Moment,
    deadline: Deadline,
) -> Option<TransactionInitializationRetrySchedule> {
    if deadline.is_elapsed_at(now) {
        return None;
    }
    let not_before = now.checked_deadline_after(COORDINATOR_LOAD_BACKOFF_TICKS)?;
    if not_before >= deadline {
        return None;
    }
    Some(TransactionInitializationRetrySchedule {
        not_before,
        retries_started,
    })
}

impl TransactionInitializationHost {
    pub(super) fn schedule_retry(
        &mut self,
        index: usize,
        now: Moment,
        retry: Option<(DeliveryStatus, bool)>,
    ) -> Result<bool, TransactionInitializationHostError> {
        let Some((delivery, coordinator_load)) = retry else {
            return Ok(false);
        };
        let operation = &self.operations[index];
        let schedule = if coordinator_load {
            plan_coordinator_load_retry(operation.retries_started, now, operation.deadline.core())
        } else {
            plan_retry(
                self.execution_limits.send_retry_policy(),
                operation.retries_started,
                now,
                operation.deadline.core(),
            )
        };
        let Some(schedule) = schedule else {
            return Ok(false);
        };
        let owner_id = operation.owner_id;
        let operation_id = operation.operation_id;
        let deadline = operation.deadline.core();
        let transaction_timeout_ms = operation
            .request
            .as_ref()
            .ok_or(TransactionInitializationHostError::UnknownOperation)?
            .transaction_timeout_ms();
        let transition = self.operations[index].machine.apply(
            owner_id,
            TransactionInitializationInput::RetryAuthorized { delivery },
        )?;
        match transition.into_effect() {
            Some(TransactionInitializationEffect::Submit {
                owner_id: effect_owner,
                operation_id: effect_operation,
                deadline: effect_deadline,
                plan,
            }) if effect_owner == owner_id
                && effect_operation == operation_id
                && effect_deadline == deadline
                && plan.transaction_timeout_ms() == transaction_timeout_ms => {}
            _ => return Err(TransactionInitializationHostError::UnexpectedEffect),
        }
        self.operations[index].retry_not_before = Some(schedule.not_before);
        self.operations[index].retries_started = schedule.retries_started;
        Ok(true)
    }
}
