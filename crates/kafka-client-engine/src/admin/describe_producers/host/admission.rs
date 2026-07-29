//! Atomic completion and retained-byte reservation before machine creation.

use core::mem::size_of;

use kafka_client_core::{
    AdminDescribeProducerOutcome, AdminDescribeProducerTarget, AdminDescribeProducersEffect,
    AdminDescribeProducersInput, AdminDescribeProducersMachine, AdminDescribeProducersPlan, Moment,
    OperationId,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    ADMIN_DESCRIBE_PRODUCERS_CAPACITY, ADMIN_DESCRIBE_PRODUCERS_RETAINED_BYTES,
    AdminDescribeProducersAdmission, AdminDescribeProducersHandoff, AdminDescribeProducersHost,
    AdminDescribeProducersHostError, AdminDescribeProducersOperation,
};
use crate::admin::describe_producers::{
    AdminDescribeProducersAdmissionErrorKind, AdminDescribeProducersObserver,
};

impl AdminDescribeProducersHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: AdminDescribeProducersPlan,
    ) -> Result<AdminDescribeProducersAdmission, AdminDescribeProducersAdmissionErrorKind> {
        if !self.accepting {
            return Err(AdminDescribeProducersAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= ADMIN_DESCRIBE_PRODUCERS_CAPACITY {
            return Err(AdminDescribeProducersAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(AdminDescribeProducersAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge = request_owner_charge(&plan)
            .ok_or(AdminDescribeProducersAdmissionErrorKind::RetainedBytes)?;
        let remaining_result_bytes = ADMIN_DESCRIBE_PRODUCERS_RETAINED_BYTES
            .checked_sub(owner_charge)
            .ok_or(AdminDescribeProducersAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(ADMIN_DESCRIBE_PRODUCERS_RETAINED_BYTES)
            .filter(|total| *total <= ADMIN_DESCRIBE_PRODUCERS_RETAINED_BYTES)
            .ok_or(AdminDescribeProducersAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let mut operation = AdminDescribeProducersOperation {
            operation_id,
            machine: AdminDescribeProducersMachine::new(operation_id, deadline.core(), plan),
            completion_id,
            deadline,
            retained_bytes: ADMIN_DESCRIBE_PRODUCERS_RETAINED_BYTES,
            remaining_result_bytes,
            submission: None,
            handoff: AdminDescribeProducersHandoff::Untouched,
            call: None,
            raw_terminal: None,
            terminal: None,
        };
        let start_result = start(&mut operation, now, deadline);
        let terminal_ready = matches!(start_result, Ok(true));
        let mut fault = start_result.err();
        if let Some(error) = fault {
            self.health = Some(error);
        }
        self.operations.push(operation);
        if terminal_ready {
            if let Err(error) = self.publish_terminal(self.operations.len() - 1) {
                self.health = Some(error);
                fault = Some(error);
            }
        }
        Ok(AdminDescribeProducersAdmission {
            observer: AdminDescribeProducersObserver::from_completion(observer),
            fault,
        })
    }

    #[cfg(test)]
    pub(in crate::admin::describe_producers) const fn retained_bytes_for_test(&self) -> usize {
        self.retained_bytes
    }
}

fn start(
    operation: &mut AdminDescribeProducersOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, AdminDescribeProducersHostError> {
    let transition = operation
        .machine
        .apply(AdminDescribeProducersInput::Start { now })?;
    match transition.into_effect() {
        Some(AdminDescribeProducersEffect::Submit {
            operation_id,
            deadline: core_deadline,
            target,
            broker_id,
        }) if operation_id == operation.operation_id && core_deadline == deadline.core() => {
            operation.submission = Some(super::AdminDescribeProducersSubmission {
                operation_id,
                deadline,
                target,
                broker_id,
            });
            Ok(false)
        }
        Some(AdminDescribeProducersEffect::Complete {
            operation_id,
            terminal,
        }) if operation_id == operation.operation_id => {
            operation.terminal = Some(terminal);
            Ok(true)
        }
        Some(_) => Err(AdminDescribeProducersHostError::SubmissionMismatch),
        None => Err(AdminDescribeProducersHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> AdminDescribeProducersAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => AdminDescribeProducersAdmissionErrorKind::Capacity,
        _ => AdminDescribeProducersAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(plan: &AdminDescribeProducersPlan) -> Option<usize> {
    let topic_bytes = plan.targets().iter().try_fold(0usize, |total, target| {
        total.checked_add(target.topic().len())
    })?;
    size_of::<AdminDescribeProducersOperation>()
        .checked_add(size_of::<super::AdminDescribeProducersSubmission>())?
        .checked_add(2usize.checked_mul(size_of::<AdminDescribeProducersPlan>())?)?
        .checked_add(
            plan.targets()
                .len()
                .checked_mul(size_of::<AdminDescribeProducerTarget>())?,
        )?
        .checked_add(
            plan.targets()
                .len()
                .checked_mul(size_of::<AdminDescribeProducerOutcome>())?,
        )?
        .checked_add(2usize.checked_mul(topic_bytes)?)
}
