//! Atomic completion and retained-byte reservation before machine creation.

use core::mem::size_of;

use kafka_client_core::{
    AdminFenceProducerOutcome, AdminFenceProducersEffect, AdminFenceProducersInput,
    AdminFenceProducersMachine, AdminFenceProducersPlan, Moment, OperationId,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    ADMIN_FENCE_PRODUCERS_CAPACITY, ADMIN_FENCE_PRODUCERS_RETAINED_BYTES,
    AdminFenceProducersAdmission, AdminFenceProducersHandoff, AdminFenceProducersHost,
    AdminFenceProducersHostError, AdminFenceProducersOperation,
};
use crate::admin::fence_producers::{
    AdminFenceProducersAdmissionErrorKind, AdminFenceProducersObserver,
};

impl AdminFenceProducersHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: AdminFenceProducersPlan,
    ) -> Result<AdminFenceProducersAdmission, AdminFenceProducersAdmissionErrorKind> {
        if !self.accepting {
            return Err(AdminFenceProducersAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= ADMIN_FENCE_PRODUCERS_CAPACITY {
            return Err(AdminFenceProducersAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(AdminFenceProducersAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge = request_owner_charge(&plan)
            .ok_or(AdminFenceProducersAdmissionErrorKind::RetainedBytes)?;
        let remaining_result_bytes = transactional_id_bytes(&plan)
            .ok_or(AdminFenceProducersAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(owner_charge)
            .filter(|total| *total <= ADMIN_FENCE_PRODUCERS_RETAINED_BYTES)
            .ok_or(AdminFenceProducersAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let mut operation = AdminFenceProducersOperation {
            operation_id,
            machine: AdminFenceProducersMachine::new(operation_id, deadline.core(), plan),
            completion_id,
            deadline,
            retained_bytes: owner_charge,
            remaining_result_bytes,
            submission: None,
            handoff: AdminFenceProducersHandoff::Untouched,
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
        Ok(AdminFenceProducersAdmission {
            observer: AdminFenceProducersObserver::from_completion(observer),
            fault,
        })
    }

    #[cfg(test)]
    pub(in crate::admin::fence_producers) const fn retained_bytes_for_test(&self) -> usize {
        self.retained_bytes
    }
}

fn start(
    operation: &mut AdminFenceProducersOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, AdminFenceProducersHostError> {
    let transition = operation
        .machine
        .apply(AdminFenceProducersInput::Start { now })?;
    match transition.into_effect() {
        Some(AdminFenceProducersEffect::Submit {
            operation_id,
            deadline: core_deadline,
            transactional_id,
        }) if operation_id == operation.operation_id && core_deadline == deadline.core() => {
            operation.submission = Some(super::AdminFenceProducersSubmission {
                operation_id,
                deadline,
                transactional_id,
            });
            Ok(false)
        }
        Some(AdminFenceProducersEffect::Complete {
            operation_id,
            terminal,
        }) if operation_id == operation.operation_id => {
            operation.terminal = Some(terminal);
            Ok(true)
        }
        Some(_) => Err(AdminFenceProducersHostError::SubmissionMismatch),
        None => Err(AdminFenceProducersHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> AdminFenceProducersAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => AdminFenceProducersAdmissionErrorKind::Capacity,
        _ => AdminFenceProducersAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(plan: &AdminFenceProducersPlan) -> Option<usize> {
    let id_bytes = transactional_id_bytes(plan)?;
    size_of::<AdminFenceProducersOperation>()
        .checked_add(size_of::<super::AdminFenceProducersSubmission>())?
        .checked_add(2usize.checked_mul(size_of::<AdminFenceProducersPlan>())?)?
        .checked_add(
            plan.transactional_ids()
                .len()
                .checked_mul(size_of::<String>())?,
        )?
        .checked_add(
            plan.transactional_ids()
                .len()
                .checked_mul(size_of::<AdminFenceProducerOutcome>())?,
        )?
        .checked_add(2usize.checked_mul(id_bytes)?)
}

fn transactional_id_bytes(plan: &AdminFenceProducersPlan) -> Option<usize> {
    plan.transactional_ids()
        .iter()
        .try_fold(0usize, |total, id| total.checked_add(id.len()))
}
