//! Atomic completion and retained-envelope reservation for election.

use core::mem::size_of;

use kafka_client_core::{
    ElectLeadersEffect, ElectLeadersInput, ElectLeadersMachine, ElectLeadersPlan,
    LeaderElectionTarget, Moment, OperationId,
};

use crate::{
    admin::elect_leaders::{ElectLeadersAdmissionErrorKind, ElectLeadersObserver},
    clock::OperationDeadline,
    completion::CompletionRegistryError,
    protocol::admin::elect_leaders::{LeaderElectionRef, generated_request_peak_charge},
};

use super::{
    ELECT_LEADERS_CAPACITY, ELECT_LEADERS_RETAINED_BYTES, ElectLeadersAdmission,
    ElectLeadersHandoff, ElectLeadersHost, ElectLeadersOperation, ElectLeadersSubmission,
};

impl ElectLeadersSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        ElectLeadersPlan,
        usize,
        usize,
    ) {
        (
            self.operation_id,
            self.deadline,
            self.plan,
            self.request_scratch_limit,
            self.result_limit,
        )
    }
}

impl ElectLeadersHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: ElectLeadersPlan,
    ) -> Result<ElectLeadersAdmission, ElectLeadersAdmissionErrorKind> {
        if !self.accepting {
            return Err(ElectLeadersAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= ELECT_LEADERS_CAPACITY {
            return Err(ElectLeadersAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(ElectLeadersAdmissionErrorKind::IdentityExhausted)?;
        let owner_charge =
            request_owner_charge(&plan).ok_or(ElectLeadersAdmissionErrorKind::RetainedBytes)?;
        let generated_request_charge = match plan.selection().selected_targets() {
            None => generated_request_peak_charge(core::iter::empty()),
            Some(targets) => generated_request_peak_charge(
                targets
                    .iter()
                    .map(|target| LeaderElectionRef::new(target.topic(), target.partition())),
            ),
        }
        .ok_or(ElectLeadersAdmissionErrorKind::RetainedBytes)?;
        let result_limit = ELECT_LEADERS_RETAINED_BYTES
            .checked_sub(owner_charge)
            .and_then(|limit| limit.checked_sub(generated_request_charge))
            .filter(|limit| *limit > 0)
            .ok_or(ElectLeadersAdmissionErrorKind::RetainedBytes)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(ELECT_LEADERS_RETAINED_BYTES)
            .filter(|total| *total <= ELECT_LEADERS_RETAINED_BYTES)
            .ok_or(ElectLeadersAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let response_plan = plan.clone();
        let mut operation = ElectLeadersOperation {
            operation_id,
            machine: ElectLeadersMachine::new(operation_id, deadline.core(), plan),
            response_plan,
            completion_id,
            deadline,
            retained_bytes: ELECT_LEADERS_RETAINED_BYTES,
            result_limit,
            request_scratch_limit: generated_request_charge,
            submission: None,
            handoff: ElectLeadersHandoff::Untouched,
            call: None,
            recovered_call: None,
            raw_terminal: None,
            terminal: None,
        };
        let transition = operation
            .machine
            .apply(ElectLeadersInput::Start { now })
            .map_err(|_error| ElectLeadersAdmissionErrorKind::HostUnavailable)?;
        match transition.into_effect() {
            Some(ElectLeadersEffect::Submit {
                operation_id: submitted_id,
                deadline: core_deadline,
                plan,
            }) if submitted_id == operation_id
                && core_deadline == deadline.core()
                && plan == operation.response_plan =>
            {
                operation.submission = Some(ElectLeadersSubmission {
                    operation_id,
                    deadline,
                    plan,
                    request_scratch_limit: generated_request_charge,
                    result_limit,
                });
            }
            Some(ElectLeadersEffect::Complete {
                operation_id: completed_id,
                terminal,
            }) if completed_id == operation_id => {
                operation.terminal = Some(terminal);
            }
            _ => return Err(ElectLeadersAdmissionErrorKind::HostUnavailable),
        }
        let terminal_ready = operation.terminal.is_some();
        self.operations.push(operation);
        let mut fault = None;
        if terminal_ready {
            if let Err(error) = self.publish_terminal(self.operations.len() - 1) {
                self.health = Some(error);
                fault = Some(error);
            }
        }
        Ok(ElectLeadersAdmission {
            observer: ElectLeadersObserver::from_completion(observer),
            fault,
        })
    }
}

fn reservation_error(error: CompletionRegistryError) -> ElectLeadersAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => ElectLeadersAdmissionErrorKind::Capacity,
        _ => ElectLeadersAdmissionErrorKind::HostUnavailable,
    }
}

fn request_owner_charge(plan: &ElectLeadersPlan) -> Option<usize> {
    let (target_count, payload) = match plan.selection().selected_targets() {
        None => (0, 0),
        Some(targets) => (
            targets.len(),
            targets.iter().try_fold(0usize, |bytes, target| {
                bytes.checked_add(target.topic().len())
            })?,
        ),
    };
    size_of::<ElectLeadersOperation>()
        .checked_add(size_of::<ElectLeadersSubmission>())?
        .checked_add(3usize.checked_mul(size_of::<ElectLeadersPlan>())?)?
        .checked_add(
            3usize.checked_mul(target_count.checked_mul(size_of::<LeaderElectionTarget>())?)?,
        )?
        .checked_add(3usize.checked_mul(payload)?)
}
