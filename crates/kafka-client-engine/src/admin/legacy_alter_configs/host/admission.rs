//! Atomic operation, terminal, and retained-byte reservation before construction.

use kafka_client_core::{
    LegacyAlterConfigsEffect, LegacyAlterConfigsInput, LegacyAlterConfigsMachine,
    LegacyAlterConfigsPlan, LegacyAlterConfigsRoute, Moment, OperationId,
};

use crate::{clock::OperationDeadline, completion::CompletionRegistryError};

use super::{
    LEGACY_ALTER_CONFIGS_CAPACITY, LEGACY_ALTER_CONFIGS_RETAINED_BYTES,
    LegacyAlterConfigsAdmission, LegacyAlterConfigsHandoff, LegacyAlterConfigsHost,
    LegacyAlterConfigsHostError, LegacyAlterConfigsOperation, LegacyAlterConfigsSubmission,
};
use crate::admin::legacy_alter_configs::{
    LegacyAlterConfigsAdmissionErrorKind, LegacyAlterConfigsObserver,
    model::LegacyAlterConfigsRetention,
};

impl LegacyAlterConfigsHost {
    pub(crate) fn try_admit(
        &mut self,
        now: Moment,
        deadline: OperationDeadline,
        plan: LegacyAlterConfigsPlan,
        retention: LegacyAlterConfigsRetention,
    ) -> Result<LegacyAlterConfigsAdmission, LegacyAlterConfigsAdmissionErrorKind> {
        if !self.accepting {
            return Err(LegacyAlterConfigsAdmissionErrorKind::Closed);
        }
        if self.operations.len() >= LEGACY_ALTER_CONFIGS_CAPACITY {
            return Err(LegacyAlterConfigsAdmissionErrorKind::Capacity);
        }
        let operation_id = self
            .next_operation_id
            .ok_or(LegacyAlterConfigsAdmissionErrorKind::IdentityExhausted)?;
        let total_bytes = self
            .retained_bytes
            .checked_add(retention.total())
            .filter(|total| *total <= LEGACY_ALTER_CONFIGS_RETAINED_BYTES)
            .ok_or(LegacyAlterConfigsAdmissionErrorKind::RetainedBytes)?;
        let result_limit =
            crate::admin::legacy_alter_configs::model::legacy_alter_configs_result_limit(&plan)
                .filter(|limit| *limit <= retention.result_limit())
                .ok_or(LegacyAlterConfigsAdmissionErrorKind::RetainedBytes)?;
        let result_contribution =
            crate::admin::legacy_alter_configs::model::legacy_alter_configs_result_contribution(
                &plan,
            )
            .ok_or(LegacyAlterConfigsAdmissionErrorKind::RetainedBytes)?;
        let (completion_id, observer) = self.completions.reserve().map_err(reservation_error)?;

        self.next_operation_id = operation_id.get().checked_add(1).map(OperationId::from_raw);
        self.retained_bytes = total_bytes;
        let mut operation = LegacyAlterConfigsOperation {
            operation_id,
            machine: LegacyAlterConfigsMachine::new(operation_id, deadline.core(), plan),
            route: None,
            plan: None,
            completion_id,
            deadline,
            retained_bytes: retention.total(),
            remaining_result_bytes: result_contribution,
            active_result_limit: result_limit,
            active_result_contribution: 0,
            submission: None,
            handoff: LegacyAlterConfigsHandoff::Untouched,
            call: None,
            recovered_call: None,
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
        if terminal_ready && let Err(error) = self.publish_terminal(self.operations.len() - 1) {
            self.health = Some(error);
            fault = Some(error);
        }
        Ok(LegacyAlterConfigsAdmission {
            observer: LegacyAlterConfigsObserver::from_completion(observer),
            fault,
        })
    }
}

fn start(
    operation: &mut LegacyAlterConfigsOperation,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<bool, LegacyAlterConfigsHostError> {
    let transition = operation
        .machine
        .apply(LegacyAlterConfigsInput::Start { now })?;
    match transition.into_effect() {
        Some(LegacyAlterConfigsEffect::Submit {
            operation_id,
            deadline: core_deadline,
            route,
            plan,
        }) => {
            if operation_id != operation.operation_id || core_deadline != deadline.core() {
                return Err(LegacyAlterConfigsHostError::SubmissionMismatch);
            }
            operation.prepare_submission(route, plan)?;
            Ok(false)
        }
        Some(LegacyAlterConfigsEffect::Complete {
            operation_id,
            terminal,
        }) => {
            if operation_id != operation.operation_id {
                return Err(LegacyAlterConfigsHostError::SubmissionMismatch);
            }
            operation.terminal = Some(terminal);
            Ok(true)
        }
        None => Err(LegacyAlterConfigsHostError::MissingSubmission),
    }
}

fn reservation_error(error: CompletionRegistryError) -> LegacyAlterConfigsAdmissionErrorKind {
    match error {
        CompletionRegistryError::Full => LegacyAlterConfigsAdmissionErrorKind::Capacity,
        _ => LegacyAlterConfigsAdmissionErrorKind::HostUnavailable,
    }
}

impl LegacyAlterConfigsOperation {
    pub(super) fn prepare_submission(
        &mut self,
        route: LegacyAlterConfigsRoute,
        plan: LegacyAlterConfigsPlan,
    ) -> Result<(), LegacyAlterConfigsHostError> {
        let result_limit = super::super::model::legacy_alter_configs_result_limit(&plan)
            .ok_or(LegacyAlterConfigsHostError::ByteAccounting)?;
        let contribution = super::super::model::legacy_alter_configs_result_contribution(&plan)
            .filter(|bytes| *bytes <= self.remaining_result_bytes)
            .ok_or(LegacyAlterConfigsHostError::ByteAccounting)?;
        self.route = Some(route);
        self.plan = Some(plan.clone());
        self.active_result_limit = result_limit;
        self.active_result_contribution = contribution;
        self.submission = Some(LegacyAlterConfigsSubmission {
            operation_id: self.operation_id,
            deadline: self.deadline,
            route,
            plan,
            result_limit,
        });
        self.handoff = LegacyAlterConfigsHandoff::Untouched;
        Ok(())
    }
}
