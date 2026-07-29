//! Exhaustive normalized-protocol translation into deterministic core input.

mod model;

use kafka_client_core::{
    DeliveryStatus, DescribeClientQuotasEffect, DescribeClientQuotasInput,
    DescribeClientQuotasPlan, OperationId,
};

use crate::{
    driver::{
        DescribeClientQuotasCall, DescribeClientQuotasDriverFailureKind,
        DescribeClientQuotasRawTerminal, DescribeClientQuotasTerminalFact,
    },
    protocol::admin::describe_client_quotas::{
        DescribeClientQuotasResponseFailure, normalize_describe_client_quotas_response,
    },
};

use super::{DescribeClientQuotasHandoff, DescribeClientQuotasHost, DescribeClientQuotasHostError};
use model::normalized_input;

impl DescribeClientQuotasHost {
    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: DescribeClientQuotasCall,
    ) -> Result<(), DescribeClientQuotasHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeClientQuotasHostError::UnknownOperation)?;
        let operation = &self.operations[index];
        if operation.handoff != DescribeClientQuotasHandoff::HandedOff
            || operation.call.is_some()
            || operation.recovered_call.is_some()
            || operation.raw_terminal.is_some()
            || operation.rejected_submission.is_some()
        {
            return Err(DescribeClientQuotasHostError::InvalidHandoff);
        }
        let matches = call.matches(
            &operation.plan,
            operation.request_scratch_limit,
            operation.result_limit,
        );
        self.operations[index].call = Some(call);
        if !matches {
            return Err(DescribeClientQuotasHostError::SubmissionMismatch);
        }
        self.apply(operation_id, DescribeClientQuotasInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
        plan: DescribeClientQuotasPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> Result<(), DescribeClientQuotasHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeClientQuotasHostError::UnknownOperation)?;
        let operation = &self.operations[index];
        if operation.handoff != DescribeClientQuotasHandoff::HandedOff
            || operation.call.is_some()
            || operation.recovered_call.is_some()
            || operation.raw_terminal.is_some()
            || operation.rejected_submission.is_some()
        {
            return Err(DescribeClientQuotasHostError::InvalidHandoff);
        }
        let matches = operation.plan == plan
            && operation.request_scratch_limit == request_scratch_limit
            && operation.result_limit == result_limit;
        self.operations[index].rejected_submission =
            Some((plan, request_scratch_limit, result_limit));
        if !matches {
            return Err(DescribeClientQuotasHostError::SubmissionMismatch);
        }
        let transition = self.operations[index]
            .machine
            .apply(DescribeClientQuotasInput::DriverRejected)?;
        let terminal = match transition.into_effect() {
            Some(DescribeClientQuotasEffect::Complete {
                operation_id: effect_id,
                terminal,
            }) if effect_id == operation_id => terminal,
            Some(_) => return Err(DescribeClientQuotasHostError::SubmissionMismatch),
            None => return Err(DescribeClientQuotasHostError::MissingTerminal),
        };
        drop(self.operations[index].rejected_submission.take());
        self.operations[index].terminal = Some(terminal);
        self.publish_terminal(index)
    }
}

pub(super) fn terminal_input(
    raw: &DescribeClientQuotasRawTerminal,
) -> (DescribeClientQuotasInput, usize) {
    match raw.fact() {
        DescribeClientQuotasTerminalFact::Response {
            selected_version: Some(selected_version),
            response,
        } => {
            match normalize_describe_client_quotas_response(
                selected_version,
                response,
                raw.result_limit(),
            ) {
                Ok(normalized) => {
                    let (
                        throttle_time_ms,
                        error_code,
                        error_message,
                        error_message_truncated,
                        entries,
                        retained_bytes,
                    ) = normalized.into_parts();
                    (
                        normalized_input(
                            throttle_time_ms,
                            error_code,
                            error_message,
                            error_message_truncated,
                            entries,
                        ),
                        retained_bytes,
                    )
                }
                Err(error) => (protocol_failure(error), 0),
            }
        }
        DescribeClientQuotasTerminalFact::Response {
            selected_version: None,
            ..
        } => (
            DescribeClientQuotasInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            },
            0,
        ),
        DescribeClientQuotasTerminalFact::Failed { kind, delivery } => {
            (driver_failure(kind, delivery), 0)
        }
    }
}

pub(super) const fn protocol_failure(
    error: DescribeClientQuotasResponseFailure,
) -> DescribeClientQuotasInput {
    match error {
        DescribeClientQuotasResponseFailure::UnsupportedApiVersion { .. } => {
            DescribeClientQuotasInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        }
        DescribeClientQuotasResponseFailure::RetainedBytes { .. } => {
            DescribeClientQuotasInput::ResponseTooLarge
        }
        DescribeClientQuotasResponseFailure::NegativeThrottleTime { .. }
        | DescribeClientQuotasResponseFailure::MissingEntriesOnSuccess
        | DescribeClientQuotasResponseFailure::EntriesWithTopLevelError { .. }
        | DescribeClientQuotasResponseFailure::TooManyEntries { .. }
        | DescribeClientQuotasResponseFailure::EmptyEntity
        | DescribeClientQuotasResponseFailure::TooManyEntityComponents { .. }
        | DescribeClientQuotasResponseFailure::EmptyEntityType
        | DescribeClientQuotasResponseFailure::EntityTypeTooLong { .. }
        | DescribeClientQuotasResponseFailure::EmptyEntityName
        | DescribeClientQuotasResponseFailure::EntityNameTooLong { .. }
        | DescribeClientQuotasResponseFailure::EmptyValues
        | DescribeClientQuotasResponseFailure::TooManyQuotaValues { .. }
        | DescribeClientQuotasResponseFailure::EmptyQuotaKey
        | DescribeClientQuotasResponseFailure::QuotaKeyTooLong { .. }
        | DescribeClientQuotasResponseFailure::NonFiniteQuotaValue
        | DescribeClientQuotasResponseFailure::DuplicateEntityType
        | DescribeClientQuotasResponseFailure::DuplicateQuotaKey
        | DescribeClientQuotasResponseFailure::DuplicateEntity => {
            DescribeClientQuotasInput::InvalidResponse
        }
    }
}

const fn driver_failure(
    kind: DescribeClientQuotasDriverFailureKind,
    delivery: DeliveryStatus,
) -> DescribeClientQuotasInput {
    match kind {
        DescribeClientQuotasDriverFailureKind::DeadlineElapsed => {
            DescribeClientQuotasInput::DriverDeadlineElapsed { delivery }
        }
        DescribeClientQuotasDriverFailureKind::Compatibility => {
            DescribeClientQuotasInput::ProtocolIncompatible { delivery }
        }
        DescribeClientQuotasDriverFailureKind::InvalidResponse => {
            DescribeClientQuotasInput::InvalidResponse
        }
        DescribeClientQuotasDriverFailureKind::Transport => {
            DescribeClientQuotasInput::TransportFailed { delivery }
        }
    }
}
