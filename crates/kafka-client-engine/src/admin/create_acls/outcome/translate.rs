//! Exhaustive core-to-engine Admin `CreateAcls` terminal translation.

use kafka_client_core::{
    CreateAclBinding as CoreBinding, CreateAclBrokerError as CoreBrokerError,
    CreateAclResult as CoreResult, CreateAclsFailureKind as CoreFailureKind,
    CreateAclsTerminal as CoreTerminal, DeliveryStatus as CoreDeliveryStatus,
};

use super::{
    CreateAclBrokerError, CreateAclOutcome, CreateAclResult, CreateAclsBatch,
    CreateAclsDeliveryStatus, CreateAclsFailure, CreateAclsFailureKind, CreateAclsOutcome,
};

/// Invalid host-prepared storage or an impossible core terminal shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CreateAclsTranslationError {
    PreparedOutcomesNotEmpty,
    PreparedOutcomesCapacity { required: usize, actual: usize },
    ResultCountMismatch { bindings: usize, results: usize },
}

/// Recoverable translation failure retaining both linear inputs unchanged.
pub(crate) struct CreateAclsTranslationFailure {
    error: CreateAclsTranslationError,
    terminal: CoreTerminal,
    prepared_outcomes: Vec<CreateAclOutcome>,
}

impl CreateAclsTranslationFailure {
    pub(crate) const fn error(&self) -> CreateAclsTranslationError {
        self.error
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        CreateAclsTranslationError,
        CoreTerminal,
        Vec<CreateAclOutcome>,
    ) {
        (self.error, self.terminal, self.prepared_outcomes)
    }
}

pub(crate) fn translate_terminal_into(
    terminal: CoreTerminal,
    mut prepared_outcomes: Vec<CreateAclOutcome>,
) -> Result<CreateAclsOutcome, CreateAclsTranslationFailure> {
    if let Err(error) = validate_prepared(
        &terminal,
        prepared_outcomes.len(),
        prepared_outcomes.capacity(),
    ) {
        return Err(CreateAclsTranslationFailure {
            error,
            terminal,
            prepared_outcomes,
        });
    }
    match terminal {
        CoreTerminal::Created(batch) => {
            let (throttle_time_ms, bindings, results) = batch.into_parts();
            for (binding, result) in bindings.into_iter().zip(results) {
                prepared_outcomes.push(CreateAclOutcome {
                    binding: translate_binding(binding),
                    result: translate_result(result),
                });
            }
            Ok(CreateAclsOutcome::Created(CreateAclsBatch {
                throttle_time_ms,
                outcomes: prepared_outcomes,
            }))
        }
        CoreTerminal::Failed(failure) => Ok(CreateAclsOutcome::Failed(CreateAclsFailure {
            kind: translate_failure_kind(failure.kind()),
            delivery: translate_delivery(failure.delivery()),
        })),
    }
}

fn validate_prepared(
    terminal: &CoreTerminal,
    prepared_len: usize,
    prepared_capacity: usize,
) -> Result<(), CreateAclsTranslationError> {
    if prepared_len != 0 {
        return Err(CreateAclsTranslationError::PreparedOutcomesNotEmpty);
    }
    let CoreTerminal::Created(batch) = terminal else {
        return Ok(());
    };
    let bindings = batch.bindings().len();
    let results = batch.results().len();
    if bindings != results {
        return Err(CreateAclsTranslationError::ResultCountMismatch { bindings, results });
    }
    if prepared_capacity < bindings {
        return Err(CreateAclsTranslationError::PreparedOutcomesCapacity {
            required: bindings,
            actual: prepared_capacity,
        });
    }
    Ok(())
}

fn translate_binding(binding: CoreBinding) -> super::super::CreateAclBinding {
    let (resource_type, resource_name, pattern_type, principal, host, operation, permission_type) =
        binding.into_parts();
    super::super::CreateAclBinding::new(
        resource_type,
        resource_name,
        pattern_type,
        principal,
        host,
        operation,
        permission_type,
    )
}

fn translate_result(result: CoreResult) -> CreateAclResult {
    match result {
        CoreResult::Created => CreateAclResult::Created,
        CoreResult::BrokerFailed(error) => {
            CreateAclResult::BrokerFailed(translate_broker_error(error))
        }
    }
}

fn translate_broker_error(error: CoreBrokerError) -> CreateAclBrokerError {
    let (code, message, message_truncated) = error.into_parts();
    CreateAclBrokerError {
        code,
        message,
        message_truncated,
    }
}

const fn translate_failure_kind(kind: CoreFailureKind) -> CreateAclsFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => CreateAclsFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => CreateAclsFailureKind::DriverRejected,
        CoreFailureKind::Transport => CreateAclsFailureKind::Transport,
        CoreFailureKind::ResponseTooLarge => CreateAclsFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => CreateAclsFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => CreateAclsFailureKind::InvalidResponse,
    }
}

const fn translate_delivery(status: CoreDeliveryStatus) -> CreateAclsDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => CreateAclsDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => CreateAclsDeliveryStatus::PossiblySent,
    }
}
