//! Allocation-free core-to-engine Admin `DeleteAcls` terminal translation.

use kafka_client_core::{
    DeleteAclBrokerError as CoreBrokerError, DeleteAclFilterResult as CoreFilterResult,
    DeleteAclMatchResult as CoreMatchResult, DeleteAclMatchingBinding as CoreMatchingBinding,
    DeleteAclsFailureKind as CoreFailureKind, DeleteAclsFilter as CoreFilter,
    DeleteAclsTerminal as CoreTerminal, DeliveryStatus as CoreDeliveryStatus,
};

use super::{
    DeleteAclBrokerError, DeleteAclFilterOutcome, DeleteAclFilterResult, DeleteAclMatchResult,
    DeleteAclMatchingBinding, DeleteAclsBatch, DeleteAclsDeliveryStatus, DeleteAclsFailure,
    DeleteAclsFailureKind, DeleteAclsOutcome, DeleteAclsPreparedOutcomes,
};

/// Invalid host-prepared storage or an impossible core terminal shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeleteAclsTranslationError {
    PreparedOutcomesNotEmpty,
    PreparedOutcomesCapacity {
        required: usize,
        actual: usize,
    },
    FilterResultCountMismatch {
        filters: usize,
        results: usize,
    },
    PreparedMatchingCount {
        required: usize,
        actual: usize,
    },
    PreparedMatchingNotEmpty {
        filter_index: usize,
    },
    PreparedMatchingCapacity {
        filter_index: usize,
        required: usize,
        actual: usize,
    },
}

/// Recoverable translation failure retaining both linear inputs unchanged.
pub(crate) struct DeleteAclsTranslationFailure {
    error: DeleteAclsTranslationError,
    terminal: CoreTerminal,
    prepared: DeleteAclsPreparedOutcomes,
}

impl DeleteAclsTranslationFailure {
    #[cfg(test)]
    pub(crate) const fn error(&self) -> DeleteAclsTranslationError {
        self.error
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        DeleteAclsTranslationError,
        CoreTerminal,
        DeleteAclsPreparedOutcomes,
    ) {
        (self.error, self.terminal, self.prepared)
    }
}

#[allow(
    clippy::result_large_err,
    reason = "translation failure must return both linear inputs intact for deterministic recovery"
)]
pub(crate) fn translate_terminal_into(
    terminal: CoreTerminal,
    prepared: DeleteAclsPreparedOutcomes,
) -> Result<DeleteAclsOutcome, DeleteAclsTranslationFailure> {
    if let Err(error) = validate_prepared(&terminal, &prepared) {
        return Err(DeleteAclsTranslationFailure {
            error,
            terminal,
            prepared,
        });
    }
    match terminal {
        CoreTerminal::Deleted(batch) => {
            let (throttle_time_ms, filters, results) = batch.into_parts();
            let DeleteAclsPreparedOutcomes {
                mut outcomes,
                matching,
            } = prepared;
            for ((filter, result), mut prepared_matching) in
                filters.into_iter().zip(results).zip(matching)
            {
                let result = match result {
                    CoreFilterResult::Matched(bindings) => {
                        for binding in bindings {
                            prepared_matching.push(translate_matching(binding));
                        }
                        DeleteAclFilterResult::Matched(prepared_matching)
                    }
                    CoreFilterResult::BrokerFailed(error) => {
                        DeleteAclFilterResult::BrokerFailed(translate_broker_error(error))
                    }
                };
                outcomes.push(DeleteAclFilterOutcome {
                    filter: translate_filter(filter),
                    result,
                });
            }
            Ok(DeleteAclsOutcome::Deleted(DeleteAclsBatch {
                throttle_time_ms,
                outcomes,
            }))
        }
        CoreTerminal::Failed(failure) => Ok(DeleteAclsOutcome::Failed(DeleteAclsFailure {
            kind: translate_failure_kind(failure.kind()),
            delivery: translate_delivery(failure.delivery()),
        })),
    }
}

fn validate_prepared(
    terminal: &CoreTerminal,
    prepared: &DeleteAclsPreparedOutcomes,
) -> Result<(), DeleteAclsTranslationError> {
    if !prepared.outcomes.is_empty() {
        return Err(DeleteAclsTranslationError::PreparedOutcomesNotEmpty);
    }
    let CoreTerminal::Deleted(batch) = terminal else {
        return Ok(());
    };
    let filters = batch.filters().len();
    let results = batch.results();
    if filters != results.len() {
        return Err(DeleteAclsTranslationError::FilterResultCountMismatch {
            filters,
            results: results.len(),
        });
    }
    if prepared.outcomes.capacity() < filters {
        return Err(DeleteAclsTranslationError::PreparedOutcomesCapacity {
            required: filters,
            actual: prepared.outcomes.capacity(),
        });
    }
    if prepared.matching.len() != filters {
        return Err(DeleteAclsTranslationError::PreparedMatchingCount {
            required: filters,
            actual: prepared.matching.len(),
        });
    }
    for (filter_index, (result, matching)) in results.iter().zip(&prepared.matching).enumerate() {
        if !matching.is_empty() {
            return Err(DeleteAclsTranslationError::PreparedMatchingNotEmpty { filter_index });
        }
        let CoreFilterResult::Matched(bindings) = result else {
            continue;
        };
        if matching.capacity() < bindings.len() {
            return Err(DeleteAclsTranslationError::PreparedMatchingCapacity {
                filter_index,
                required: bindings.len(),
                actual: matching.capacity(),
            });
        }
    }
    Ok(())
}

fn translate_filter(filter: CoreFilter) -> super::super::DeleteAclsFilter {
    let (resource_type, resource_name, pattern_type, principal, host, operation, permission_type) =
        filter.into_parts();
    super::super::DeleteAclsFilter::new(
        resource_type,
        resource_name,
        pattern_type,
        principal,
        host,
        operation,
        permission_type,
    )
}

fn translate_matching(binding: CoreMatchingBinding) -> DeleteAclMatchingBinding {
    let (
        resource_type,
        resource_name,
        pattern_type,
        principal,
        host,
        operation,
        permission_type,
        result,
    ) = binding.into_parts();
    DeleteAclMatchingBinding {
        resource_type,
        resource_name,
        pattern_type,
        principal,
        host,
        operation,
        permission_type,
        result: translate_match_result(result),
    }
}

fn translate_match_result(result: CoreMatchResult) -> DeleteAclMatchResult {
    match result {
        CoreMatchResult::Deleted => DeleteAclMatchResult::Deleted,
        CoreMatchResult::BrokerFailed(error) => {
            DeleteAclMatchResult::BrokerFailed(translate_broker_error(error))
        }
    }
}

fn translate_broker_error(error: CoreBrokerError) -> DeleteAclBrokerError {
    let (code, message, message_truncated) = error.into_parts();
    DeleteAclBrokerError {
        code,
        message,
        message_truncated,
    }
}

const fn translate_failure_kind(kind: CoreFailureKind) -> DeleteAclsFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => DeleteAclsFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => DeleteAclsFailureKind::DriverRejected,
        CoreFailureKind::Transport => DeleteAclsFailureKind::Transport,
        CoreFailureKind::ResponseTooLarge => DeleteAclsFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => DeleteAclsFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => DeleteAclsFailureKind::InvalidResponse,
    }
}

const fn translate_delivery(status: CoreDeliveryStatus) -> DeleteAclsDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => DeleteAclsDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => DeleteAclsDeliveryStatus::PossiblySent,
    }
}
