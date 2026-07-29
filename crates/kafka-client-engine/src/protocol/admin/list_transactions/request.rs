//! Bounded API-key 66 request materialization with an exact version floor.

use core::mem::size_of;

use kafka_wire::{ListTransactionsRequest, RetainedSize};
use kafka_wire_core::StrBytes;

use super::{
    ListTransactionsRequestPlan,
    validation::{
        LIST_TRANSACTIONS_MAX_FILTER_BYTES, LIST_TRANSACTIONS_MAX_PATTERN_BYTES,
        LIST_TRANSACTIONS_MAX_PRODUCER_FILTERS, LIST_TRANSACTIONS_MAX_REQUEST_STATE_BYTES,
        LIST_TRANSACTIONS_MAX_STATE_FILTERS,
    },
    version::list_transactions_version_floor,
};

/// Invalid filters or unavailable retained capacity before driver admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ListTransactionsRequestFailure {
    TooManyStateFilters {
        actual: usize,
        max: usize,
    },
    StateFilterTooLong {
        actual: usize,
        max: usize,
    },
    StateFilterBytesExceeded {
        required: usize,
        max: usize,
    },
    DuplicateStateFilter,
    TooManyProducerIdFilters {
        actual: usize,
        max: usize,
    },
    DuplicateProducerId {
        actual: i64,
    },
    NegativeDurationFilter {
        actual: i64,
    },
    PatternTooLong {
        actual: usize,
        max: usize,
    },
    RetainedBytes {
        required: usize,
        limit: usize,
    },
    Allocation {
        field: &'static str,
        requested: usize,
    },
}

/// Builds one canonical generated request and its required API-version floor.
pub(crate) fn list_transactions_request(
    plan: ListTransactionsRequestPlan<'_>,
    retained_limit: usize,
) -> Result<(ListTransactionsRequest, i16), ListTransactionsRequestFailure> {
    validate_request(plan)?;
    let required = request_charge(plan).unwrap_or(usize::MAX);
    ensure_limit(required, retained_limit)?;

    let mut state_filters: Vec<StrBytes> = Vec::new();
    state_filters
        .try_reserve_exact(plan.state_filters().len())
        .map_err(|_| ListTransactionsRequestFailure::Allocation {
            field: "state_filters",
            requested: plan.state_filters().len(),
        })?;
    state_filters.extend(
        plan.state_filters()
            .iter()
            .map(|state| state.as_str().into()),
    );
    state_filters.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));

    let mut producer_id_filters = Vec::new();
    producer_id_filters
        .try_reserve_exact(plan.producer_id_filters().len())
        .map_err(|_| ListTransactionsRequestFailure::Allocation {
            field: "producer_id_filters",
            requested: plan.producer_id_filters().len(),
        })?;
    producer_id_filters.extend_from_slice(plan.producer_id_filters());
    producer_id_filters.sort_unstable();

    let duration_filter = plan.duration_filter_ms().unwrap_or(-1);
    let mut request = ListTransactionsRequest::default();
    request.state_filters = state_filters;
    request.producer_id_filters = producer_id_filters;
    request.duration_filter = duration_filter;
    request.transactional_id_pattern = plan.transactional_id_pattern().map(Into::into);
    ensure_limit(request.retained_size().heap_bytes(), retained_limit)?;
    Ok((request, list_transactions_version_floor(plan)))
}

fn validate_request(
    plan: ListTransactionsRequestPlan<'_>,
) -> Result<(), ListTransactionsRequestFailure> {
    validate_state_filters(plan.state_filters())?;
    validate_producer_filters(plan.producer_id_filters())?;
    if let Some(actual) = plan.duration_filter_ms().filter(|duration| *duration < 0) {
        return Err(ListTransactionsRequestFailure::NegativeDurationFilter { actual });
    }
    if let Some(pattern) = plan.transactional_id_pattern() {
        if pattern.len() > LIST_TRANSACTIONS_MAX_PATTERN_BYTES {
            return Err(ListTransactionsRequestFailure::PatternTooLong {
                actual: pattern.len(),
                max: LIST_TRANSACTIONS_MAX_PATTERN_BYTES,
            });
        }
    }
    Ok(())
}

fn validate_state_filters(states: &[String]) -> Result<(), ListTransactionsRequestFailure> {
    if states.len() > LIST_TRANSACTIONS_MAX_STATE_FILTERS {
        return Err(ListTransactionsRequestFailure::TooManyStateFilters {
            actual: states.len(),
            max: LIST_TRANSACTIONS_MAX_STATE_FILTERS,
        });
    }
    let mut bytes = 0usize;
    for (index, state) in states.iter().enumerate() {
        if state.len() > LIST_TRANSACTIONS_MAX_REQUEST_STATE_BYTES {
            return Err(ListTransactionsRequestFailure::StateFilterTooLong {
                actual: state.len(),
                max: LIST_TRANSACTIONS_MAX_REQUEST_STATE_BYTES,
            });
        }
        bytes = bytes.checked_add(state.len()).unwrap_or(usize::MAX);
        if bytes > LIST_TRANSACTIONS_MAX_FILTER_BYTES {
            return Err(ListTransactionsRequestFailure::StateFilterBytesExceeded {
                required: bytes,
                max: LIST_TRANSACTIONS_MAX_FILTER_BYTES,
            });
        }
        if states[..index].contains(state) {
            return Err(ListTransactionsRequestFailure::DuplicateStateFilter);
        }
    }
    Ok(())
}

fn validate_producer_filters(producers: &[i64]) -> Result<(), ListTransactionsRequestFailure> {
    if producers.len() > LIST_TRANSACTIONS_MAX_PRODUCER_FILTERS {
        return Err(ListTransactionsRequestFailure::TooManyProducerIdFilters {
            actual: producers.len(),
            max: LIST_TRANSACTIONS_MAX_PRODUCER_FILTERS,
        });
    }
    for (index, producer) in producers.iter().copied().enumerate() {
        if producers[..index].contains(&producer) {
            return Err(ListTransactionsRequestFailure::DuplicateProducerId { actual: producer });
        }
    }
    Ok(())
}

fn request_charge(plan: ListTransactionsRequestPlan<'_>) -> Option<usize> {
    size_of::<ListTransactionsRequest>()
        .checked_add(
            plan.state_filters()
                .len()
                .checked_mul(size_of::<kafka_wire_core::StrBytes>())?,
        )?
        .checked_add(
            plan.producer_id_filters()
                .len()
                .checked_mul(size_of::<i64>())?,
        )?
        .checked_add(
            plan.state_filters()
                .iter()
                .try_fold(0usize, |bytes, state| bytes.checked_add(state.len()))?,
        )?
        .checked_add(plan.transactional_id_pattern().map_or(0, str::len))
}

fn ensure_limit(required: usize, limit: usize) -> Result<(), ListTransactionsRequestFailure> {
    if required > limit {
        return Err(ListTransactionsRequestFailure::RetainedBytes { required, limit });
    }
    Ok(())
}
