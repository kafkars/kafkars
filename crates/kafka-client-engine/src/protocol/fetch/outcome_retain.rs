//! Final broker or success retention after ordered Fetch classification.

use core::num::NonZeroI16;

use super::{
    FetchBrokerFailure, FetchBrokerLevel, FetchOutcome, FetchOutcomeFailure, FetchResponse,
    RejectedFetchOutcome, RetainedFetchOutcome,
    model::FetchLeader,
    outcome::reject,
    retention::{FetchOutputReservation, settle},
};

pub(super) fn retain_broker(
    level: FetchBrokerLevel,
    code: NonZeroI16,
    leader: Option<FetchLeader>,
    reservation: FetchOutputReservation,
) -> Result<RetainedFetchOutcome, RejectedFetchOutcome> {
    let charge = settle(reservation, &[]).map_err(|(failure, reservation)| {
        reject(FetchOutcomeFailure::Retention(failure), reservation)
    })?;
    Ok(RetainedFetchOutcome::new(
        None,
        FetchOutcome::BrokerFailure(FetchBrokerFailure::new(level, code, leader)),
        charge,
    ))
}

pub(super) fn retain_success(
    requested_offset: i64,
    throttle_ticks: u64,
    normalized: FetchResponse,
    reservation: FetchOutputReservation,
) -> Result<RetainedFetchOutcome, RejectedFetchOutcome> {
    let mut topics = normalized.topics.into_iter();
    let (Some(topic), None) = (topics.next(), topics.next()) else {
        return Err(reject(
            FetchOutcomeFailure::CorrelatedShapeLost,
            reservation,
        ));
    };
    let mut partitions = topic.partitions.into_iter();
    let (Some(partition), None) = (partitions.next(), partitions.next()) else {
        return Err(reject(
            FetchOutcomeFailure::CorrelatedShapeLost,
            reservation,
        ));
    };
    let next_offset = partition
        .batches
        .last()
        .map_or(requested_offset, |batch| batch.next_offset)
        .max(requested_offset);
    let mut data_batches = Vec::new();
    for mut batch in partition.batches {
        if batch.is_control {
            continue;
        }
        batch
            .records
            .retain(|record| record.offset >= requested_offset);
        if !batch.records.is_empty() {
            data_batches.push(batch);
        }
    }
    let data_batches = data_batches.into_boxed_slice();
    let charge = settle(reservation, &data_batches).map_err(|(failure, reservation)| {
        reject(FetchOutcomeFailure::Retention(failure), reservation)
    })?;
    Ok(RetainedFetchOutcome::new(
        Some(throttle_ticks),
        FetchOutcome::Success {
            next_offset,
            data_batches,
        },
        charge,
    ))
}

pub(super) fn retain_empty_success(
    requested_offset: i64,
    throttle_ticks: u64,
    reservation: FetchOutputReservation,
) -> Result<RetainedFetchOutcome, RejectedFetchOutcome> {
    let charge = settle(reservation, &[]).map_err(|(failure, reservation)| {
        reject(FetchOutcomeFailure::Retention(failure), reservation)
    })?;
    Ok(RetainedFetchOutcome::new(
        Some(throttle_ticks),
        FetchOutcome::Success {
            next_offset: requested_offset,
            data_batches: Box::default(),
        },
        charge,
    ))
}
