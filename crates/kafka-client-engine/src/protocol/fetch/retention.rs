//! Linear hard reservation and exact charge for retained Fetch output.

use core::mem::size_of;

use bytes::Bytes;

use super::model::{FetchBatch, FetchHeader, FetchRecord};

/// Capacity acquired before one Fetch may retain an oversized first batch.
///
/// This bounds the stable, publishable application-data graph. Generated
/// response DTOs and temporary decoded records are a separate scratch domain
/// bounded by [`super::FetchDecodeLimits`].
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct FetchOutputReservation {
    bytes: usize,
}

impl FetchOutputReservation {
    /// Records capacity already removed from the consumer retained-byte owner.
    ///
    /// The executor must acquire this capacity from its hard response limit,
    /// never from Kafka's soft `max_bytes` or `partition_max_bytes` fields.
    pub(crate) const fn from_acquired_capacity(bytes: usize) -> Self {
        Self { bytes }
    }

    /// Returns the hard capacity held by this linear token.
    pub(crate) const fn bytes(&self) -> usize {
        self.bytes
    }
}

/// Exact stable policy accounting retained with one normalized Fetch result.
///
/// Like `kafka-wire`'s `RetainedSize`, this counts descriptor capacity and
/// visible byte spans. It deliberately does not estimate unique backing-store
/// capacity or process RSS.
#[derive(Debug, Eq, PartialEq)]
pub(super) struct FetchRetainedCharge {
    reserved_bytes: usize,
    retained_bytes: usize,
}

impl FetchRetainedCharge {
    pub(super) const fn reserved_bytes(&self) -> usize {
        self.reserved_bytes
    }

    pub(super) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    pub(super) const fn unused_bytes(&self) -> usize {
        self.reserved_bytes - self.retained_bytes
    }
}

/// Why a hard output reservation could not become an exact retained charge.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FetchRetentionFailure {
    /// Descriptor or visible-byte accounting exceeded `usize`.
    AccountingOverflow,
    /// The normalized output exceeded capacity acquired before Fetch.
    ReservationExceeded {
        /// Exact stable accounted output charge.
        actual: usize,
        /// Hard capacity carried by the reservation.
        reserved: usize,
    },
}

pub(super) fn settle(
    reservation: FetchOutputReservation,
    batches: &[FetchBatch],
) -> Result<FetchRetainedCharge, (FetchRetentionFailure, FetchOutputReservation)> {
    let retained_bytes = match retained_bytes(batches) {
        Ok(bytes) => bytes,
        Err(failure) => return Err((failure, reservation)),
    };
    if retained_bytes > reservation.bytes {
        return Err((
            FetchRetentionFailure::ReservationExceeded {
                actual: retained_bytes,
                reserved: reservation.bytes,
            },
            reservation,
        ));
    }
    Ok(FetchRetainedCharge {
        reserved_bytes: reservation.bytes,
        retained_bytes,
    })
}

fn retained_bytes(batches: &[FetchBatch]) -> Result<usize, FetchRetentionFailure> {
    let mut bytes = slice_bytes(batches)?;
    for batch in batches {
        bytes = add(
            bytes,
            capacity_bytes::<FetchRecord>(batch.records.capacity())?,
        )?;
        for record in &batch.records {
            bytes = add(bytes, visible(record.key.as_ref()))?;
            bytes = add(bytes, visible(record.value.as_ref()))?;
            bytes = add(
                bytes,
                capacity_bytes::<FetchHeader>(record.headers.capacity())?,
            )?;
            for header in &record.headers {
                bytes = add(bytes, header.key.len())?;
                bytes = add(bytes, visible(header.value.as_ref()))?;
            }
        }
    }
    Ok(bytes)
}

fn slice_bytes<T>(values: &[T]) -> Result<usize, FetchRetentionFailure> {
    values
        .len()
        .checked_mul(size_of::<T>())
        .ok_or(FetchRetentionFailure::AccountingOverflow)
}

fn capacity_bytes<T>(capacity: usize) -> Result<usize, FetchRetentionFailure> {
    capacity
        .checked_mul(size_of::<T>())
        .ok_or(FetchRetentionFailure::AccountingOverflow)
}

fn visible(value: Option<&Bytes>) -> usize {
    value.map_or(0, Bytes::len)
}

fn add(left: usize, right: usize) -> Result<usize, FetchRetentionFailure> {
    left.checked_add(right)
        .ok_or(FetchRetentionFailure::AccountingOverflow)
}
