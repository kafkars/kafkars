//! Linear hard reservation and exact charge for retained Fetch output.

use core::mem::size_of;
use std::sync::Arc;

use bytes::Bytes;

use super::model::{FetchBatch, FetchHeader, FetchRecord};

/// Sole non-clone owner allowed to issue reservations within one store domain.
#[derive(Debug)]
pub(crate) struct FetchReservationDomain {
    identity: Arc<()>,
}

impl FetchReservationDomain {
    /// Creates one unforgeable in-process reservation domain.
    pub(crate) fn create_store_domain() -> Self {
        Self {
            identity: Arc::new(()),
        }
    }

    /// Issues the only two tokens for one store-local reservation.
    pub(crate) fn issue_pair(
        &self,
        sequence: u64,
        bytes: usize,
    ) -> (FetchOutputReservation, FetchOutputReservation) {
        let token = || FetchOutputReservation {
            domain: Arc::clone(&self.identity),
            sequence,
            bytes,
        };
        (token(), token())
    }
}

/// Capacity acquired before one Fetch may retain an oversized first batch.
///
/// This bounds the stable, publishable application-data graph. Generated
/// response DTOs and temporary decoded records are a separate scratch domain
/// bounded by [`super::FetchDecodeLimits`].
#[derive(Debug)]
pub(crate) struct FetchOutputReservation {
    domain: Arc<()>,
    sequence: u64,
    bytes: usize,
}

impl PartialEq for FetchOutputReservation {
    fn eq(&self, other: &Self) -> bool {
        self.same_reservation(other)
    }
}

impl Eq for FetchOutputReservation {}

impl FetchOutputReservation {
    /// Reports whether this token belongs to one exact reservation.
    pub(crate) fn same_reservation(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.domain, &other.domain)
            && self.sequence == other.sequence
            && self.bytes == other.bytes
    }

    /// Reports whether this token belongs to the owner's private domain.
    pub(crate) fn same_domain(&self, owner: &FetchReservationDomain) -> bool {
        Arc::ptr_eq(&self.domain, &owner.identity)
    }

    pub(crate) const fn sequence(&self) -> u64 {
        self.sequence
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
#[derive(Debug)]
pub(super) struct FetchRetainedCharge {
    domain: Arc<()>,
    sequence: u64,
    reserved_bytes: usize,
    retained_bytes: usize,
}

impl PartialEq for FetchRetainedCharge {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.domain, &other.domain)
            && self.sequence == other.sequence
            && self.reserved_bytes == other.reserved_bytes
            && self.retained_bytes == other.retained_bytes
    }
}

impl Eq for FetchRetainedCharge {}

impl FetchRetainedCharge {
    pub(super) fn same_reservation(&self, reservation: &FetchOutputReservation) -> bool {
        Arc::ptr_eq(&self.domain, &reservation.domain)
            && self.sequence == reservation.sequence
            && self.reserved_bytes == reservation.bytes
    }

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
        domain: reservation.domain,
        sequence: reservation.sequence,
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
