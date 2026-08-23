//! Atomic bounded ledger for decoded `ShareFetch` acquisition facts.

use crate::{ByteCount, Moment};

use super::{
    ShareAcquiredRange, ShareAcquisitionAdmissionError,
    ShareAcquisitionAdmissionErrorKind as ErrorKind, ShareAcquisitionGeneration,
    ShareAcquisitionPolicy, ShareFetchSessionFence, acquisition::ShareAcquisitionEntry,
};

/// Deterministic owner of all locally live acquisition ranges for one consumer.
#[derive(Debug, Eq, PartialEq)]
pub struct ShareAcquisitionLedger {
    pub(super) policy: ShareAcquisitionPolicy,
    pub(super) next_generation: Option<ShareAcquisitionGeneration>,
    pub(super) entries: Vec<ShareAcquisitionEntry>,
    pub(super) retained_records: u64,
    pub(super) retained_bytes: ByteCount,
}

impl ShareAcquisitionLedger {
    /// Reserves the complete configured range capacity before any broker fact is admitted.
    pub fn try_new(policy: ShareAcquisitionPolicy) -> Result<Self, ErrorKind> {
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(policy.max_ranges())
            .map_err(|_error| ErrorKind::AllocationFailed)?;
        Ok(Self {
            policy,
            next_generation: Some(ShareAcquisitionGeneration::initial()),
            entries,
            retained_records: 0,
            retained_bytes: ByteCount::new(0),
        })
    }

    /// Atomically admits a response-ordered set under one exact session fence.
    pub fn try_admit(
        &mut self,
        fence: ShareFetchSessionFence,
        now: Moment,
        ranges: Vec<ShareAcquiredRange>,
    ) -> Result<usize, ShareAcquisitionAdmissionError> {
        if ranges.is_empty() {
            return Ok(0);
        }
        let preflight = self.preflight(now, &ranges);
        let (first_generation, next_generation, records, bytes) = match preflight {
            Ok(value) => value,
            Err(kind) => return Err(ShareAcquisitionAdmissionError::new(kind, ranges)),
        };
        let admitted = ranges.len();
        let mut generation = first_generation;
        for range in ranges {
            self.entries
                .push(ShareAcquisitionEntry::staged(generation, fence, range));
            generation = match generation.checked_next() {
                Some(next) => next,
                None => generation,
            };
        }
        self.next_generation = next_generation;
        self.retained_records = records;
        self.retained_bytes = bytes;
        Ok(admitted)
    }

    /// Returns the number of live broker-lock ranges.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether no acquisition or abandoned lock remains.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns records still protected from duplicate local admission.
    pub const fn retained_records(&self) -> u64 {
        self.retained_records
    }

    /// Returns local payload bytes still owned by staged or delivered entries.
    pub const fn retained_bytes(&self) -> ByteCount {
        self.retained_bytes
    }

    fn preflight(
        &self,
        now: Moment,
        ranges: &[ShareAcquiredRange],
    ) -> Result<
        (
            ShareAcquisitionGeneration,
            Option<ShareAcquisitionGeneration>,
            u64,
            ByteCount,
        ),
        ErrorKind,
    > {
        let final_ranges = self
            .entries
            .len()
            .checked_add(ranges.len())
            .ok_or(ErrorKind::RangeCapacity)?;
        if final_ranges > self.policy.max_ranges() {
            return Err(ErrorKind::RangeCapacity);
        }

        let mut records = self.retained_records;
        let mut bytes = self.retained_bytes;
        let first_generation = self.next_generation.ok_or(ErrorKind::GenerationExhausted)?;
        let mut generation = Some(first_generation);
        for (index, range) in ranges.iter().copied().enumerate() {
            if range.lock_deadline().is_elapsed_at(now) {
                return Err(ErrorKind::ExpiredLock);
            }
            if self
                .entries
                .iter()
                .any(|entry| entry.range.conflicts_topic_identity(range))
                || ranges[..index]
                    .iter()
                    .copied()
                    .any(|candidate| candidate.conflicts_topic_identity(range))
            {
                return Err(ErrorKind::TopicIdentityMismatch);
            }
            if self.entries.iter().any(|entry| entry.range.overlaps(range))
                || ranges[..index]
                    .iter()
                    .copied()
                    .any(|candidate| candidate.overlaps(range))
            {
                return Err(ErrorKind::OverlappingRange);
            }
            records = records
                .checked_add(range.record_count())
                .ok_or(ErrorKind::RecordCapacity)?;
            if records > self.policy.max_records() {
                return Err(ErrorKind::RecordCapacity);
            }
            bytes = bytes
                .checked_add(range.retained_bytes())
                .ok_or(ErrorKind::ByteCapacity)?;
            if bytes > self.policy.max_retained_bytes() {
                return Err(ErrorKind::ByteCapacity);
            }
            generation = generation
                .ok_or(ErrorKind::GenerationExhausted)?
                .checked_next();
        }
        Ok((first_generation, generation, records, bytes))
    }
}
