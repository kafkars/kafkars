//! Bounded partition, payload, and acquired-range normalization for `ShareFetch`.

use core::num::NonZeroI16;

use kafka_wire::share_fetch_response::PartitionData;

use super::{
    SHARE_FETCH_MAX_PARTITIONS, SHARE_FETCH_MAX_RANGES, ShareFetchAcquiredRange,
    ShareFetchCorrelation, ShareFetchPartition, ShareFetchPartitionRejection,
    ShareFetchResponseFailure, ShareFetchResponseLimits,
};

pub(super) struct ShareFetchBudget {
    limits: ShareFetchResponseLimits,
    partitions: usize,
    ranges: usize,
    records: u64,
    bytes: usize,
}

impl ShareFetchBudget {
    pub(super) const fn new(limits: ShareFetchResponseLimits) -> Self {
        Self {
            limits,
            partitions: 0,
            ranges: 0,
            records: 0,
            bytes: 0,
        }
    }

    pub(super) fn add_partitions(&mut self, count: usize) -> Result<(), ShareFetchResponseFailure> {
        self.partitions = self.partitions.checked_add(count).ok_or(
            ShareFetchResponseFailure::PartitionCount {
                actual: usize::MAX,
                limit: SHARE_FETCH_MAX_PARTITIONS,
            },
        )?;
        if self.partitions > SHARE_FETCH_MAX_PARTITIONS {
            return Err(ShareFetchResponseFailure::PartitionCount {
                actual: self.partitions,
                limit: SHARE_FETCH_MAX_PARTITIONS,
            });
        }
        Ok(())
    }

    fn add_ranges(&mut self, count: usize) -> Result<(), ShareFetchResponseFailure> {
        self.ranges =
            self.ranges
                .checked_add(count)
                .ok_or(ShareFetchResponseFailure::RangeCount {
                    actual: usize::MAX,
                    limit: SHARE_FETCH_MAX_RANGES,
                })?;
        if self.ranges > SHARE_FETCH_MAX_RANGES {
            return Err(ShareFetchResponseFailure::RangeCount {
                actual: self.ranges,
                limit: SHARE_FETCH_MAX_RANGES,
            });
        }
        Ok(())
    }

    fn add_records(&mut self, count: u64) -> Result<(), ShareFetchResponseFailure> {
        self.records =
            self.records
                .checked_add(count)
                .ok_or(ShareFetchResponseFailure::RecordCount {
                    actual: u64::MAX,
                    limit: self.limits.max_records(),
                })?;
        if self.records > self.limits.max_records() {
            return Err(ShareFetchResponseFailure::RecordCount {
                actual: self.records,
                limit: self.limits.max_records(),
            });
        }
        Ok(())
    }

    pub(super) fn add_bytes(&mut self, count: usize) -> Result<(), ShareFetchResponseFailure> {
        self.bytes =
            self.bytes
                .checked_add(count)
                .ok_or(ShareFetchResponseFailure::RetainedBytes {
                    actual: usize::MAX,
                    limit: self.limits.max_retained_bytes(),
                })?;
        if self.bytes > self.limits.max_retained_bytes() {
            return Err(ShareFetchResponseFailure::RetainedBytes {
                actual: self.bytes,
                limit: self.limits.max_retained_bytes(),
            });
        }
        Ok(())
    }

    pub(super) const fn partitions(&self) -> usize {
        self.partitions
    }

    pub(super) const fn records(&self) -> u64 {
        self.records
    }

    pub(super) const fn bytes(&self) -> usize {
        self.bytes
    }
}

pub(super) fn normalize_partition(
    source: PartitionData,
    topic_id: [u8; 16],
    correlation: &ShareFetchCorrelation,
    budget: &mut ShareFetchBudget,
) -> Result<ShareFetchPartition, ShareFetchResponseFailure> {
    let partition = u32::try_from(source.partition_index)
        .map_err(|_| ShareFetchResponseFailure::NegativePartition(source.partition_index))?;
    if !correlation.contains(topic_id, partition) {
        return Err(ShareFetchResponseFailure::UnknownPartition(partition));
    }
    let fetch_error = NonZeroI16::new(source.error_code);
    let acknowledge_error = NonZeroI16::new(source.acknowledge_error_code);
    let current_leader = normalize_leader(
        source.current_leader.leader_id,
        source.current_leader.leader_epoch,
    )?;
    if (fetch_error.is_some() || acknowledge_error.is_some())
        && (!source.records.is_empty() || !source.acquired_records.is_empty())
    {
        return Err(ShareFetchResponseFailure::PartitionPayloadWithError);
    }
    budget.add_bytes(source.records.len())?;
    budget.add_ranges(source.acquired_records.len())?;
    let mut acquired = Vec::new();
    acquired
        .try_reserve_exact(source.acquired_records.len())
        .map_err(|_| ShareFetchResponseFailure::Allocation)?;
    for range in source.acquired_records {
        if range.first_offset < 0 || range.last_offset < range.first_offset {
            return Err(ShareFetchResponseFailure::InvalidAcquiredOffsets {
                first: range.first_offset,
                last: range.last_offset,
            });
        }
        if range.delivery_count <= 0 {
            return Err(ShareFetchResponseFailure::InvalidDeliveryCount(
                range.delivery_count,
            ));
        }
        let count = range.last_offset.unsigned_abs() - range.first_offset.unsigned_abs() + 1;
        budget.add_records(count)?;
        let normalized = ShareFetchAcquiredRange {
            first_offset: range.first_offset,
            last_offset: range.last_offset,
            delivery_count: range.delivery_count,
        };
        if acquired.iter().any(|candidate: &ShareFetchAcquiredRange| {
            candidate.first_offset <= normalized.last_offset
                && normalized.first_offset <= candidate.last_offset
        }) {
            return Err(ShareFetchResponseFailure::OverlappingAcquiredRange);
        }
        acquired.push(normalized);
    }
    let rejection = (fetch_error.is_some() || acknowledge_error.is_some()).then_some(
        ShareFetchPartitionRejection {
            fetch_error,
            acknowledge_error,
            current_leader,
        },
    );
    Ok(ShareFetchPartition {
        partition,
        rejection,
        records: source.records,
        acquired,
    })
}

fn normalize_leader(
    leader_id: i32,
    leader_epoch: i32,
) -> Result<Option<(i32, i32)>, ShareFetchResponseFailure> {
    match (leader_id, leader_epoch) {
        (-1, -1) => Ok(None),
        (leader, epoch) if leader >= 0 && epoch >= 0 => Ok(Some((leader, epoch))),
        _ => Err(ShareFetchResponseFailure::InvalidCurrentLeader {
            leader_id,
            leader_epoch,
        }),
    }
}
