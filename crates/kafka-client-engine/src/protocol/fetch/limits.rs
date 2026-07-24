//! Per-response resource accounting for hostile Fetch results.

use kafka_wire::RetainedSize;
use kafka_wire_records::RecordDecodeLimits;

use super::failure::FetchDecodeFailure;

/// Explicit resource limits for normalizing one generated Fetch response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FetchDecodeLimits {
    pub(crate) max_response_retained_bytes: usize,
    pub(crate) max_response_allocations: usize,
    pub(crate) max_topics: usize,
    pub(crate) max_partitions: usize,
    pub(crate) max_endpoints: usize,
    pub(crate) max_batches: usize,
    pub(crate) max_records: usize,
    pub(crate) max_headers: usize,
    pub(crate) max_logical_record_bytes: usize,
    pub(crate) max_compressed_backing_bytes: usize,
    pub(crate) record_batch: RecordDecodeLimits,
}

impl FetchDecodeLimits {
    pub(crate) const fn new(record_batch: RecordDecodeLimits) -> Self {
        Self {
            max_response_retained_bytes: 64 * 1024 * 1024,
            max_response_allocations: 1_000_000,
            max_topics: 1_024,
            max_partitions: 16_384,
            max_endpoints: 4_096,
            max_batches: 16_384,
            max_records: 1_000_000,
            max_headers: 4_000_000,
            max_logical_record_bytes: 128 * 1024 * 1024,
            max_compressed_backing_bytes: 128 * 1024 * 1024,
            record_batch,
        }
    }
}

impl Default for FetchDecodeLimits {
    fn default() -> Self {
        Self::new(RecordDecodeLimits::default())
    }
}

#[derive(Debug)]
pub(super) struct FetchBudget {
    limits: FetchDecodeLimits,
    partitions: usize,
    batches: usize,
    records: usize,
    headers: usize,
    logical_record_bytes: usize,
    compressed_backing_bytes: usize,
}

impl FetchBudget {
    pub(super) fn start(
        response: &kafka_wire::FetchResponse,
        limits: FetchDecodeLimits,
    ) -> Result<Self, FetchDecodeFailure> {
        let retained = response.retained_size();
        check_limit(
            retained.heap_bytes(),
            limits.max_response_retained_bytes,
            |actual, limit| FetchDecodeFailure::ResponseRetainedBytes { actual, limit },
        )?;
        check_limit(
            retained.allocations(),
            limits.max_response_allocations,
            |actual, limit| FetchDecodeFailure::ResponseAllocations { actual, limit },
        )?;
        check_limit(
            response.responses.len(),
            limits.max_topics,
            |actual, limit| FetchDecodeFailure::TopicCount { actual, limit },
        )?;
        check_limit(
            response.node_endpoints.len(),
            limits.max_endpoints,
            |actual, limit| FetchDecodeFailure::EndpointCount { actual, limit },
        )?;
        Ok(Self {
            limits,
            partitions: 0,
            batches: 0,
            records: 0,
            headers: 0,
            logical_record_bytes: 0,
            compressed_backing_bytes: 0,
        })
    }

    pub(super) const fn record_limits(&self) -> RecordDecodeLimits {
        self.limits.record_batch
    }

    pub(super) fn add_partitions(&mut self, count: usize) -> Result<(), FetchDecodeFailure> {
        add_limited(
            &mut self.partitions,
            count,
            self.limits.max_partitions,
            |actual, limit| FetchDecodeFailure::PartitionCount { actual, limit },
        )
    }

    pub(super) fn add_batch(&mut self, compressed: bool) -> Result<(), FetchDecodeFailure> {
        add_limited(
            &mut self.batches,
            1,
            self.limits.max_batches,
            |actual, limit| FetchDecodeFailure::BatchCount { actual, limit },
        )?;
        if compressed {
            add_limited(
                &mut self.compressed_backing_bytes,
                self.limits.record_batch.max_decompressed_records_bytes,
                self.limits.max_compressed_backing_bytes,
                |actual, limit| FetchDecodeFailure::CompressedBackingBytes { actual, limit },
            )?;
        }
        Ok(())
    }

    pub(super) fn add_record(
        &mut self,
        header_count: usize,
        logical_bytes: usize,
    ) -> Result<(), FetchDecodeFailure> {
        add_limited(
            &mut self.records,
            1,
            self.limits.max_records,
            |actual, limit| FetchDecodeFailure::RecordCount { actual, limit },
        )?;
        add_limited(
            &mut self.headers,
            header_count,
            self.limits.max_headers,
            |actual, limit| FetchDecodeFailure::HeaderCount { actual, limit },
        )?;
        add_limited(
            &mut self.logical_record_bytes,
            logical_bytes,
            self.limits.max_logical_record_bytes,
            |actual, limit| FetchDecodeFailure::LogicalRecordBytes { actual, limit },
        )
    }
}

fn check_limit(
    actual: usize,
    limit: usize,
    failure: fn(usize, usize) -> FetchDecodeFailure,
) -> Result<(), FetchDecodeFailure> {
    if actual > limit {
        return Err(failure(actual, limit));
    }
    Ok(())
}

fn add_limited(
    current: &mut usize,
    amount: usize,
    limit: usize,
    failure: fn(usize, usize) -> FetchDecodeFailure,
) -> Result<(), FetchDecodeFailure> {
    let actual = current.checked_add(amount).unwrap_or(usize::MAX);
    check_limit(actual, limit, failure)?;
    *current = actual;
    Ok(())
}
