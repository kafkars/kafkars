//! Record-batch decoding and absolute engine descriptor materialization.

use bytes::Bytes;
use kafka_wire_records::{Compression, RecordBatch};

use super::{
    batch_model::normalize_batch, failure::FetchDecodeFailure, limits::FetchBudget,
    model::FetchBatch,
};

pub(super) fn decode_batches(
    mut bytes: Bytes,
    topic: usize,
    partition: usize,
    budget: &mut FetchBudget,
) -> Result<Vec<FetchBatch>, FetchDecodeFailure> {
    let mut batches = Vec::new();
    let mut previous_last_offset = None;
    while !bytes.is_empty() {
        let batch_index = batches.len();
        let decoded =
            RecordBatch::decode(&mut bytes, budget.record_limits()).map_err(|source| {
                FetchDecodeFailure::RecordBatch {
                    topic,
                    partition,
                    batch: batch_index,
                    source,
                }
            })?;
        budget.add_batch(decoded.compression != Compression::None)?;
        let batch = normalize_batch(decoded, budget)?;
        if let Some(previous) = previous_last_offset
            && batch.base_offset <= previous
        {
            return Err(FetchDecodeFailure::BatchOffsetOverlap {
                previous_last_offset: previous,
                base_offset: batch.base_offset,
            });
        }
        previous_last_offset = Some(batch.last_offset);
        batches.push(batch);
    }
    Ok(batches)
}
