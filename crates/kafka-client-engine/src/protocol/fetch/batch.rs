//! Record-batch decoding and absolute engine descriptor materialization.

use bytes::Bytes;
use kafka_wire_records::{RecordBatch, RecordBatchDecode};

use super::{
    batch_model::normalize_batch,
    failure::FetchDecodeFailure,
    limits::FetchBudget,
    limits::FetchDecodeLimits,
    model::{FetchBatch, FetchRecordPayload},
};

pub(crate) fn decode_record_payload(
    bytes: Bytes,
    limits: FetchDecodeLimits,
) -> Result<FetchRecordPayload, FetchDecodeFailure> {
    let mut budget = FetchBudget::for_record_payload(bytes.len(), limits)?;
    let batches = decode_batches(bytes, 0, 0, &mut budget)?;
    Ok(FetchRecordPayload {
        batches,
        records: budget.records(),
        logical_bytes: budget.logical_record_bytes(),
    })
}

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
        let decoded = RecordBatch::decode_next(
            &mut bytes,
            budget.record_limits(),
            budget.remaining_additional_retained_payload_bytes(),
        )
        .map_err(|source| FetchDecodeFailure::RecordBatch {
            topic,
            partition,
            batch: batch_index,
            source,
        })?;
        let (decoded, additional_retained_payload_bytes) = match decoded {
            RecordBatchDecode::Complete {
                batch,
                additional_retained_payload_bytes,
            } => (batch, additional_retained_payload_bytes),
            RecordBatchDecode::PartialTrailing { .. } => break,
            _ => {
                return Err(FetchDecodeFailure::UnsupportedRecordBatchDecode {
                    topic,
                    partition,
                    batch: batch_index,
                });
            }
        };
        budget.add_batch(additional_retained_payload_bytes)?;
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
