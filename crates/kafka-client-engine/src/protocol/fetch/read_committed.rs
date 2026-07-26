//! Response-local read-committed filtering over normalized Fetch facts.

use super::{
    control_record::{FetchControlRecordKind, decode_control_record},
    failure::FetchDecodeFailure,
    model::{FetchAbortedTransaction, FetchPartition, FetchResponse},
};

/// Hides aborted application records without changing complete-batch progress.
pub(super) fn filter_read_committed(
    response: &mut FetchResponse,
) -> Result<(), FetchDecodeFailure> {
    for topic in &mut response.topics {
        for partition in &mut topic.partitions {
            filter_partition(partition)?;
        }
    }
    Ok(())
}

fn filter_partition(partition: &mut FetchPartition) -> Result<(), FetchDecodeFailure> {
    let last_stable_offset = partition
        .last_stable_offset
        .ok_or(FetchDecodeFailure::MissingLastStableOffset)?;
    partition
        .aborted_transactions
        .sort_unstable_by_key(|transaction| (transaction.first_offset, transaction.producer_id));
    partition.aborted_transactions.dedup_by(|left, right| {
        left.first_offset == right.first_offset && left.producer_id == right.producer_id
    });
    let aborted_transactions = partition.aborted_transactions.as_slice();
    validate_aborted_transactions(aborted_transactions, last_stable_offset)?;

    let mut active = Vec::new();
    active
        .try_reserve_exact(aborted_transactions.len())
        .map_err(|_error| FetchDecodeFailure::ReadCommittedScratch {
            required: aborted_transactions.len(),
        })?;
    let mut next_aborted = 0;
    for batch in &mut partition.batches {
        if batch.last_offset >= last_stable_offset {
            return Err(FetchDecodeFailure::BatchAtOrAfterLastStableOffset {
                last_offset: batch.last_offset,
                last_stable_offset,
            });
        }
        while aborted_transactions
            .get(next_aborted)
            .is_some_and(|transaction| transaction.first_offset <= batch.last_offset)
        {
            active.push(aborted_transactions[next_aborted]);
            next_aborted += 1;
        }
        if batch.is_control && !batch.is_transactional {
            return Err(FetchDecodeFailure::NonTransactionalControlIdentity);
        }
        if batch.is_control && batch.is_transactional {
            let producer_id = batch
                .producer
                .ok_or(FetchDecodeFailure::TransactionalIdentityMissing)?
                .producer_id;
            match decode_control_record(batch).map_err(FetchDecodeFailure::ControlRecord)? {
                FetchControlRecordKind::Abort => {
                    if let Some(index) = active
                        .iter()
                        .position(|transaction| transaction.producer_id == producer_id)
                    {
                        active.remove(index);
                    }
                }
                FetchControlRecordKind::Commit => {}
                FetchControlRecordKind::Other(actual) => {
                    return Err(FetchDecodeFailure::UnsupportedControlRecordType { actual });
                }
            }
        } else if batch.is_transactional {
            let producer_id = batch
                .producer
                .ok_or(FetchDecodeFailure::TransactionalIdentityMissing)?
                .producer_id;
            if active
                .iter()
                .any(|transaction| transaction.producer_id == producer_id)
            {
                batch.records.clear();
            }
        }
    }
    Ok(())
}

fn validate_aborted_transactions(
    transactions: &[FetchAbortedTransaction],
    last_stable_offset: i64,
) -> Result<(), FetchDecodeFailure> {
    for transaction in transactions {
        if transaction.first_offset >= last_stable_offset {
            return Err(
                FetchDecodeFailure::AbortedTransactionAtOrAfterLastStableOffset {
                    first_offset: transaction.first_offset,
                    last_stable_offset,
                },
            );
        }
    }
    Ok(())
}
