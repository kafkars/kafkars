//! Conservative allocation proof for generated reassignment request and result.

use core::mem::size_of;

use kafka_client_core::{
    AlterPartitionReassignmentOutcome, AlterPartitionReassignmentResult,
    AlterPartitionReassignmentsBatch,
};
use kafka_wire::{
    AlterPartitionReassignmentsRequest,
    alter_partition_reassignments_request::{ReassignablePartition, ReassignableTopic},
};

use super::AlterPartitionReassignmentRef;

const REQUEST_SORT_ENTRY: usize = size_of::<usize>();
const BORROWED_CHANGE: usize = size_of::<AlterPartitionReassignmentRef<'static>>();
const GENERATED_TOPIC: usize = size_of::<ReassignableTopic>();
const GENERATED_PARTITION: usize = size_of::<ReassignablePartition>();
const GENERATED_REPLICA: usize = size_of::<i32>();
const OWNED_OUTCOME: usize = size_of::<AlterPartitionReassignmentOutcome>();
const OWNED_RESULT: usize = size_of::<AlterPartitionReassignmentResult>();
const EXPECTED_SORT_ENTRY: usize = size_of::<(&'static str, i32, usize)>();
const RESPONSE_SORT_ENTRY: usize =
    size_of::<(&'static str, i32, i16, Option<&'static str>, usize)>();

/// Conservatively treats every change as a separate generated topic.
pub(crate) fn generated_request_peak_charge<'a>(
    mut changes: impl Iterator<Item = AlterPartitionReassignmentRef<'a>>,
) -> Option<usize> {
    changes.try_fold(
        size_of::<AlterPartitionReassignmentsRequest>()
            .checked_add(size_of::<Vec<usize>>())?
            .checked_add(size_of::<Vec<AlterPartitionReassignmentRef<'static>>>())?,
        |charge, change| {
            charge
                .checked_add(REQUEST_SORT_ENTRY)?
                .checked_add(BORROWED_CHANGE)?
                .checked_add(GENERATED_TOPIC)?
                .checked_add(GENERATED_PARTITION)?
                .checked_add(change.topic().len())?
                .checked_add(
                    change
                        .replicas()
                        .map_or(0, <[i32]>::len)
                        .checked_mul(GENERATED_REPLICA)?,
                )
        },
    )
}

pub(super) fn result_charge<'a>(
    mut changes: impl Iterator<Item = AlterPartitionReassignmentRef<'a>>,
    diagnostic_bytes: usize,
) -> Option<usize> {
    changes.try_fold(
        size_of::<AlterPartitionReassignmentsBatch>()
            .checked_add(size_of::<Vec<AlterPartitionReassignmentOutcome>>())?
            .checked_add(diagnostic_bytes)?,
        |charge, change| {
            charge
                .checked_add(OWNED_OUTCOME)?
                .checked_add(OWNED_RESULT)?
                .checked_add(EXPECTED_SORT_ENTRY)?
                .checked_add(RESPONSE_SORT_ENTRY)?
                .checked_add(change.topic().len())
        },
    )
}
