//! Allocation-free retained-capacity proof for normalized reassignments.

use core::mem::size_of;

use kafka_client_core::{ListPartitionReassignmentsTerminal, PartitionReassignmentOutcome};

const BASE_RESULT_CHARGE: usize = size_of::<ListPartitionReassignmentsTerminal>();
const OWNED_ENTRY_CHARGE: usize = size_of::<PartitionReassignmentOutcome>();
const BORROWED_ENTRY_CHARGE: usize = size_of::<(&'static str, i32, usize)>();
const BORROWED_TOPIC_CHARGE: usize = size_of::<&'static str>();
const SELECTED_INDEX_CHARGE: usize = size_of::<usize>();
const OWNED_BROKER_ID_CHARGE: usize = size_of::<i32>();
const BROKER_SCRATCH_CHARGE: usize = size_of::<i32>();

pub(super) fn successful_result_charge<'a>(
    entries: impl Iterator<Item = (&'a str, usize, usize, usize)>,
    topic_count: usize,
    selected_target_count: usize,
) -> Option<(usize, usize)> {
    let mut charge = BASE_RESULT_CHARGE;
    charge = charge
        .checked_add(topic_count.checked_mul(BORROWED_TOPIC_CHARGE)?)?
        .checked_add(selected_target_count.checked_mul(SELECTED_INDEX_CHARGE)?)?;
    let mut row_count = 0usize;
    for (topic, replicas, adding, removing) in entries {
        row_count = row_count.checked_add(1)?;
        let broker_count = replicas.checked_add(adding)?.checked_add(removing)?;
        charge = charge
            .checked_add(OWNED_ENTRY_CHARGE)?
            .checked_add(BORROWED_ENTRY_CHARGE)?
            .checked_add(topic.len())?
            .checked_add(
                broker_count
                    .checked_mul(OWNED_BROKER_ID_CHARGE.checked_add(BROKER_SCRATCH_CHARGE)?)?,
            )?;
    }
    Some((row_count, charge))
}

pub(super) fn broker_error_result_charge(message_bytes: usize) -> Option<usize> {
    BASE_RESULT_CHARGE.checked_add(message_bytes)
}

#[cfg(test)]
pub(super) const MINIMUM_ENTRY_CHARGE: usize =
    OWNED_ENTRY_CHARGE + BORROWED_ENTRY_CHARGE + OWNED_BROKER_ID_CHARGE;
