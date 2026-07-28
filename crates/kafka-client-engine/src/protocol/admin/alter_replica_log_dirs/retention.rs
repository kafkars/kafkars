//! Conservative generated request, correlation scratch, and result accounting.

use core::mem::size_of;

use kafka_client_core::AlterReplicaLogDirOutcome;
use kafka_wire::{
    AlterReplicaLogDirsRequest, RetainedSize,
    alter_replica_log_dirs_request::{AlterReplicaLogDir, AlterReplicaLogDirTopic},
};

use super::{
    AlterReplicaLogDirAssignmentRef, NormalizedAlterReplicaLogDirOutcome,
    NormalizedAlterReplicaLogDirsResponse,
};

pub(super) const MAX_TOPIC_NAME_BYTES: usize = 249;
pub(super) const MAX_LOG_DIR_PATH_BYTES: usize = i16::MAX as usize;
pub(super) const MAX_LOG_DIRS: usize = 1_024;
pub(super) const MAX_TOPIC_GROUPS: usize = 16 * 1_024;
pub(super) const MAX_ASSIGNMENTS: usize = 1_024 * 1_024;

pub(super) fn request_peak_charge(
    assignments: &[AlterReplicaLogDirAssignmentRef<'_>],
) -> Option<usize> {
    assignments.iter().try_fold(
        size_of::<AlterReplicaLogDirsRequest>()
            .checked_add(assignments.len().checked_mul(size_of::<usize>())?)?,
        |charge, assignment| {
            charge
                .checked_add(size_of::<AlterReplicaLogDir>())?
                .checked_add(size_of::<AlterReplicaLogDirTopic>())?
                .checked_add(size_of::<i32>())?
                .checked_add(assignment.log_dir().len())?
                .checked_add(assignment.topic().len())
        },
    )
}

pub(super) fn actual_request_peak_charge(
    request: &AlterReplicaLogDirsRequest,
    order_capacity: usize,
) -> Option<usize> {
    size_of::<AlterReplicaLogDirsRequest>()
        .checked_add(order_capacity.checked_mul(size_of::<usize>())?)?
        .checked_add(request.retained_size().heap_bytes())
}

pub(super) fn response_peak_charge(
    assignments: &[AlterReplicaLogDirAssignmentRef<'_>],
    returned_partition_count: usize,
) -> Option<usize> {
    assignments.iter().try_fold(
        size_of::<NormalizedAlterReplicaLogDirsResponse>()
            .checked_add(
                assignments
                    .len()
                    .checked_mul(size_of::<NormalizedAlterReplicaLogDirOutcome>())?,
            )?
            .checked_add(
                assignments
                    .len()
                    .checked_mul(size_of::<AlterReplicaLogDirOutcome>())?,
            )?
            .checked_add(
                assignments
                    .len()
                    .checked_mul(size_of::<Expected<'static>>())?,
            )?
            .checked_add(returned_partition_count.checked_mul(size_of::<Returned<'static>>())?)?,
        |charge, assignment| charge.checked_add(assignment.topic().len()),
    )
}

pub(super) fn normalized_retained_charge(
    response: &NormalizedAlterReplicaLogDirsResponse,
) -> Option<usize> {
    response.outcomes.iter().try_fold(
        size_of::<NormalizedAlterReplicaLogDirsResponse>().checked_add(
            response
                .outcomes
                .capacity()
                .checked_mul(size_of::<NormalizedAlterReplicaLogDirOutcome>())?,
        )?,
        |charge, outcome| charge.checked_add(outcome.topic.capacity()),
    )
}

pub(super) fn actual_response_peak_charge(
    response: &NormalizedAlterReplicaLogDirsResponse,
    expected_capacity: usize,
    returned_capacity: usize,
) -> Option<usize> {
    normalized_retained_charge(response)?
        .checked_add(expected_capacity.checked_mul(size_of::<Expected<'static>>())?)?
        .checked_add(returned_capacity.checked_mul(size_of::<Returned<'static>>())?)
}

#[derive(Clone, Copy)]
pub(super) struct Expected<'a> {
    pub(super) topic: &'a str,
    pub(super) partition: i32,
    pub(super) caller_index: usize,
}

#[derive(Clone, Copy)]
pub(super) struct Returned<'a> {
    pub(super) topic: &'a str,
    pub(super) partition: i32,
    pub(super) error_code: i16,
    pub(super) source_topic: usize,
}
