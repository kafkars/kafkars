//! Conservative allocation proof for generated election request and result.

use core::mem::size_of;

use kafka_client_core::{ElectLeadersBatch, LeaderElectionOutcome, LeaderElectionResult};
use kafka_wire::{ElectLeadersRequest, elect_leaders_request::TopicPartitions};

use super::LeaderElectionRef;

const REQUEST_SORT_ENTRY: usize = size_of::<usize>();
const BORROWED_TARGET: usize = size_of::<LeaderElectionRef<'static>>();
const GENERATED_TOPIC: usize = size_of::<TopicPartitions>();
const GENERATED_PARTITION: usize = size_of::<i32>();
const OWNED_OUTCOME: usize = size_of::<LeaderElectionOutcome>();
const OWNED_RESULT: usize = size_of::<LeaderElectionResult>();
const EXPECTED_SORT_ENTRY: usize = size_of::<(&'static str, i32, usize)>();
const RESPONSE_SORT_ENTRY: usize =
    size_of::<(&'static str, i32, i16, Option<&'static str>, usize)>();

/// Conservatively treats every target as a separate generated topic.
pub(crate) fn generated_request_peak_charge<'a>(
    mut targets: impl Iterator<Item = LeaderElectionRef<'a>>,
) -> Option<usize> {
    targets.try_fold(
        size_of::<ElectLeadersRequest>()
            .checked_add(size_of::<Vec<usize>>())?
            .checked_add(size_of::<Vec<LeaderElectionRef<'static>>>())?,
        |charge, target| {
            charge
                .checked_add(REQUEST_SORT_ENTRY)?
                .checked_add(BORROWED_TARGET)?
                .checked_add(GENERATED_TOPIC)?
                .checked_add(GENERATED_PARTITION)?
                .checked_add(target.topic().len())
        },
    )
}

pub(super) fn result_charge<'a>(
    mut targets: impl Iterator<Item = LeaderElectionRef<'a>>,
    diagnostic_bytes: usize,
) -> Option<usize> {
    targets.try_fold(
        size_of::<ElectLeadersBatch>()
            .checked_add(size_of::<Vec<LeaderElectionOutcome>>())?
            .checked_add(diagnostic_bytes)?,
        |charge, target| {
            charge
                .checked_add(OWNED_OUTCOME)?
                .checked_add(OWNED_RESULT)?
                .checked_add(EXPECTED_SORT_ENTRY)?
                .checked_add(RESPONSE_SORT_ENTRY)?
                .checked_add(target.topic().len())
        },
    )
}
