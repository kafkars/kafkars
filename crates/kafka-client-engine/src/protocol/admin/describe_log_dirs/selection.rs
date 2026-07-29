//! Bounded adaptation from flat core selection into generated API-key 35 shapes.

use core::mem::size_of;

use kafka_client_core::{AdminDescribeLogDirsPartition, AdminDescribeLogDirsSelection};
use kafka_wire::{DescribeLogDirsRequest, DescribeLogDirsResponse};

use super::{
    DescribeLogDirsSelectionRef, DescribeLogDirsTopicSelectionRef,
    NormalizedDescribeLogDirsResponse,
    request::{DescribeLogDirsRequestFailure, describe_log_dirs_request},
    response::{DescribeLogDirsResponseFailure, normalize_describe_log_dirs_response},
};

/// Flat-selection or generated-request construction failure before driver ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeLogDirsSelectionRequestFailure {
    RetainedBytes,
    Request(DescribeLogDirsRequestFailure),
}

/// Flat-selection or generated-response normalization failure after driver ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeLogDirsSelectionResponseFailure {
    RetainedBytes,
    Response(DescribeLogDirsResponseFailure),
}

/// Builds one nullable all-topic or explicit selected-partition request.
pub(crate) fn describe_log_dirs_request_for_selection(
    selection: &AdminDescribeLogDirsSelection,
    retained_limit: usize,
) -> Result<DescribeLogDirsRequest, DescribeLogDirsSelectionRequestFailure> {
    let Some(groups) = SelectionGroups::new(selection, retained_limit)
        .map_err(|()| DescribeLogDirsSelectionRequestFailure::RetainedBytes)?
    else {
        return describe_log_dirs_request(DescribeLogDirsSelectionRef::AllTopics, 0)
            .map_err(DescribeLogDirsSelectionRequestFailure::Request);
    };
    let references = groups
        .references()
        .map_err(|()| DescribeLogDirsSelectionRequestFailure::RetainedBytes)?;
    let request_limit = groups
        .remaining_limit(references.capacity(), retained_limit)
        .ok_or(DescribeLogDirsSelectionRequestFailure::RetainedBytes)?;
    describe_log_dirs_request(
        DescribeLogDirsSelectionRef::Selected(&references),
        request_limit,
    )
    .map_err(DescribeLogDirsSelectionRequestFailure::Request)
}

/// Normalizes one generated response against the exact flat core selection.
pub(crate) fn normalize_describe_log_dirs_response_for_selection(
    selection: &AdminDescribeLogDirsSelection,
    selected_version: i16,
    response: &DescribeLogDirsResponse,
    retained_limit: usize,
) -> Result<NormalizedDescribeLogDirsResponse, DescribeLogDirsSelectionResponseFailure> {
    let Some(groups) = SelectionGroups::new(selection, retained_limit)
        .map_err(|()| DescribeLogDirsSelectionResponseFailure::RetainedBytes)?
    else {
        return normalize_describe_log_dirs_response(
            DescribeLogDirsSelectionRef::AllTopics,
            selected_version,
            response,
            retained_limit,
        )
        .map_err(DescribeLogDirsSelectionResponseFailure::Response);
    };
    let references = groups
        .references()
        .map_err(|()| DescribeLogDirsSelectionResponseFailure::RetainedBytes)?;
    let response_limit = groups
        .remaining_limit(references.capacity(), retained_limit)
        .ok_or(DescribeLogDirsSelectionResponseFailure::RetainedBytes)?;
    normalize_describe_log_dirs_response(
        DescribeLogDirsSelectionRef::Selected(&references),
        selected_version,
        response,
        response_limit,
    )
    .map_err(DescribeLogDirsSelectionResponseFailure::Response)
}

struct SelectionGroup<'a> {
    topic: &'a str,
    partitions: Vec<i32>,
}

struct SelectionGroups<'a> {
    groups: Vec<SelectionGroup<'a>>,
}

impl<'a> SelectionGroups<'a> {
    fn new(
        selection: &'a AdminDescribeLogDirsSelection,
        retained_limit: usize,
    ) -> Result<Option<Self>, ()> {
        let Some(selected) = selection.selected_partitions() else {
            return Ok(None);
        };
        let per_partition_bytes = size_of::<SelectionGroup<'_>>()
            .checked_add(size_of::<i32>())
            .and_then(|bytes| bytes.checked_add(size_of::<DescribeLogDirsTopicSelectionRef<'_>>()))
            .ok_or(())?;
        let minimum_bytes = selected.len().checked_mul(per_partition_bytes).ok_or(())?;
        retained_limit.checked_sub(minimum_bytes).ok_or(())?;

        let mut groups: Vec<SelectionGroup<'_>> = Vec::new();
        groups.try_reserve_exact(selected.len()).map_err(|_| ())?;
        for partition in selected {
            push_partition(&mut groups, partition)?;
        }
        let grouped = Self { groups };
        grouped
            .remaining_limit(grouped.groups.len(), retained_limit)
            .ok_or(())?;
        Ok(Some(grouped))
    }

    fn references(&self) -> Result<Vec<DescribeLogDirsTopicSelectionRef<'_>>, ()> {
        let mut references = Vec::new();
        references
            .try_reserve_exact(self.groups.len())
            .map_err(|_| ())?;
        references.extend(
            self.groups
                .iter()
                .map(|group| DescribeLogDirsTopicSelectionRef::new(group.topic, &group.partitions)),
        );
        Ok(references)
    }

    fn remaining_limit(&self, reference_capacity: usize, retained_limit: usize) -> Option<usize> {
        let group_bytes = self
            .groups
            .capacity()
            .checked_mul(size_of::<SelectionGroup<'_>>())?;
        let partition_bytes = self.groups.iter().try_fold(0usize, |bytes, group| {
            bytes.checked_add(group.partitions.capacity().checked_mul(size_of::<i32>())?)
        })?;
        let reference_bytes =
            reference_capacity.checked_mul(size_of::<DescribeLogDirsTopicSelectionRef<'_>>())?;
        retained_limit.checked_sub(
            group_bytes
                .checked_add(partition_bytes)?
                .checked_add(reference_bytes)?,
        )
    }
}

fn push_partition<'a>(
    groups: &mut Vec<SelectionGroup<'a>>,
    selected: &'a AdminDescribeLogDirsPartition,
) -> Result<(), ()> {
    if let Some(group) = groups
        .iter_mut()
        .find(|group| group.topic == selected.topic())
    {
        group.partitions.try_reserve_exact(1).map_err(|_| ())?;
        group.partitions.push(selected.partition());
    } else {
        let mut partitions = Vec::new();
        partitions.try_reserve_exact(1).map_err(|_| ())?;
        partitions.push(selected.partition());
        groups.push(SelectionGroup {
            topic: selected.topic(),
            partitions,
        });
    }
    Ok(())
}
