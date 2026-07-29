//! Canonicalization for API-89 topology, task, and scalar collections.

use super::Charge;
use crate::admin::describe_streams_group::{
    DescribeStreamsGroupAssignment, DescribeStreamsGroupEndpoint, DescribeStreamsGroupKeyValue,
    DescribeStreamsGroupSubtopology, DescribeStreamsGroupTaskIds, DescribeStreamsGroupTaskOffset,
    DescribeStreamsGroupTopicInfo, DescribeStreamsGroupTopology,
};

pub(super) fn canonical_topology(
    topology: DescribeStreamsGroupTopology,
    charge: &mut Charge,
) -> Option<DescribeStreamsGroupTopology> {
    let (epoch, subtopologies) = topology.into_parts();
    if epoch < 0 {
        charge.invalid = true;
        return None;
    }
    let subtopologies = match subtopologies {
        Some(values) => Some(canonical_subtopologies(values, charge)?),
        None => None,
    };
    Some(DescribeStreamsGroupTopology::new(epoch, subtopologies))
}

fn canonical_subtopologies(
    mut values: Vec<DescribeStreamsGroupSubtopology>,
    charge: &mut Charge,
) -> Option<Vec<DescribeStreamsGroupSubtopology>> {
    if !charge.items::<DescribeStreamsGroupSubtopology>(values.len()) {
        return None;
    }
    for value in &mut values {
        let (id, source, sinks, changelogs, repartitions) = value.clone().into_parts();
        if id.is_empty() || !charge.scalar(&id) {
            charge.invalid = id.is_empty();
            return None;
        }
        *value = DescribeStreamsGroupSubtopology::new(
            id,
            canonical_strings(source, charge, false)?,
            canonical_strings(sinks, charge, false)?,
            canonical_topic_infos(changelogs, charge)?,
            canonical_topic_infos(repartitions, charge)?,
        );
    }
    values.sort_unstable_by(|left, right| {
        left.subtopology_id()
            .as_bytes()
            .cmp(right.subtopology_id().as_bytes())
    });
    reject_adjacent(&values, |value| value.subtopology_id(), charge)?;
    Some(values)
}

fn canonical_topic_infos(
    mut values: Vec<DescribeStreamsGroupTopicInfo>,
    charge: &mut Charge,
) -> Option<Vec<DescribeStreamsGroupTopicInfo>> {
    if !charge.items::<DescribeStreamsGroupTopicInfo>(values.len()) {
        return None;
    }
    for value in &mut values {
        let (name, partitions, replication_factor, configs) = value.clone().into_parts();
        if name.is_empty() || partitions < 0 || replication_factor < 0 || !charge.scalar(&name) {
            charge.invalid = name.is_empty() || partitions < 0 || replication_factor < 0;
            return None;
        }
        *value = DescribeStreamsGroupTopicInfo::new(
            name,
            partitions,
            replication_factor,
            canonical_key_values(configs, charge)?,
        );
    }
    values.sort_unstable_by(|left, right| left.name().as_bytes().cmp(right.name().as_bytes()));
    reject_adjacent(&values, |value| value.name(), charge)?;
    Some(values)
}

pub(super) fn canonical_key_values(
    mut values: Vec<DescribeStreamsGroupKeyValue>,
    charge: &mut Charge,
) -> Option<Vec<DescribeStreamsGroupKeyValue>> {
    if !charge.items::<DescribeStreamsGroupKeyValue>(values.len()) {
        return None;
    }
    for value in &values {
        let (key, scalar) = value.clone().into_parts();
        if key.is_empty() || !charge.scalar(&key) || !charge.scalar(&scalar) {
            charge.invalid = key.is_empty();
            return None;
        }
    }
    values.sort_unstable_by(|left, right| left.key().as_bytes().cmp(right.key().as_bytes()));
    reject_adjacent(&values, |value| value.key(), charge)?;
    Some(values)
}

pub(super) fn canonical_endpoint(
    endpoint: DescribeStreamsGroupEndpoint,
    charge: &mut Charge,
) -> Option<DescribeStreamsGroupEndpoint> {
    let (host, port) = endpoint.into_parts();
    if host.is_empty() || port == 0 || !charge.scalar(&host) {
        charge.invalid = host.is_empty() || port == 0;
        return None;
    }
    Some(DescribeStreamsGroupEndpoint::new(host, port))
}

pub(super) fn canonical_task_offsets(
    mut values: Vec<DescribeStreamsGroupTaskOffset>,
    charge: &mut Charge,
) -> Option<Vec<DescribeStreamsGroupTaskOffset>> {
    if !charge.items::<DescribeStreamsGroupTaskOffset>(values.len()) {
        return None;
    }
    for value in &values {
        let (subtopology_id, partition, _) = value.clone().into_parts();
        if subtopology_id.is_empty() || partition < 0 || !charge.scalar(&subtopology_id) {
            charge.invalid = subtopology_id.is_empty() || partition < 0;
            return None;
        }
    }
    values.sort_unstable_by(|left, right| left.identity().cmp(&right.identity()));
    if values
        .windows(2)
        .any(|pair| pair[0].identity() == pair[1].identity())
    {
        charge.invalid = true;
        return None;
    }
    Some(values)
}

pub(super) fn canonical_assignment(
    assignment: DescribeStreamsGroupAssignment,
    charge: &mut Charge,
) -> Option<DescribeStreamsGroupAssignment> {
    let (active, standby, warmup) = assignment.into_parts();
    Some(DescribeStreamsGroupAssignment::new(
        canonical_task_ids(active, charge)?,
        canonical_task_ids(standby, charge)?,
        canonical_task_ids(warmup, charge)?,
    ))
}

fn canonical_task_ids(
    mut values: Vec<DescribeStreamsGroupTaskIds>,
    charge: &mut Charge,
) -> Option<Vec<DescribeStreamsGroupTaskIds>> {
    if !charge.items::<DescribeStreamsGroupTaskIds>(values.len()) {
        return None;
    }
    for value in &mut values {
        let (id, mut partitions) = value.clone().into_parts();
        if id.is_empty() || !charge.scalar(&id) || !charge.partition_items(partitions.len()) {
            charge.invalid = id.is_empty();
            return None;
        }
        if partitions.iter().any(|partition| *partition < 0) {
            charge.invalid = true;
            return None;
        }
        partitions.sort_unstable();
        if partitions.windows(2).any(|pair| pair[0] == pair[1]) {
            charge.invalid = true;
            return None;
        }
        *value = DescribeStreamsGroupTaskIds::new(id, partitions);
    }
    values.sort_unstable_by(|left, right| {
        left.subtopology_id()
            .as_bytes()
            .cmp(right.subtopology_id().as_bytes())
    });
    reject_adjacent(&values, |value| value.subtopology_id(), charge)?;
    Some(values)
}

pub(super) fn canonical_strings(
    mut values: Vec<String>,
    charge: &mut Charge,
    require_nonempty: bool,
) -> Option<Vec<String>> {
    if !charge.items::<String>(values.len()) {
        return None;
    }
    for value in &values {
        if (require_nonempty && value.is_empty()) || !charge.scalar(value) {
            charge.invalid = require_nonempty && value.is_empty();
            return None;
        }
    }
    values.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    if values.windows(2).any(|pair| pair[0] == pair[1]) {
        charge.invalid = true;
        return None;
    }
    Some(values)
}

fn reject_adjacent<T, F>(values: &[T], identity: F, charge: &mut Charge) -> Option<()>
where
    F: Fn(&T) -> &str,
{
    if values
        .windows(2)
        .any(|pair| identity(&pair[0]) == identity(&pair[1]))
    {
        charge.invalid = true;
        return None;
    }
    Some(())
}
