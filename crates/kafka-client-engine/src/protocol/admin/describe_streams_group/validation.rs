//! Structural validation before API-89 response materialization.

use kafka_wire::streams_group_describe_response::{
    Assignment, DescribedGroup, KeyValue, Member, Subtopology, TaskIds, TaskOffset, TopicInfo,
    TopologyDescription, TopologyDescriptionNode,
};
use kafka_wire_core::StrBytes;

use super::DescribeStreamsGroupProtocolFailure;

pub(super) fn validate_success_group(
    group: &DescribedGroup,
    include_authorized_operations: bool,
    include_topology_description: bool,
    selected_version: i16,
) -> Result<(), DescribeStreamsGroupProtocolFailure> {
    if group.error_message.is_some() {
        return Err(DescribeStreamsGroupProtocolFailure::DiagnosticOnSuccess);
    }
    nonempty(&group.group_state)?;
    if group.group_epoch < 0 || group.assignment_epoch < 0 {
        return Err(DescribeStreamsGroupProtocolFailure::InvalidEpoch);
    }
    if !include_authorized_operations && group.authorized_operations != i32::MIN {
        return Err(DescribeStreamsGroupProtocolFailure::UnexpectedAuthorizedOperations);
    }
    validate_topology_description_state(group, include_topology_description, selected_version)?;
    if let Some(topology) = &group.topology {
        if topology.epoch < 0 {
            return Err(DescribeStreamsGroupProtocolFailure::InvalidEpoch);
        }
        if let Some(subtopologies) = &topology.subtopologies {
            unique_by(subtopologies, |value| value.subtopology_id.as_str())?;
            for subtopology in subtopologies {
                validate_subtopology(subtopology)?;
            }
        }
    }
    unique_by(&group.members, |value| value.member_id.as_str())?;
    for member in &group.members {
        validate_member(member)?;
    }
    if let Some(description) = &group.topology_description {
        validate_topology_description(description)?;
    }
    Ok(())
}

fn validate_topology_description_state(
    group: &DescribedGroup,
    requested: bool,
    version: i16,
) -> Result<(), DescribeStreamsGroupProtocolFailure> {
    if version == 0 {
        if group.topology_description_status != 0 || group.topology_description.is_some() {
            return Err(DescribeStreamsGroupProtocolFailure::TopologyDescriptionStatusMismatch);
        }
        return Ok(());
    }
    if !requested {
        if group.topology_description_status != 0 || group.topology_description.is_some() {
            return Err(DescribeStreamsGroupProtocolFailure::TopologyDescriptionStatusMismatch);
        }
        return Ok(());
    }
    let payload_matches = match group.topology_description_status {
        1 | 2 => group.topology_description.is_none(),
        3 => group.topology_description.is_some(),
        0 => false,
        _ => group.topology_description.is_none(),
    };
    payload_matches
        .then_some(())
        .ok_or(DescribeStreamsGroupProtocolFailure::TopologyDescriptionStatusMismatch)
}

fn validate_subtopology(value: &Subtopology) -> Result<(), DescribeStreamsGroupProtocolFailure> {
    nonempty(&value.subtopology_id)?;
    unique_strings(&value.source_topics, true)?;
    unique_strings(&value.repartition_sink_topics, true)?;
    validate_topic_infos(&value.state_changelog_topics)?;
    validate_topic_infos(&value.repartition_source_topics)
}

fn validate_topic_infos(values: &[TopicInfo]) -> Result<(), DescribeStreamsGroupProtocolFailure> {
    unique_by(values, |value| value.name.as_str())?;
    for value in values {
        nonempty(&value.name)?;
        if value.partitions < 0 || value.replication_factor < 0 {
            return Err(DescribeStreamsGroupProtocolFailure::InvalidNumericValue);
        }
        validate_key_values(&value.topic_configs)?;
    }
    Ok(())
}

fn validate_member(member: &Member) -> Result<(), DescribeStreamsGroupProtocolFailure> {
    nonempty(&member.member_id)?;
    optional_nonempty(member.instance_id.as_ref())?;
    optional_nonempty(member.rack_id.as_ref())?;
    if member.member_epoch < 0 || member.topology_epoch < 0 {
        return Err(DescribeStreamsGroupProtocolFailure::InvalidEpoch);
    }
    nonempty(&member.process_id)?;
    if let Some(endpoint) = &member.user_endpoint {
        nonempty(&endpoint.host)?;
    }
    validate_key_values(&member.client_tags)?;
    validate_task_offsets(&member.task_offsets)?;
    validate_task_offsets(&member.task_end_offsets)?;
    validate_assignment(&member.assignment)?;
    validate_assignment(&member.target_assignment)
}

fn validate_key_values(values: &[KeyValue]) -> Result<(), DescribeStreamsGroupProtocolFailure> {
    unique_by(values, |value| value.key.as_str())?;
    for value in values {
        nonempty(&value.key)?;
    }
    Ok(())
}

fn validate_task_offsets(values: &[TaskOffset]) -> Result<(), DescribeStreamsGroupProtocolFailure> {
    let mut identities = try_scratch(values.len())?;
    for value in values {
        nonempty(&value.subtopology_id)?;
        if value.partition < 0 || value.offset < 0 {
            return Err(DescribeStreamsGroupProtocolFailure::InvalidNumericValue);
        }
        identities.push((value.subtopology_id.as_str(), value.partition));
    }
    identities.sort_unstable();
    reject_adjacent_duplicates(&identities)
}

fn validate_assignment(assignment: &Assignment) -> Result<(), DescribeStreamsGroupProtocolFailure> {
    validate_task_ids(&assignment.active_tasks)?;
    validate_task_ids(&assignment.standby_tasks)?;
    validate_task_ids(&assignment.warmup_tasks)
}

fn validate_task_ids(values: &[TaskIds]) -> Result<(), DescribeStreamsGroupProtocolFailure> {
    unique_by(values, |value| value.subtopology_id.as_str())?;
    for value in values {
        nonempty(&value.subtopology_id)?;
        let mut partitions = try_scratch(value.partitions.len())?;
        partitions.extend(value.partitions.iter().copied());
        if partitions.iter().any(|partition| *partition < 0) {
            return Err(DescribeStreamsGroupProtocolFailure::InvalidNumericValue);
        }
        partitions.sort_unstable();
        reject_adjacent_duplicates(&partitions)?;
    }
    Ok(())
}

fn validate_topology_description(
    value: &TopologyDescription,
) -> Result<(), DescribeStreamsGroupProtocolFailure> {
    unique_by(&value.subtopologies, |item| item.subtopology_id.as_str())?;
    for subtopology in &value.subtopologies {
        nonempty(&subtopology.subtopology_id)?;
        unique_by(&subtopology.nodes, |node| node.name.as_str())?;
        for node in &subtopology.nodes {
            validate_node(node)?;
        }
    }
    let mut stores = try_scratch(value.global_stores.len())?;
    for store in &value.global_stores {
        validate_node(&store.source)?;
        validate_node(&store.processor)?;
        stores.push((store.source.name.as_str(), store.processor.name.as_str()));
    }
    stores.sort_unstable();
    reject_adjacent_duplicates(&stores)
}

fn validate_node(
    node: &TopologyDescriptionNode,
) -> Result<(), DescribeStreamsGroupProtocolFailure> {
    nonempty(&node.name)?;
    unique_strings(&node.source_topics, true)?;
    optional_nonempty(node.sink_topic.as_ref())?;
    unique_strings(&node.stores, true)?;
    unique_strings(&node.successors, true)
}

fn unique_strings(
    values: &[StrBytes],
    reject_empty: bool,
) -> Result<(), DescribeStreamsGroupProtocolFailure> {
    if reject_empty && values.iter().any(StrBytes::is_empty) {
        return Err(DescribeStreamsGroupProtocolFailure::EmptyRequiredScalar);
    }
    let mut ordered = try_scratch(values.len())?;
    ordered.extend(values.iter().map(StrBytes::as_str));
    ordered.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    reject_adjacent_duplicates(&ordered)
}

fn unique_by<'a, T>(
    values: &'a [T],
    identity: impl Fn(&'a T) -> &'a str,
) -> Result<(), DescribeStreamsGroupProtocolFailure> {
    let mut ordered = try_scratch(values.len())?;
    ordered.extend(values.iter().map(identity));
    ordered.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    reject_adjacent_duplicates(&ordered)
}

fn reject_adjacent_duplicates<T: PartialEq>(
    values: &[T],
) -> Result<(), DescribeStreamsGroupProtocolFailure> {
    if values.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(DescribeStreamsGroupProtocolFailure::DuplicateIdentity);
    }
    Ok(())
}

fn try_scratch<T>(count: usize) -> Result<Vec<T>, DescribeStreamsGroupProtocolFailure> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|_| DescribeStreamsGroupProtocolFailure::Allocation)?;
    Ok(values)
}

fn nonempty(value: &StrBytes) -> Result<(), DescribeStreamsGroupProtocolFailure> {
    (!value.is_empty())
        .then_some(())
        .ok_or(DescribeStreamsGroupProtocolFailure::EmptyRequiredScalar)
}

fn optional_nonempty(value: Option<&StrBytes>) -> Result<(), DescribeStreamsGroupProtocolFailure> {
    value.map_or(Ok(()), nonempty)
}
