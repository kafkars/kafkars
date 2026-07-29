//! Fallible deterministic materialization of one validated API-89 response.

use core::num::NonZeroI16;

use kafka_client_core::{
    DescribeStreamsGroupAssignment, DescribeStreamsGroupBrokerError,
    DescribeStreamsGroupDescription, DescribeStreamsGroupEndpoint, DescribeStreamsGroupKeyValue,
    DescribeStreamsGroupMember, DescribeStreamsGroupResult, DescribeStreamsGroupSubtopology,
    DescribeStreamsGroupTaskIds, DescribeStreamsGroupTaskOffset, DescribeStreamsGroupTopicInfo,
    DescribeStreamsGroupTopology, DescribeStreamsGroupTopologyDescription,
    DescribeStreamsGroupTopologyDescriptionGlobalStore,
    DescribeStreamsGroupTopologyDescriptionNode, DescribeStreamsGroupTopologyDescriptionStatus,
    DescribeStreamsGroupTopologyDescriptionSubtopology,
};
use kafka_wire::streams_group_describe_response::{
    Assignment, DescribedGroup, KeyValue, Member, Subtopology, TaskIds, TaskOffset, TopicInfo,
    Topology, TopologyDescription, TopologyDescriptionGlobalStore, TopologyDescriptionNode,
    TopologyDescriptionSubtopology,
};
use kafka_wire_core::StrBytes;

use super::{
    DescribeStreamsGroupProtocolFailure, NormalizedDescribeStreamsGroupResult,
    retention::{bounded_diagnostic, clone_string},
};

pub(super) fn materialize_error(
    throttle_time_ms: u32,
    group: &DescribedGroup,
) -> Result<NormalizedDescribeStreamsGroupResult, DescribeStreamsGroupProtocolFailure> {
    let code = NonZeroI16::new(group.error_code)
        .ok_or(DescribeStreamsGroupProtocolFailure::InvalidNumericValue)?;
    let (message, truncated) = bounded_diagnostic(group.error_message.as_deref())?;
    Ok(NormalizedDescribeStreamsGroupResult::Failed(
        DescribeStreamsGroupBrokerError::new(throttle_time_ms, code, message, truncated),
    ))
}

pub(super) fn materialize_success(
    throttle_time_ms: u32,
    group: &DescribedGroup,
    include_authorized_operations: bool,
    selected_version: i16,
) -> Result<NormalizedDescribeStreamsGroupResult, DescribeStreamsGroupProtocolFailure> {
    let mut members = collect_map(&group.members, materialize_member)?;
    members.sort_unstable_by(|left, right| {
        left.member_id()
            .as_bytes()
            .cmp(right.member_id().as_bytes())
    });
    let description = DescribeStreamsGroupDescription::new(
        clone_string(group.group_id.as_str())?,
        clone_string(group.group_state.as_str())?,
        group.group_epoch,
        group.assignment_epoch,
        group
            .topology
            .as_ref()
            .map(materialize_topology)
            .transpose()?,
        members,
        include_authorized_operations
            .then_some(group.authorized_operations)
            .filter(|operations| *operations != i32::MIN),
        group
            .topology_description
            .as_ref()
            .map(materialize_topology_description)
            .transpose()?,
        (selected_version == 1).then_some(DescribeStreamsGroupTopologyDescriptionStatus::new(
            group.topology_description_status,
        )),
    );
    Ok(NormalizedDescribeStreamsGroupResult::Described(
        DescribeStreamsGroupResult::new(throttle_time_ms, description),
    ))
}

fn materialize_topology(
    value: &Topology,
) -> Result<DescribeStreamsGroupTopology, DescribeStreamsGroupProtocolFailure> {
    let subtopologies = value
        .subtopologies
        .as_ref()
        .map(|values| {
            let mut values = collect_map(values, materialize_subtopology)?;
            values.sort_unstable_by(|left, right| {
                left.subtopology_id()
                    .as_bytes()
                    .cmp(right.subtopology_id().as_bytes())
            });
            Ok(values)
        })
        .transpose()?;
    Ok(DescribeStreamsGroupTopology::new(
        value.epoch,
        subtopologies,
    ))
}

fn materialize_subtopology(
    value: &Subtopology,
) -> Result<DescribeStreamsGroupSubtopology, DescribeStreamsGroupProtocolFailure> {
    Ok(DescribeStreamsGroupSubtopology::new(
        clone_string(value.subtopology_id.as_str())?,
        strings(&value.source_topics)?,
        strings(&value.repartition_sink_topics)?,
        topic_infos(&value.state_changelog_topics)?,
        topic_infos(&value.repartition_source_topics)?,
    ))
}

fn topic_infos(
    values: &[TopicInfo],
) -> Result<Vec<DescribeStreamsGroupTopicInfo>, DescribeStreamsGroupProtocolFailure> {
    let mut output = collect_map(values, |value| {
        Ok(DescribeStreamsGroupTopicInfo::new(
            clone_string(value.name.as_str())?,
            value.partitions,
            value.replication_factor,
            key_values(&value.topic_configs)?,
        ))
    })?;
    output.sort_unstable_by(
        |left: &DescribeStreamsGroupTopicInfo, right: &DescribeStreamsGroupTopicInfo| {
            left.name().as_bytes().cmp(right.name().as_bytes())
        },
    );
    Ok(output)
}

fn materialize_member(
    value: &Member,
) -> Result<DescribeStreamsGroupMember, DescribeStreamsGroupProtocolFailure> {
    Ok(DescribeStreamsGroupMember::new(
        clone_string(value.member_id.as_str())?,
        value.member_epoch,
        optional_string(value.instance_id.as_ref())?,
        optional_string(value.rack_id.as_ref())?,
        clone_string(value.client_id.as_str())?,
        clone_string(value.client_host.as_str())?,
        value.topology_epoch,
        clone_string(value.process_id.as_str())?,
        value
            .user_endpoint
            .as_ref()
            .map(|endpoint| {
                Ok(DescribeStreamsGroupEndpoint::new(
                    clone_string(endpoint.host.as_str())?,
                    endpoint.port,
                ))
            })
            .transpose()?,
        key_values(&value.client_tags)?,
        task_offsets(&value.task_offsets)?,
        task_offsets(&value.task_end_offsets)?,
        materialize_assignment(&value.assignment)?,
        materialize_assignment(&value.target_assignment)?,
        value.is_classic,
    ))
}

fn key_values(
    values: &[KeyValue],
) -> Result<Vec<DescribeStreamsGroupKeyValue>, DescribeStreamsGroupProtocolFailure> {
    let mut output = collect_map(values, |value| {
        Ok(DescribeStreamsGroupKeyValue::new(
            clone_string(value.key.as_str())?,
            clone_string(value.value.as_str())?,
        ))
    })?;
    output.sort_unstable_by(
        |left: &DescribeStreamsGroupKeyValue, right: &DescribeStreamsGroupKeyValue| {
            left.key().as_bytes().cmp(right.key().as_bytes())
        },
    );
    Ok(output)
}

fn task_offsets(
    values: &[TaskOffset],
) -> Result<Vec<DescribeStreamsGroupTaskOffset>, DescribeStreamsGroupProtocolFailure> {
    let mut output = collect_map(values, |value| {
        Ok(DescribeStreamsGroupTaskOffset::new(
            clone_string(value.subtopology_id.as_str())?,
            value.partition,
            value.offset,
        ))
    })?;
    output.sort_unstable_by(
        |left: &DescribeStreamsGroupTaskOffset, right: &DescribeStreamsGroupTaskOffset| {
            left.identity().cmp(&right.identity())
        },
    );
    Ok(output)
}

fn materialize_assignment(
    value: &Assignment,
) -> Result<DescribeStreamsGroupAssignment, DescribeStreamsGroupProtocolFailure> {
    Ok(DescribeStreamsGroupAssignment::new(
        task_ids(&value.active_tasks)?,
        task_ids(&value.standby_tasks)?,
        task_ids(&value.warmup_tasks)?,
    ))
}

fn task_ids(
    values: &[TaskIds],
) -> Result<Vec<DescribeStreamsGroupTaskIds>, DescribeStreamsGroupProtocolFailure> {
    let mut output = collect_map(values, |value| {
        let mut partitions = try_vec(value.partitions.len())?;
        partitions.extend(value.partitions.iter().copied());
        partitions.sort_unstable();
        Ok(DescribeStreamsGroupTaskIds::new(
            clone_string(value.subtopology_id.as_str())?,
            partitions,
        ))
    })?;
    output.sort_unstable_by(
        |left: &DescribeStreamsGroupTaskIds, right: &DescribeStreamsGroupTaskIds| {
            left.subtopology_id()
                .as_bytes()
                .cmp(right.subtopology_id().as_bytes())
        },
    );
    Ok(output)
}

fn materialize_topology_description(
    value: &TopologyDescription,
) -> Result<DescribeStreamsGroupTopologyDescription, DescribeStreamsGroupProtocolFailure> {
    let mut subtopologies = collect_map(
        &value.subtopologies,
        materialize_topology_description_subtopology,
    )?;
    subtopologies.sort_unstable_by(|left, right| {
        left.subtopology_id()
            .as_bytes()
            .cmp(right.subtopology_id().as_bytes())
    });
    let mut global_stores = collect_map(
        &value.global_stores,
        materialize_topology_description_global_store,
    )?;
    global_stores.sort_unstable_by(|left, right| left.identity().cmp(&right.identity()));
    Ok(DescribeStreamsGroupTopologyDescription::new(
        subtopologies,
        global_stores,
    ))
}

fn materialize_topology_description_subtopology(
    value: &TopologyDescriptionSubtopology,
) -> Result<DescribeStreamsGroupTopologyDescriptionSubtopology, DescribeStreamsGroupProtocolFailure>
{
    let mut nodes = collect_map(&value.nodes, materialize_topology_description_node)?;
    nodes.sort_unstable_by(|left, right| left.name().as_bytes().cmp(right.name().as_bytes()));
    Ok(DescribeStreamsGroupTopologyDescriptionSubtopology::new(
        clone_string(value.subtopology_id.as_str())?,
        nodes,
    ))
}

fn materialize_topology_description_global_store(
    value: &TopologyDescriptionGlobalStore,
) -> Result<DescribeStreamsGroupTopologyDescriptionGlobalStore, DescribeStreamsGroupProtocolFailure>
{
    Ok(DescribeStreamsGroupTopologyDescriptionGlobalStore::new(
        materialize_topology_description_node(&value.source)?,
        materialize_topology_description_node(&value.processor)?,
    ))
}

fn materialize_topology_description_node(
    value: &TopologyDescriptionNode,
) -> Result<DescribeStreamsGroupTopologyDescriptionNode, DescribeStreamsGroupProtocolFailure> {
    Ok(DescribeStreamsGroupTopologyDescriptionNode::new(
        clone_string(value.name.as_str())?,
        value.node_type,
        strings(&value.source_topics)?,
        optional_string(value.sink_topic.as_ref())?,
        strings(&value.stores)?,
        strings(&value.successors)?,
    ))
}

fn strings(values: &[StrBytes]) -> Result<Vec<String>, DescribeStreamsGroupProtocolFailure> {
    let mut output = collect_map(values, |value| clone_string(value.as_str()))?;
    output.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    Ok(output)
}

fn optional_string(
    value: Option<&StrBytes>,
) -> Result<Option<String>, DescribeStreamsGroupProtocolFailure> {
    value.map(|value| clone_string(value.as_str())).transpose()
}

fn collect_map<T, U>(
    values: &[T],
    mut map: impl FnMut(&T) -> Result<U, DescribeStreamsGroupProtocolFailure>,
) -> Result<Vec<U>, DescribeStreamsGroupProtocolFailure> {
    let mut output = try_vec(values.len())?;
    for value in values {
        output.push(map(value)?);
    }
    Ok(output)
}

fn try_vec<T>(count: usize) -> Result<Vec<T>, DescribeStreamsGroupProtocolFailure> {
    let mut output = Vec::new();
    output
        .try_reserve_exact(count)
        .map_err(|_| DescribeStreamsGroupProtocolFailure::Allocation)?;
    Ok(output)
}
