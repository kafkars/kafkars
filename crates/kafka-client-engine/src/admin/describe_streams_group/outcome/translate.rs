//! Exhaustive core-to-engine API-89 terminal translation.

use kafka_client_core::{
    DeliveryStatus as CoreDeliveryStatus, DescribeStreamsGroupFailureKind as CoreFailureKind,
    DescribeStreamsGroupTerminal as CoreTerminal,
};

use super::{
    DescribeStreamsGroupBatchOutcome, DescribeStreamsGroupBrokerError,
    DescribeStreamsGroupDeliveryStatus, DescribeStreamsGroupFailure,
    DescribeStreamsGroupFailureKind, DescribeStreamsGroupOutcome, DescribeStreamsGroupsBatch,
};
use crate::admin::describe_streams_group::{
    DescribeStreamsGroupAssignment, DescribeStreamsGroupDescription, DescribeStreamsGroupEndpoint,
    DescribeStreamsGroupKeyValue, DescribeStreamsGroupMember, DescribeStreamsGroupResult,
    DescribeStreamsGroupSubtopology, DescribeStreamsGroupTaskIds, DescribeStreamsGroupTaskOffset,
    DescribeStreamsGroupTopicInfo, DescribeStreamsGroupTopology,
    DescribeStreamsGroupTopologyDescription, DescribeStreamsGroupTopologyDescriptionGlobalStore,
    DescribeStreamsGroupTopologyDescriptionNode, DescribeStreamsGroupTopologyDescriptionStatus,
    DescribeStreamsGroupTopologyDescriptionSubtopology,
};

pub(crate) fn translate_terminal(terminal: CoreTerminal) -> DescribeStreamsGroupOutcome {
    match terminal {
        CoreTerminal::Described(result) => {
            DescribeStreamsGroupOutcome::Described(translate_result(result))
        }
        CoreTerminal::BrokerRejected(error) => {
            DescribeStreamsGroupOutcome::BrokerRejected(translate_broker_error(error))
        }
        CoreTerminal::Batch(batch) => {
            let (throttle_time_ms, outcomes) = batch.into_parts();
            DescribeStreamsGroupOutcome::Batch(DescribeStreamsGroupsBatch {
                throttle_time_ms,
                outcomes: outcomes.into_iter().map(translate_batch_outcome).collect(),
            })
        }
        CoreTerminal::Failed(failure) => {
            DescribeStreamsGroupOutcome::Failed(DescribeStreamsGroupFailure {
                kind: failure_kind(failure.kind()),
                delivery: delivery(failure.delivery()),
            })
        }
    }
}

fn translate_batch_outcome(
    outcome: kafka_client_core::DescribeStreamsGroupOutcome,
) -> DescribeStreamsGroupBatchOutcome {
    match outcome {
        kafka_client_core::DescribeStreamsGroupOutcome::Described(result) => {
            DescribeStreamsGroupBatchOutcome::Described(translate_result(result))
        }
        kafka_client_core::DescribeStreamsGroupOutcome::BrokerRejected { group_id, error } => {
            DescribeStreamsGroupBatchOutcome::BrokerRejected {
                group_id,
                error: translate_broker_error(error),
            }
        }
    }
}

fn translate_result(
    result: kafka_client_core::DescribeStreamsGroupResult,
) -> DescribeStreamsGroupResult {
    let (throttle_time_ms, description) = result.into_parts();
    DescribeStreamsGroupResult::new(throttle_time_ms, translate_description(description))
}

fn translate_broker_error(
    error: kafka_client_core::DescribeStreamsGroupBrokerError,
) -> DescribeStreamsGroupBrokerError {
    let (throttle_time_ms, code, message, message_truncated) = error.into_parts();
    DescribeStreamsGroupBrokerError {
        throttle_time_ms,
        code,
        message,
        message_truncated,
    }
}

fn translate_description(
    description: kafka_client_core::DescribeStreamsGroupDescription,
) -> DescribeStreamsGroupDescription {
    let (
        group_id,
        state,
        group_epoch,
        assignment_epoch,
        topology,
        members,
        authorized_operations,
        topology_description,
        topology_description_status,
    ) = description.into_parts();
    DescribeStreamsGroupDescription::new(
        group_id,
        state,
        group_epoch,
        assignment_epoch,
        topology.map(translate_topology),
        members.into_iter().map(translate_member).collect(),
        authorized_operations,
        topology_description.map(translate_topology_description),
        topology_description_status
            .map(|status| DescribeStreamsGroupTopologyDescriptionStatus::new(status.raw())),
    )
}

fn translate_topology(
    topology: kafka_client_core::DescribeStreamsGroupTopology,
) -> DescribeStreamsGroupTopology {
    let (epoch, subtopologies) = topology.into_parts();
    DescribeStreamsGroupTopology::new(
        epoch,
        subtopologies.map(|values| values.into_iter().map(translate_subtopology).collect()),
    )
}

fn translate_subtopology(
    value: kafka_client_core::DescribeStreamsGroupSubtopology,
) -> DescribeStreamsGroupSubtopology {
    let (id, source, sinks, changelogs, repartitions) = value.into_parts();
    DescribeStreamsGroupSubtopology::new(
        id,
        source,
        sinks,
        changelogs.into_iter().map(translate_topic_info).collect(),
        repartitions.into_iter().map(translate_topic_info).collect(),
    )
}

fn translate_topic_info(
    value: kafka_client_core::DescribeStreamsGroupTopicInfo,
) -> DescribeStreamsGroupTopicInfo {
    let (name, partitions, replication_factor, configs) = value.into_parts();
    DescribeStreamsGroupTopicInfo::new(
        name,
        partitions,
        replication_factor,
        configs.into_iter().map(translate_key_value).collect(),
    )
}

fn translate_key_value(
    value: kafka_client_core::DescribeStreamsGroupKeyValue,
) -> DescribeStreamsGroupKeyValue {
    let (key, value) = value.into_parts();
    DescribeStreamsGroupKeyValue::new(key, value)
}

fn translate_member(
    member: kafka_client_core::DescribeStreamsGroupMember,
) -> DescribeStreamsGroupMember {
    let (
        member_id,
        member_epoch,
        instance_id,
        rack_id,
        client_id,
        client_host,
        topology_epoch,
        process_id,
        user_endpoint,
        client_tags,
        task_offsets,
        task_end_offsets,
        assignment,
        target_assignment,
        is_classic,
    ) = member.into_parts();
    DescribeStreamsGroupMember::new(
        member_id,
        member_epoch,
        instance_id,
        rack_id,
        client_id,
        client_host,
        topology_epoch,
        process_id,
        user_endpoint.map(translate_endpoint),
        client_tags.into_iter().map(translate_key_value).collect(),
        task_offsets
            .into_iter()
            .map(translate_task_offset)
            .collect(),
        task_end_offsets
            .into_iter()
            .map(translate_task_offset)
            .collect(),
        translate_assignment(assignment),
        translate_assignment(target_assignment),
        is_classic,
    )
}

fn translate_endpoint(
    endpoint: kafka_client_core::DescribeStreamsGroupEndpoint,
) -> DescribeStreamsGroupEndpoint {
    let (host, port) = endpoint.into_parts();
    DescribeStreamsGroupEndpoint::new(host, port)
}

fn translate_task_offset(
    offset: kafka_client_core::DescribeStreamsGroupTaskOffset,
) -> DescribeStreamsGroupTaskOffset {
    let (subtopology_id, partition, offset) = offset.into_parts();
    DescribeStreamsGroupTaskOffset::new(subtopology_id, partition, offset)
}

fn translate_assignment(
    assignment: kafka_client_core::DescribeStreamsGroupAssignment,
) -> DescribeStreamsGroupAssignment {
    let (active, standby, warmup) = assignment.into_parts();
    DescribeStreamsGroupAssignment::new(
        active.into_iter().map(translate_task_ids).collect(),
        standby.into_iter().map(translate_task_ids).collect(),
        warmup.into_iter().map(translate_task_ids).collect(),
    )
}

fn translate_task_ids(
    tasks: kafka_client_core::DescribeStreamsGroupTaskIds,
) -> DescribeStreamsGroupTaskIds {
    let (subtopology_id, partitions) = tasks.into_parts();
    DescribeStreamsGroupTaskIds::new(subtopology_id, partitions)
}

fn translate_topology_description(
    description: kafka_client_core::DescribeStreamsGroupTopologyDescription,
) -> DescribeStreamsGroupTopologyDescription {
    let (subtopologies, global_stores) = description.into_parts();
    DescribeStreamsGroupTopologyDescription::new(
        subtopologies
            .into_iter()
            .map(|value| {
                let (id, nodes) = value.into_parts();
                DescribeStreamsGroupTopologyDescriptionSubtopology::new(
                    id,
                    nodes.into_iter().map(translate_node).collect(),
                )
            })
            .collect(),
        global_stores
            .into_iter()
            .map(|value| {
                let (source, processor) = value.into_parts();
                DescribeStreamsGroupTopologyDescriptionGlobalStore::new(
                    translate_node(source),
                    translate_node(processor),
                )
            })
            .collect(),
    )
}

fn translate_node(
    node: kafka_client_core::DescribeStreamsGroupTopologyDescriptionNode,
) -> DescribeStreamsGroupTopologyDescriptionNode {
    let (name, node_type, source_topics, sink_topic, stores, successors) = node.into_parts();
    DescribeStreamsGroupTopologyDescriptionNode::new(
        name,
        node_type,
        source_topics,
        sink_topic,
        stores,
        successors,
    )
}

const fn failure_kind(kind: CoreFailureKind) -> DescribeStreamsGroupFailureKind {
    match kind {
        CoreFailureKind::DeadlineElapsed => DescribeStreamsGroupFailureKind::DeadlineElapsed,
        CoreFailureKind::DriverRejected => DescribeStreamsGroupFailureKind::DriverRejected,
        CoreFailureKind::Transport => DescribeStreamsGroupFailureKind::Transport,
        CoreFailureKind::ResponseTooLarge => DescribeStreamsGroupFailureKind::ResponseTooLarge,
        CoreFailureKind::Compatibility => DescribeStreamsGroupFailureKind::Compatibility,
        CoreFailureKind::InvalidResponse => DescribeStreamsGroupFailureKind::InvalidResponse,
    }
}

const fn delivery(status: CoreDeliveryStatus) -> DescribeStreamsGroupDeliveryStatus {
    match status {
        CoreDeliveryStatus::NotSent => DescribeStreamsGroupDeliveryStatus::NotSent,
        CoreDeliveryStatus::PossiblySent => DescribeStreamsGroupDeliveryStatus::PossiblySent,
    }
}
