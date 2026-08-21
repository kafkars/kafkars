//! Exhaustive stable translation of engine `StreamsGroup` description outcomes.

use std::time::Duration;

use crate::{
    DeliveryStatus as PublicDeliveryStatus, ErrorKind, KafkaError,
    admin::{
        DescribeStreamsGroupResult as PublicResult, StreamsGroupAssignment,
        StreamsGroupDescription, StreamsGroupEndpoint, StreamsGroupKeyValue, StreamsGroupMember,
        StreamsGroupSubtopology, StreamsGroupTaskIds, StreamsGroupTaskOffset,
        StreamsGroupTopicInfo, StreamsGroupTopology, StreamsGroupTopologyDescription,
        StreamsGroupTopologyDescriptionStatus, StreamsGroupTopologyDescriptionSubtopology,
        StreamsGroupTopologyGlobalStore, StreamsGroupTopologyNode, StreamsGroupTopologyNodeType,
    },
};

use super::{
    engine::{
        AcceptedFaultKind, AdmissionError, AdmissionErrorKind, Assignment, BrokerError,
        DeliveryStatus, Description, Endpoint, Failure, FailureKind, KeyValue, Member,
        ObserverError, Outcome, Subtopology, TaskIds, TaskOffset, TopicInfo, Topology,
        TopologyDescription, TopologyDescriptionGlobalStore, TopologyDescriptionNode,
        TopologyDescriptionStatus, TopologyDescriptionSubtopology,
    },
    operation::AdminDescribeStreamsGroupResult,
};

pub(in crate::bridge) fn translate_admission_error(error: AdmissionError) -> KafkaError {
    translate_admission_kind(error.kind())
}

pub(super) fn translate_admission_kind(kind: AdmissionErrorKind) -> KafkaError {
    let public = match kind {
        AdmissionErrorKind::InvalidRequest | AdmissionErrorKind::InvalidDeadline => {
            ErrorKind::Configuration
        }
        AdmissionErrorKind::Contended
        | AdmissionErrorKind::Capacity
        | AdmissionErrorKind::RetainedBytes => ErrorKind::Backpressure,
        AdmissionErrorKind::Closed => ErrorKind::State,
        AdmissionErrorKind::IdentityExhausted | AdmissionErrorKind::HostUnavailable => {
            ErrorKind::Internal
        }
    };
    KafkaError::new(
        public,
        format!("DescribeStreamsGroup admission failed: {kind:?}"),
    )
    .with_delivery_status(PublicDeliveryStatus::NotSent)
}

pub(in crate::bridge) fn translate_accepted_fault(fault: AcceptedFaultKind) -> KafkaError {
    match fault {
        AcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "DescribeStreamsGroup was accepted but its host wake failed",
        ),
        AcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "DescribeStreamsGroup was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<Outcome, ObserverError>,
) -> AdminDescribeStreamsGroupResult {
    match result {
        Ok(Outcome::Described(result)) => {
            let (throttle_time_ms, description) = result.into_parts();
            Ok(PublicResult::new(
                Duration::from_millis(u64::from(throttle_time_ms)),
                translate_description(description),
            ))
        }
        Ok(Outcome::BrokerRejected(error)) => Err(translate_broker_error(error)),
        Ok(Outcome::Failed(failure)) => Err(translate_failure(failure)),
        Ok(Outcome::Batch(_)) => Err(KafkaError::new(
            ErrorKind::Internal,
            "DescribeStreamsGroup received a batch terminal from its singular plan",
        )
        .with_delivery_status(PublicDeliveryStatus::PossiblySent)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

pub(in crate::bridge) fn translate_description(
    description: Description,
) -> StreamsGroupDescription {
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
    StreamsGroupDescription::new(
        group_id,
        state,
        group_epoch,
        assignment_epoch,
        topology.map(translate_topology),
        members.into_iter().map(translate_member).collect(),
        authorized_operations,
        topology_description.map(translate_topology_description),
        topology_description_status.map(translate_topology_description_status),
    )
}

fn translate_topology(topology: Topology) -> StreamsGroupTopology {
    let (epoch, subtopologies) = topology.into_parts();
    StreamsGroupTopology::new(
        epoch,
        subtopologies.map(|values| values.into_iter().map(translate_subtopology).collect()),
    )
}

fn translate_subtopology(subtopology: Subtopology) -> StreamsGroupSubtopology {
    let (
        subtopology_id,
        source_topics,
        repartition_sink_topics,
        state_changelog_topics,
        repartition_source_topics,
    ) = subtopology.into_parts();
    StreamsGroupSubtopology::new(
        subtopology_id,
        source_topics,
        repartition_sink_topics,
        state_changelog_topics
            .into_iter()
            .map(translate_topic_info)
            .collect(),
        repartition_source_topics
            .into_iter()
            .map(translate_topic_info)
            .collect(),
    )
}

fn translate_topic_info(topic: TopicInfo) -> StreamsGroupTopicInfo {
    let (name, partitions, replication_factor, configs) = topic.into_parts();
    StreamsGroupTopicInfo::new(
        name,
        partitions,
        replication_factor,
        configs.into_iter().map(translate_key_value).collect(),
    )
}

fn translate_key_value(pair: KeyValue) -> StreamsGroupKeyValue {
    let (key, value) = pair.into_parts();
    StreamsGroupKeyValue::new(key, value)
}

fn translate_member(member: Member) -> StreamsGroupMember {
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
    StreamsGroupMember::new(
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

fn translate_endpoint(endpoint: Endpoint) -> StreamsGroupEndpoint {
    let (host, port) = endpoint.into_parts();
    StreamsGroupEndpoint::new(host, port)
}

fn translate_task_offset(offset: TaskOffset) -> StreamsGroupTaskOffset {
    let (subtopology_id, partition, offset) = offset.into_parts();
    StreamsGroupTaskOffset::new(subtopology_id, partition, offset)
}

fn translate_assignment(assignment: Assignment) -> StreamsGroupAssignment {
    let (active_tasks, standby_tasks, warmup_tasks) = assignment.into_parts();
    StreamsGroupAssignment::new(
        active_tasks.into_iter().map(translate_task_ids).collect(),
        standby_tasks.into_iter().map(translate_task_ids).collect(),
        warmup_tasks.into_iter().map(translate_task_ids).collect(),
    )
}

fn translate_task_ids(tasks: TaskIds) -> StreamsGroupTaskIds {
    let (subtopology_id, partitions) = tasks.into_parts();
    StreamsGroupTaskIds::new(subtopology_id, partitions)
}

fn translate_topology_description(
    description: TopologyDescription,
) -> StreamsGroupTopologyDescription {
    let (subtopologies, global_stores) = description.into_parts();
    StreamsGroupTopologyDescription::new(
        subtopologies
            .into_iter()
            .map(translate_topology_description_subtopology)
            .collect(),
        global_stores
            .into_iter()
            .map(translate_topology_description_global_store)
            .collect(),
    )
}

fn translate_topology_description_subtopology(
    subtopology: TopologyDescriptionSubtopology,
) -> StreamsGroupTopologyDescriptionSubtopology {
    let (subtopology_id, nodes) = subtopology.into_parts();
    StreamsGroupTopologyDescriptionSubtopology::new(
        subtopology_id,
        nodes
            .into_iter()
            .map(translate_topology_description_node)
            .collect(),
    )
}

fn translate_topology_description_global_store(
    store: TopologyDescriptionGlobalStore,
) -> StreamsGroupTopologyGlobalStore {
    let (source, processor) = store.into_parts();
    StreamsGroupTopologyGlobalStore::new(
        translate_topology_description_node(source),
        translate_topology_description_node(processor),
    )
}

fn translate_topology_description_node(node: TopologyDescriptionNode) -> StreamsGroupTopologyNode {
    let (name, node_type, source_topics, sink_topic, stores, successors) = node.into_parts();
    StreamsGroupTopologyNode::new(
        name,
        StreamsGroupTopologyNodeType::from_engine(node_type),
        source_topics,
        sink_topic,
        stores,
        successors,
    )
}

const fn translate_topology_description_status(
    status: TopologyDescriptionStatus,
) -> StreamsGroupTopologyDescriptionStatus {
    StreamsGroupTopologyDescriptionStatus::from_engine(status.raw())
}

pub(in crate::bridge) fn translate_broker_error(error: BrokerError) -> KafkaError {
    let (throttle_time_ms, code, message, message_truncated) = error.into_parts();
    let context =
        format!("Kafka rejected DescribeStreamsGroup after {throttle_time_ms} ms throttle");
    let diagnostic = match message {
        Some(message) => format!("{context} with broker code {code}: {message}"),
        None => format!("{context} with broker code {code}"),
    };
    KafkaError::new(ErrorKind::Broker, diagnostic)
        .with_broker_code(Some(code))
        .with_delivery_status(PublicDeliveryStatus::PossiblySent)
        .with_diagnostic_truncated(message_truncated)
}

pub(in crate::bridge) fn translate_failure(failure: Failure) -> KafkaError {
    translate_failure_parts(failure.kind(), failure.delivery())
}

pub(super) fn translate_failure_parts(kind: FailureKind, delivery: DeliveryStatus) -> KafkaError {
    let public = match kind {
        FailureKind::DeadlineElapsed => ErrorKind::Timeout,
        FailureKind::DriverRejected | FailureKind::ResponseTooLarge => ErrorKind::Backpressure,
        FailureKind::Transport => ErrorKind::Transport,
        FailureKind::Compatibility => ErrorKind::Compatibility,
        FailureKind::InvalidResponse => ErrorKind::Broker,
    };
    KafkaError::new(public, format!("DescribeStreamsGroup failed: {kind:?}"))
        .with_delivery_status(translate_delivery(delivery))
}

const fn translate_delivery(delivery: DeliveryStatus) -> PublicDeliveryStatus {
    match delivery {
        DeliveryStatus::NotSent => PublicDeliveryStatus::NotSent,
        DeliveryStatus::PossiblySent => PublicDeliveryStatus::PossiblySent,
    }
}

pub(in crate::bridge) fn translate_observer_error(error: ObserverError) -> KafkaError {
    let public = match error {
        ObserverError::AlreadyObserved => ErrorKind::State,
        ObserverError::Stale => ErrorKind::Internal,
    };
    KafkaError::new(public, error.to_string())
}
