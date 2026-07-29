//! Checked text, collection, and retained-capacity accounting for API-89.

use core::mem::size_of;

use kafka_client_core::{
    DESCRIBE_STREAMS_GROUP_DIAGNOSTIC_BYTES, DESCRIBE_STREAMS_GROUP_MAX_COLLECTION_ITEMS,
    DESCRIBE_STREAMS_GROUP_MAX_RESPONSE_TEXT_BYTES, DESCRIBE_STREAMS_GROUP_MAX_SCALAR_BYTES,
};
use kafka_wire::streams_group_describe_response::{
    Assignment, DescribedGroup, KeyValue, Member, Subtopology, TaskIds, TaskOffset, TopicInfo,
    TopologyDescription, TopologyDescriptionNode,
};
use kafka_wire_core::StrBytes;

use super::DescribeStreamsGroupProtocolFailure;

pub(super) const MAX_RESPONSE_TEXT_BYTES: usize = DESCRIBE_STREAMS_GROUP_MAX_RESPONSE_TEXT_BYTES;

pub(super) fn response_required_bytes(
    group: &DescribedGroup,
) -> Result<usize, DescribeStreamsGroupProtocolFailure> {
    let mut budget = Budget::new(size_of::<DescribedGroup>());
    budget.scalar(&group.group_id)?;
    budget.scalar(&group.group_state)?;
    budget.items::<Member>(group.members.len())?;
    if let Some(topology) = &group.topology {
        if let Some(subtopologies) = &topology.subtopologies {
            budget.items::<Subtopology>(subtopologies.len())?;
            for subtopology in subtopologies {
                budget.subtopology(subtopology)?;
            }
        }
    }
    for member in &group.members {
        budget.member(member)?;
    }
    if let Some(description) = &group.topology_description {
        budget.topology_description(description)?;
    }
    Ok(budget.retained)
}

pub(super) fn bounded_diagnostic(
    value: Option<&str>,
) -> Result<(Option<String>, bool), DescribeStreamsGroupProtocolFailure> {
    let Some(value) = value else {
        return Ok((None, false));
    };
    if value.len() > MAX_RESPONSE_TEXT_BYTES {
        return Err(DescribeStreamsGroupProtocolFailure::GroupDiagnosticTooLarge);
    }
    if value.len() <= DESCRIBE_STREAMS_GROUP_DIAGNOSTIC_BYTES {
        return Ok((Some(clone_string(value)?), false));
    }
    let mut end = DESCRIBE_STREAMS_GROUP_DIAGNOSTIC_BYTES;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    Ok((Some(clone_string(&value[..end])?), true))
}

pub(super) fn clone_string(value: &str) -> Result<String, DescribeStreamsGroupProtocolFailure> {
    let mut output = String::new();
    output
        .try_reserve_exact(value.len())
        .map_err(|_| DescribeStreamsGroupProtocolFailure::Allocation)?;
    output.push_str(value);
    Ok(output)
}

struct Budget {
    text: usize,
    retained: usize,
}

impl Budget {
    const fn new(retained: usize) -> Self {
        Self { text: 0, retained }
    }

    fn scalar(&mut self, value: &StrBytes) -> Result<(), DescribeStreamsGroupProtocolFailure> {
        if value.len() > DESCRIBE_STREAMS_GROUP_MAX_SCALAR_BYTES {
            return Err(DescribeStreamsGroupProtocolFailure::ScalarTooLarge);
        }
        self.text = self
            .text
            .checked_add(value.len())
            .ok_or(DescribeStreamsGroupProtocolFailure::ResponseTextBytesExceeded)?;
        if self.text > MAX_RESPONSE_TEXT_BYTES {
            return Err(DescribeStreamsGroupProtocolFailure::ResponseTextBytesExceeded);
        }
        self.retained = self
            .retained
            .checked_add(value.len())
            .ok_or(DescribeStreamsGroupProtocolFailure::RetainedBytesOverflow)?;
        Ok(())
    }

    fn optional_scalar(
        &mut self,
        value: Option<&StrBytes>,
    ) -> Result<(), DescribeStreamsGroupProtocolFailure> {
        value.map_or(Ok(()), |value| self.scalar(value))
    }

    fn items<T>(&mut self, count: usize) -> Result<(), DescribeStreamsGroupProtocolFailure> {
        if count > DESCRIBE_STREAMS_GROUP_MAX_COLLECTION_ITEMS {
            return Err(DescribeStreamsGroupProtocolFailure::TooManyItems);
        }
        self.retained = count
            .checked_mul(size_of::<T>())
            .and_then(|bytes| self.retained.checked_add(bytes))
            .ok_or(DescribeStreamsGroupProtocolFailure::RetainedBytesOverflow)?;
        Ok(())
    }

    fn strings(&mut self, values: &[StrBytes]) -> Result<(), DescribeStreamsGroupProtocolFailure> {
        self.items::<String>(values.len())?;
        for value in values {
            self.scalar(value)?;
        }
        Ok(())
    }

    fn key_values(
        &mut self,
        values: &[KeyValue],
    ) -> Result<(), DescribeStreamsGroupProtocolFailure> {
        self.items::<KeyValue>(values.len())?;
        for value in values {
            self.scalar(&value.key)?;
            self.scalar(&value.value)?;
        }
        Ok(())
    }

    fn topic_infos(
        &mut self,
        values: &[TopicInfo],
    ) -> Result<(), DescribeStreamsGroupProtocolFailure> {
        self.items::<TopicInfo>(values.len())?;
        for value in values {
            self.scalar(&value.name)?;
            self.key_values(&value.topic_configs)?;
        }
        Ok(())
    }

    fn subtopology(
        &mut self,
        value: &Subtopology,
    ) -> Result<(), DescribeStreamsGroupProtocolFailure> {
        self.scalar(&value.subtopology_id)?;
        self.strings(&value.source_topics)?;
        self.strings(&value.repartition_sink_topics)?;
        self.topic_infos(&value.state_changelog_topics)?;
        self.topic_infos(&value.repartition_source_topics)
    }

    fn task_ids(&mut self, values: &[TaskIds]) -> Result<(), DescribeStreamsGroupProtocolFailure> {
        self.items::<TaskIds>(values.len())?;
        for value in values {
            self.scalar(&value.subtopology_id)?;
            self.items::<i32>(value.partitions.len())?;
        }
        Ok(())
    }

    fn assignment(
        &mut self,
        value: &Assignment,
    ) -> Result<(), DescribeStreamsGroupProtocolFailure> {
        self.task_ids(&value.active_tasks)?;
        self.task_ids(&value.standby_tasks)?;
        self.task_ids(&value.warmup_tasks)
    }

    fn task_offsets(
        &mut self,
        values: &[TaskOffset],
    ) -> Result<(), DescribeStreamsGroupProtocolFailure> {
        self.items::<TaskOffset>(values.len())?;
        for value in values {
            self.scalar(&value.subtopology_id)?;
        }
        Ok(())
    }

    fn member(&mut self, value: &Member) -> Result<(), DescribeStreamsGroupProtocolFailure> {
        self.scalar(&value.member_id)?;
        self.optional_scalar(value.instance_id.as_ref())?;
        self.optional_scalar(value.rack_id.as_ref())?;
        self.scalar(&value.client_id)?;
        self.scalar(&value.client_host)?;
        self.scalar(&value.process_id)?;
        if let Some(endpoint) = &value.user_endpoint {
            self.scalar(&endpoint.host)?;
        }
        self.key_values(&value.client_tags)?;
        self.task_offsets(&value.task_offsets)?;
        self.task_offsets(&value.task_end_offsets)?;
        self.assignment(&value.assignment)?;
        self.assignment(&value.target_assignment)
    }

    fn node(
        &mut self,
        value: &TopologyDescriptionNode,
    ) -> Result<(), DescribeStreamsGroupProtocolFailure> {
        self.scalar(&value.name)?;
        self.strings(&value.source_topics)?;
        self.optional_scalar(value.sink_topic.as_ref())?;
        self.strings(&value.stores)?;
        self.strings(&value.successors)
    }

    fn topology_description(
        &mut self,
        value: &TopologyDescription,
    ) -> Result<(), DescribeStreamsGroupProtocolFailure> {
        self.items::<usize>(value.subtopologies.len())?;
        for subtopology in &value.subtopologies {
            self.scalar(&subtopology.subtopology_id)?;
            self.items::<TopologyDescriptionNode>(subtopology.nodes.len())?;
            for node in &subtopology.nodes {
                self.node(node)?;
            }
        }
        self.items::<usize>(value.global_stores.len())?;
        for store in &value.global_stores {
            self.node(&store.source)?;
            self.node(&store.processor)?;
        }
        Ok(())
    }
}
