//! Canonicalization for the optional stable-v1 topology-description graph.

use super::{Charge, value::canonical_strings};
use crate::admin::describe_streams_group::{
    DescribeStreamsGroupTopologyDescription, DescribeStreamsGroupTopologyDescriptionGlobalStore,
    DescribeStreamsGroupTopologyDescriptionNode,
    DescribeStreamsGroupTopologyDescriptionSubtopology,
};

pub(super) fn canonical_topology_description(
    description: DescribeStreamsGroupTopologyDescription,
    charge: &mut Charge,
) -> Option<DescribeStreamsGroupTopologyDescription> {
    let (mut subtopologies, mut global_stores) = description.into_parts();
    if !charge.items::<DescribeStreamsGroupTopologyDescriptionSubtopology>(subtopologies.len())
        || !charge.items::<DescribeStreamsGroupTopologyDescriptionGlobalStore>(global_stores.len())
    {
        return None;
    }
    for subtopology in &mut subtopologies {
        let (id, mut nodes) = subtopology.clone().into_parts();
        if id.is_empty()
            || !charge.scalar(&id)
            || !charge.items::<DescribeStreamsGroupTopologyDescriptionNode>(nodes.len())
        {
            charge.invalid = id.is_empty();
            return None;
        }
        for node in &mut nodes {
            *node = canonical_node(node.clone(), charge)?;
        }
        nodes.sort_unstable_by(|left, right| left.name().as_bytes().cmp(right.name().as_bytes()));
        if nodes
            .windows(2)
            .any(|pair| pair[0].name() == pair[1].name())
        {
            charge.invalid = true;
            return None;
        }
        *subtopology = DescribeStreamsGroupTopologyDescriptionSubtopology::new(id, nodes);
    }
    subtopologies.sort_unstable_by(|left, right| {
        left.subtopology_id()
            .as_bytes()
            .cmp(right.subtopology_id().as_bytes())
    });
    if subtopologies
        .windows(2)
        .any(|pair| pair[0].subtopology_id() == pair[1].subtopology_id())
    {
        charge.invalid = true;
        return None;
    }
    for store in &mut global_stores {
        let (source, processor) = store.clone().into_parts();
        *store = DescribeStreamsGroupTopologyDescriptionGlobalStore::new(
            canonical_node(source, charge)?,
            canonical_node(processor, charge)?,
        );
    }
    global_stores.sort_unstable_by(|left, right| left.identity().cmp(&right.identity()));
    if global_stores
        .windows(2)
        .any(|pair| pair[0].identity() == pair[1].identity())
    {
        charge.invalid = true;
        return None;
    }
    Some(DescribeStreamsGroupTopologyDescription::new(
        subtopologies,
        global_stores,
    ))
}

fn canonical_node(
    node: DescribeStreamsGroupTopologyDescriptionNode,
    charge: &mut Charge,
) -> Option<DescribeStreamsGroupTopologyDescriptionNode> {
    let (name, node_type, source_topics, sink_topic, stores, successors) = node.into_parts();
    if name.is_empty()
        || sink_topic.as_ref().is_some_and(String::is_empty)
        || !charge.scalar(&name)
        || !charge.optional_scalar(sink_topic.as_deref())
    {
        charge.invalid = name.is_empty() || sink_topic.as_ref().is_some_and(String::is_empty);
        return None;
    }
    Some(DescribeStreamsGroupTopologyDescriptionNode::new(
        name,
        node_type,
        canonical_strings(source_topics, charge, true)?,
        sink_topic,
        canonical_strings(stores, charge, true)?,
        canonical_strings(successors, charge, true)?,
    ))
}
