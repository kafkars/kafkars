//! Conservative retained-result accounting for normalized topic descriptions.

use kafka_wire::{MetadataResponse, metadata_response::MetadataResponseTopic};

const BASE_RESULT_BYTES: usize = 8 * 1024;
const TOPIC_OWNER_BYTES: usize = 512;
const PARTITION_OWNER_BYTES: usize = 256;
const BROKER_ID_OWNER_BYTES: usize = 8;

pub(super) fn ensure_result_fits(
    topics: &[String],
    response: &MetadataResponse,
    retained_bytes: usize,
) -> bool {
    named_result_charge(topics, response).is_some_and(|charge| charge <= retained_bytes)
}

fn named_result_charge(topics: &[String], response: &MetadataResponse) -> Option<usize> {
    let mut charge = BASE_RESULT_BYTES;
    for requested in topics {
        let Some(topic) = response.topics.iter().find(|topic| {
            topic
                .name
                .as_ref()
                .is_some_and(|name| name.as_str() == requested)
        }) else {
            continue;
        };
        charge = add_topic_charge(charge, requested, topic)?;
    }
    Some(charge)
}

pub(super) fn all_result_fits(response: &MetadataResponse, retained_bytes: usize) -> bool {
    let charge = response
        .topics
        .iter()
        .try_fold(BASE_RESULT_BYTES, |charge, topic| {
            let name = topic.name.as_ref()?;
            add_topic_charge(charge, name.as_str(), topic)
        });
    charge.is_some_and(|charge| charge <= retained_bytes)
}

fn add_topic_charge(mut charge: usize, name: &str, topic: &MetadataResponseTopic) -> Option<usize> {
    charge = charge
        .checked_add(TOPIC_OWNER_BYTES)?
        .checked_add(name.len())?;
    if topic.error_code != 0 {
        return Some(charge);
    }
    charge = charge.checked_add(name.len())?;
    for partition in &topic.partitions {
        let broker_ids = partition
            .replica_nodes
            .len()
            .checked_add(partition.isr_nodes.len())?
            .checked_add(partition.offline_replicas.len())?;
        charge = charge
            .checked_add(PARTITION_OWNER_BYTES)?
            .checked_add(broker_ids.checked_mul(BROKER_ID_OWNER_BYTES)?)?;
    }
    Some(charge)
}
