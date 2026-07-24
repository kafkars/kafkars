//! Conservative retained-result accounting for normalized topic descriptions.

use kafka_client_core::DescribeTopicsPlan;
use kafka_wire::MetadataResponse;

const BASE_RESULT_BYTES: usize = 8 * 1024;
const TOPIC_OWNER_BYTES: usize = 512;
const PARTITION_OWNER_BYTES: usize = 256;
const BROKER_ID_OWNER_BYTES: usize = 8;

pub(super) fn ensure_result_fits(
    plan: &DescribeTopicsPlan,
    response: &MetadataResponse,
    retained_bytes: usize,
) -> bool {
    result_charge(plan, response).is_some_and(|charge| charge <= retained_bytes)
}

fn result_charge(plan: &DescribeTopicsPlan, response: &MetadataResponse) -> Option<usize> {
    let mut charge = BASE_RESULT_BYTES;
    for requested in plan.topics() {
        charge = charge
            .checked_add(TOPIC_OWNER_BYTES)?
            .checked_add(requested.len())?;
        let Some(topic) = response.topics.iter().find(|topic| {
            topic
                .name
                .as_ref()
                .is_some_and(|name| name.as_str() == requested)
        }) else {
            continue;
        };
        if topic.error_code != 0 {
            continue;
        }
        charge = charge.checked_add(requested.len())?;
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
    }
    Some(charge)
}
