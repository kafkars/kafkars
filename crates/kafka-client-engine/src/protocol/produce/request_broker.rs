//! Fallible first-seen topic planning for one broker-routed Produce request.

use std::{collections::HashMap, sync::Arc};

use kafka_client_core::Moment;
use kafka_wire::{ProduceRequest, produce_request::TopicProduceData};

use crate::clock::OperationDeadline;

use super::request::{ACKS_ALL, MaterializedProduce, remaining_broker_timeout_ms};

struct BrokerTopicPlan {
    topic: Arc<str>,
    partitions: usize,
}

type BrokerTopicPlanning = (HashMap<Arc<str>, usize>, Vec<BrokerTopicPlan>);

pub(super) fn build_broker_routed_request(
    batches: Vec<MaterializedProduce>,
    now: Moment,
    deadline: OperationDeadline,
) -> Result<ProduceRequest, Vec<MaterializedProduce>> {
    let Some((topic_indexes, topic_plans)) = plan_topics(&batches) else {
        return Err(batches);
    };
    let Some(topic_data) = build_topic_data(topic_plans) else {
        return Err(batches);
    };

    let mut request = ProduceRequest::default();
    request.acks = ACKS_ALL;
    request.timeout_ms = remaining_broker_timeout_ms(now, deadline);
    request.topic_data = topic_data;
    for batch in batches {
        let index = topic_indexes
            .get(batch.topic_name())
            .copied()
            .unwrap_or_else(|| unreachable!("planned Produce topic remains indexed"));
        let (_topic, partition) = batch.into_partition_data();
        request.topic_data[index].partition_data.push(partition);
    }
    Ok(request)
}

fn plan_topics(batches: &[MaterializedProduce]) -> Option<BrokerTopicPlanning> {
    let mut topic_indexes: HashMap<Arc<str>, usize> = HashMap::new();
    let mut topic_plans: Vec<BrokerTopicPlan> = Vec::new();
    topic_indexes.try_reserve(batches.len()).ok()?;
    topic_plans.try_reserve_exact(batches.len()).ok()?;
    for batch in batches {
        if let Some(index) = topic_indexes.get(batch.topic_name()).copied() {
            topic_plans[index].partitions = topic_plans[index].partitions.saturating_add(1);
        } else {
            let index = topic_plans.len();
            let topic = batch.topic_owner();
            topic_plans.push(BrokerTopicPlan {
                topic: Arc::clone(&topic),
                partitions: 1,
            });
            topic_indexes.insert(topic, index);
        }
    }
    Some((topic_indexes, topic_plans))
}

fn build_topic_data(topic_plans: Vec<BrokerTopicPlan>) -> Option<Vec<TopicProduceData>> {
    let mut topic_data = Vec::new();
    topic_data.try_reserve_exact(topic_plans.len()).ok()?;
    for plan in topic_plans {
        let mut partition_data = Vec::new();
        partition_data.try_reserve_exact(plan.partitions).ok()?;
        let mut topic = TopicProduceData::default();
        topic.name = plan.topic.as_ref().into();
        topic.partition_data = partition_data;
        topic_data.push(topic);
    }
    Some(topic_data)
}
