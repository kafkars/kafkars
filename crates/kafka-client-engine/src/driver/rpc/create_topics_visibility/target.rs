//! Exact requested topology retained only for topics Kafka reported as created.

use kafka_client_core::{
    CreateTopicPlacement, CreateTopicResult, CreateTopicsInput, CreateTopicsPlan,
};
use kafka_driver::TopicName;

#[derive(Debug)]
pub(in super::super) struct CreateTopicVisibilityTarget {
    pub(super) topic: TopicName,
    pub(super) expected_partition_count: u32,
}

pub(in super::super) fn visibility_targets(
    plan: &CreateTopicsPlan,
    input: &CreateTopicsInput,
) -> Result<Vec<CreateTopicVisibilityTarget>, ()> {
    let CreateTopicsInput::BrokerResponded { outcomes } = input else {
        return Ok(Vec::new());
    };
    if plan.validate_only() {
        return Ok(Vec::new());
    }
    if plan.topics().len() != outcomes.len() {
        return Err(());
    }
    let mut targets = Vec::new();
    for (specification, outcome) in plan.topics().iter().zip(outcomes) {
        if specification.name() != outcome.topic() {
            return Err(());
        }
        if !matches!(outcome.result(), CreateTopicResult::Created) {
            continue;
        }
        let expected_partition_count = match specification.placement() {
            CreateTopicPlacement::Automatic { partitions, .. } => {
                u32::try_from(*partitions).map_err(|_error| ())?
            }
            CreateTopicPlacement::Manual { assignments, .. } => {
                u32::try_from(assignments.len()).map_err(|_error| ())?
            }
        };
        let topic = TopicName::new(specification.name().to_owned()).map_err(|_error| ())?;
        targets.push(CreateTopicVisibilityTarget {
            topic,
            expected_partition_count,
        });
    }
    Ok(targets)
}

#[cfg(test)]
impl CreateTopicVisibilityTarget {
    pub(in super::super) fn topic_for_test(&self) -> &str {
        self.topic.as_str()
    }

    pub(in super::super) const fn partition_count_for_test(&self) -> u32 {
        self.expected_partition_count
    }
}
