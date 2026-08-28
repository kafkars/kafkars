//! Exact `TopicView` correlation over borrow-only prepared route facts.

use kafka_client_core::{PartitionIndex, ProducerAttemptFailureKind};

use crate::{
    driver::TopicRouteView,
    producer::execution::{PreparedProduceRouteCandidate, PreparedProduceRouteKey},
};

const GROUP_SAME_BROKER: bool = true;

pub(super) struct RoutedProduceGroup {
    broker_id: i32,
    candidates: Vec<PreparedProduceRouteCandidate>,
}

impl RoutedProduceGroup {
    pub(super) const fn broker_id(&self) -> i32 {
        self.broker_id
    }

    pub(super) fn into_candidates(self) -> Vec<PreparedProduceRouteCandidate> {
        self.candidates
    }
}

pub(super) fn route_candidates(
    candidates: Vec<PreparedProduceRouteCandidate>,
    key: &PreparedProduceRouteKey,
    view: &TopicRouteView,
) -> Result<
    Vec<RoutedProduceGroup>,
    (
        Vec<PreparedProduceRouteCandidate>,
        ProducerAttemptFailureKind,
    ),
> {
    let topic_id = view.kafka_topic_id();
    if key
        .expected_topic_uuid()
        .is_some_and(|expected| topic_id != Some(expected))
    {
        return Err((candidates, ProducerAttemptFailureKind::Identity));
    }
    let generation = view.metadata_generation();
    if key
        .retry_topic_identity()
        .is_some_and(|(_expected, floor)| generation <= floor)
    {
        return Err((candidates, ProducerAttemptFailureKind::RouteUnavailable));
    }

    let mut brokers = Vec::new();
    if brokers.try_reserve_exact(candidates.len()).is_err() {
        return Err((candidates, ProducerAttemptFailureKind::LocalCapacity));
    }
    for candidate in &candidates {
        let Ok(partition) = u32::try_from(candidate.partition()) else {
            return Err((candidates, ProducerAttemptFailureKind::Permanent));
        };
        let Some(broker_id) = view.leader_broker_id(PartitionIndex::from_raw(partition)) else {
            return Err((candidates, ProducerAttemptFailureKind::RouteUnavailable));
        };
        brokers.push(broker_id);
    }
    let Some(plans) = plan_groups(&brokers) else {
        return Err((candidates, ProducerAttemptFailureKind::LocalCapacity));
    };
    let Some(mut groups) = allocate_groups(&plans) else {
        return Err((candidates, ProducerAttemptFailureKind::LocalCapacity));
    };
    for (candidate, broker_id) in candidates.into_iter().zip(brokers) {
        let group = groups
            .iter_mut()
            .find(|group| {
                group.broker_id == broker_id && (GROUP_SAME_BROKER || group.candidates.is_empty())
            })
            .unwrap_or_else(|| unreachable!("preallocated broker group remains available"));
        group.candidates.push(candidate);
    }
    Ok(groups)
}

pub(super) fn plan_groups(brokers: &[i32]) -> Option<Vec<(i32, usize)>> {
    let mut plans: Vec<(i32, usize)> = Vec::new();
    plans.try_reserve_exact(brokers.len()).ok()?;
    for broker_id in brokers {
        if GROUP_SAME_BROKER {
            if let Some((_broker, count)) = plans.iter_mut().find(|(broker, _)| broker == broker_id)
            {
                *count = count.saturating_add(1);
                continue;
            }
        }
        plans.push((*broker_id, 1));
    }
    Some(plans)
}

pub(super) fn first_available_broker_group<I, F>(broker_ids: I, available: F) -> Option<usize>
where
    I: IntoIterator<Item = i32>,
    F: FnMut(i32) -> bool,
{
    broker_ids.into_iter().position(available)
}

fn allocate_groups(plans: &[(i32, usize)]) -> Option<Vec<RoutedProduceGroup>> {
    let mut groups = Vec::new();
    groups.try_reserve_exact(plans.len()).ok()?;
    for (broker_id, count) in plans {
        let mut candidates = Vec::new();
        candidates.try_reserve_exact(*count).ok()?;
        groups.push(RoutedProduceGroup {
            broker_id: *broker_id,
            candidates,
        });
    }
    Some(groups)
}
