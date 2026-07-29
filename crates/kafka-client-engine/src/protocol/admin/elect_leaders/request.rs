//! Charged generated API-key 43 construction from caller-ordered targets.

use std::{error::Error, fmt};

use kafka_client_core::LeaderElectionType;
use kafka_wire::{ElectLeadersRequest, elect_leaders_request::TopicPartitions};

use super::{
    ElectLeadersSelectionRef, LeaderElectionRef, model::election_type_code,
    retention::generated_request_peak_charge,
};

/// Request construction failure before generated or driver ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ElectLeadersRequestFailure {
    NegativeTimeout,
    EmptySelection,
    RetainedBytes,
}

impl fmt::Display for ElectLeadersRequestFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NegativeTimeout => "leader-election request timeout is negative",
            Self::EmptySelection => "selected leader-election request is empty",
            Self::RetainedBytes => "generated leader-election request exceeds its proven budget",
        })
    }
}

impl Error for ElectLeadersRequestFailure {}

/// Builds one all-partition or selected-partition request without routing or retry policy.
pub(crate) fn elect_leaders_request(
    election_type: LeaderElectionType,
    selection: ElectLeadersSelectionRef<'_>,
    timeout_ms: i32,
    scratch_limit: usize,
) -> Result<ElectLeadersRequest, ElectLeadersRequestFailure> {
    if timeout_ms < 0 {
        return Err(ElectLeadersRequestFailure::NegativeTimeout);
    }
    let targets = match selection {
        ElectLeadersSelectionRef::AllPartitions => &[][..],
        ElectLeadersSelectionRef::Selected(targets) if targets.is_empty() => {
            return Err(ElectLeadersRequestFailure::EmptySelection);
        }
        ElectLeadersSelectionRef::Selected(targets) => targets,
    };
    let charge = generated_request_peak_charge(targets.iter().copied())
        .ok_or(ElectLeadersRequestFailure::RetainedBytes)?;
    if charge > scratch_limit {
        return Err(ElectLeadersRequestFailure::RetainedBytes);
    }
    let order = grouped_order(targets)?;
    let mut request = ElectLeadersRequest::default();
    request.election_type = election_type_code(election_type);
    request.timeout_ms = timeout_ms;
    request.topic_partitions = match selection {
        ElectLeadersSelectionRef::AllPartitions => None,
        ElectLeadersSelectionRef::Selected(_) => Some(group_topics(targets, &order)),
    };
    Ok(request)
}

fn grouped_order(
    targets: &[LeaderElectionRef<'_>],
) -> Result<Vec<usize>, ElectLeadersRequestFailure> {
    let mut order = Vec::new();
    order
        .try_reserve_exact(targets.len())
        .map_err(|_| ElectLeadersRequestFailure::RetainedBytes)?;
    order.extend(0..targets.len());
    order.sort_unstable_by(|left, right| {
        targets[*left]
            .topic()
            .as_bytes()
            .cmp(targets[*right].topic().as_bytes())
            .then_with(|| left.cmp(right))
    });
    Ok(order)
}

fn group_topics(targets: &[LeaderElectionRef<'_>], order: &[usize]) -> Vec<TopicPartitions> {
    let mut topics = Vec::new();
    let mut cursor = 0usize;
    while cursor < order.len() {
        let topic_name = targets[order[cursor]].topic();
        let mut topic = TopicPartitions::default();
        topic.topic = topic_name.into();
        while cursor < order.len() && targets[order[cursor]].topic() == topic_name {
            topic.partitions.push(targets[order[cursor]].partition());
            cursor += 1;
        }
        topics.push(topic);
    }
    topics
}
