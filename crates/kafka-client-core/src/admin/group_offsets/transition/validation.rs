//! Structural validation and deterministic correlation for group-offset facts.

use crate::admin::group_offsets::{
    GroupOffsetOutcome, GroupOffsetResult, ListConsumerGroupOffsetsBatch,
    ListConsumerGroupOffsetsSelection,
};

pub(super) fn correlate_outcomes(
    selection: &ListConsumerGroupOffsetsSelection,
    batch: ListConsumerGroupOffsetsBatch,
) -> Option<ListConsumerGroupOffsetsBatch> {
    let (throttle_time_ms, mut outcomes) = batch.into_parts();
    if outcomes.iter().any(outcome_is_malformed) {
        return None;
    }
    match selection {
        ListConsumerGroupOffsetsSelection::All => {
            outcomes.sort_by(|left, right| {
                left.topic()
                    .as_bytes()
                    .cmp(right.topic().as_bytes())
                    .then_with(|| left.partition().cmp(&right.partition()))
            });
            if outcomes.windows(2).any(|pair| {
                pair[0].topic() == pair[1].topic() && pair[0].partition() == pair[1].partition()
            }) {
                return None;
            }
        }
        ListConsumerGroupOffsetsSelection::Selected(targets) => {
            if outcomes.len() != targets.len()
                || !targets.iter().zip(&outcomes).all(|(target, outcome)| {
                    target.topic() == outcome.topic() && target.partition() == outcome.partition()
                })
            {
                return None;
            }
        }
    }
    Some(ListConsumerGroupOffsetsBatch::new(
        throttle_time_ms,
        outcomes,
    ))
}

fn outcome_is_malformed(outcome: &GroupOffsetOutcome) -> bool {
    if outcome.topic().is_empty() || outcome.partition() < 0 {
        return true;
    }
    let GroupOffsetResult::Described(description) = outcome.result() else {
        return false;
    };
    description.offset().is_some_and(|offset| offset < 0)
        || description.leader_epoch().is_some_and(|epoch| epoch < 0)
}
