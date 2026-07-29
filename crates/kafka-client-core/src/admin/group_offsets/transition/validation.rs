//! Structural validation for normalized group-offset response facts.

use core::cmp::Ordering;

use crate::admin::group_offsets::{GroupOffsetOutcome, GroupOffsetResult};

pub(super) fn outcomes_are_normalized(outcomes: &[GroupOffsetOutcome]) -> bool {
    if outcomes.iter().any(outcome_is_malformed) {
        return false;
    }
    outcomes.windows(2).all(|pair| {
        match pair[0].topic().as_bytes().cmp(pair[1].topic().as_bytes()) {
            Ordering::Less => true,
            Ordering::Equal => pair[0].partition() < pair[1].partition(),
            Ordering::Greater => false,
        }
    })
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
