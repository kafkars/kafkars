//! Core effect classification and exact claim-shape validation.

use kafka_client_core::{AssignedConsumerEffect, AssignedTopicPartition};

use super::super::{AssignedConsumerEventStoreError, EventClaim};

pub(in crate::consumer::assigned_event) const fn effect_claim(
    effect: AssignedConsumerEffect,
) -> Option<EventClaim> {
    match effect {
        AssignedConsumerEffect::ResolvePosition { fence, .. }
        | AssignedConsumerEffect::ArmPositionThrottle { fence, .. }
        | AssignedConsumerEffect::PositionResolutionFailed { fence, .. } => {
            Some(EventClaim::Position(fence))
        }
        AssignedConsumerEffect::FetchReady { fence, .. }
        | AssignedConsumerEffect::ArmFetchThrottle { fence, .. } => Some(EventClaim::Fetch(fence)),
        _ => None,
    }
}

pub(super) fn has_duplicate_claim(effects: &[AssignedConsumerEffect]) -> bool {
    effects.iter().enumerate().any(|(index, effect)| {
        let Some(claim) = effect_claim(*effect) else {
            return false;
        };
        effects[index + 1..]
            .iter()
            .filter_map(|later| effect_claim(*later))
            .any(|later| later.partition() == claim.partition())
    })
}

pub(super) fn validate_no_claim_transition(
    partition: AssignedTopicPartition,
    effects: &[AssignedConsumerEffect],
) -> Result<(), AssignedConsumerEventStoreError> {
    if effects.is_empty()
        || matches!(
            effects,
            [AssignedConsumerEffect::Suspend { fence }] if fence.partition() == partition
        )
    {
        Ok(())
    } else {
        Err(AssignedConsumerEventStoreError::TransitionMismatch)
    }
}
