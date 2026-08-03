//! Pure claim, event, and position projection for terminal effects.

use std::sync::Arc;

use kafka_client_core::AssignedConsumerEffect;

use super::{AssignedConsumerEvent, claim::EventClaim};

pub(super) const fn terminal_claim(effect: AssignedConsumerEffect) -> Option<EventClaim> {
    match effect {
        AssignedConsumerEffect::PositionResolutionFailed { fence, .. } => {
            Some(EventClaim::Position(fence))
        }
        AssignedConsumerEffect::FetchThrottleFailed { fence, .. }
        | AssignedConsumerEffect::FetchFailed { fence, .. } => Some(EventClaim::Fetch(fence)),
        _ => None,
    }
}

pub(super) fn terminal_event(
    topic: Arc<str>,
    effect: AssignedConsumerEffect,
) -> Result<AssignedConsumerEvent, Arc<str>> {
    match effect {
        AssignedConsumerEffect::PositionResolutionFailed { fence, failure } => {
            Ok(AssignedConsumerEvent::PositionResolutionFailed {
                topic,
                fence,
                failure,
            })
        }
        AssignedConsumerEffect::FetchThrottleFailed { fence, failure } => {
            Ok(AssignedConsumerEvent::FetchThrottleFailed {
                topic,
                fence,
                failure,
            })
        }
        AssignedConsumerEffect::FetchFailed { fence, failure } => {
            Ok(AssignedConsumerEvent::FetchFailed {
                topic,
                fence,
                failure,
            })
        }
        _ => Err(topic),
    }
}
