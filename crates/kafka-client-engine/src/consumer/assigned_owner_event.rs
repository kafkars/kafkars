//! Exact topic retention and FIFO transfer for terminal assigned-consumer effects.

use std::sync::Arc;

use kafka_client_core::AssignedConsumerEffect;

use super::{
    assigned_event::AssignedConsumerEvent,
    assigned_owner::AssignedConsumerOwner,
    assigned_owner_effect::FrontEffect,
    assigned_owner_fault::{AssignedConsumerEffectFailure, AssignedConsumerOwnerFault},
};

impl AssignedConsumerOwner {
    /// Transfers one retained scalar failure event without affecting close admission.
    pub(crate) fn take_event(&mut self) -> Option<AssignedConsumerEvent> {
        self.events.take_event()
    }

    pub(super) fn retain_terminal_event(&mut self, effect: AssignedConsumerEffect) -> FrontEffect {
        let Some(topic_id) = terminal_topic(effect) else {
            return FrontEffect::Idle;
        };
        let topic = match self.topics.name(topic_id) {
            Ok(topic) => Arc::clone(topic),
            Err(failure) => {
                self.fault = Some(AssignedConsumerOwnerFault::Effect {
                    effect,
                    failure: AssignedConsumerEffectFailure::Topic(failure),
                });
                return FrontEffect::Idle;
            }
        };
        match self.events.retain_terminal(topic, effect) {
            Ok(()) => {
                self.effects.pop_front();
                FrontEffect::Interpreted
            }
            Err((error, topic)) => {
                self.fault = Some(AssignedConsumerOwnerFault::Event {
                    effect,
                    error,
                    topic,
                });
                FrontEffect::Idle
            }
        }
    }
}

pub(super) const fn is_terminal_event(effect: AssignedConsumerEffect) -> bool {
    matches!(
        effect,
        AssignedConsumerEffect::PositionResolutionFailed { .. }
            | AssignedConsumerEffect::FetchThrottleFailed { .. }
            | AssignedConsumerEffect::FetchFailed { .. }
    )
}

const fn terminal_topic(effect: AssignedConsumerEffect) -> Option<kafka_client_core::TopicId> {
    match effect {
        AssignedConsumerEffect::PositionResolutionFailed { fence, .. } => {
            Some(fence.partition().topic_id())
        }
        AssignedConsumerEffect::FetchThrottleFailed { fence, .. }
        | AssignedConsumerEffect::FetchFailed { fence, .. } => {
            Some(fence.position().partition().topic_id())
        }
        _ => None,
    }
}
