//! Exact retention, translation, and delivery observation of terminal group Fetch facts.

use std::sync::Arc;

use kafka_client_core::{
    AssignedConsumerEffect, DeliveryOwnership, FetchFailure, FetchThrottleFailure,
};

use crate::consumer::{GroupConsumerFetchFailureKind, assigned_event::AssignedConsumerEvent};

use super::{
    super::session_catalog::GroupSessionCatalog,
    delivery::ClassicGroupFetchDeliveryError,
    model::{ClassicGroupFetchEffectFailure, ClassicGroupFetchFront, ClassicGroupFetchOwnerFault},
    owner::{ClassicGroupFetchOwner, FIRST_GROUP_FETCH_PARTITIONS},
};

impl ClassicGroupFetchOwner {
    /// Retires one unobserved terminal only after explicit close has retired
    /// the assignment and consumed the unique application observer.
    pub(in crate::consumer::group) fn discard_one_retired_terminal_for_close(&mut self) -> bool {
        if self.activation.is_some() || self.machine.assignment_epoch().is_some() {
            return false;
        }
        self.events.take_event().is_some()
    }

    pub(super) fn interpret_terminal_observation(
        &mut self,
        effect: AssignedConsumerEffect,
        catalog: &GroupSessionCatalog,
    ) -> Option<ClassicGroupFetchFront> {
        let position = match effect {
            AssignedConsumerEffect::FetchThrottleFailed { fence, .. }
            | AssignedConsumerEffect::FetchFailed { fence, .. } => fence.position(),
            _ => return None,
        };
        let ownership = self.machine.delivery_ownership(effect_fetch_fence(effect));
        if ownership == Ok(DeliveryOwnership::Superseded)
            || (self.activation.is_none()
                && ownership == Err(kafka_client_core::AssignedConsumerMachineError::NoAssignment))
        {
            return Some(self.discard_superseded_terminal_effect(effect));
        }
        if self.has_exact_retirement_control(position) {
            return Some(self.discard_terminal_effect(effect));
        }
        let topic = match catalog.topic_name(position.partition().topic_id()) {
            Ok(topic) => Arc::clone(topic),
            Err(error) => {
                self.fault = Some(ClassicGroupFetchOwnerFault::Effect {
                    effect,
                    failure: ClassicGroupFetchEffectFailure::TerminalCatalog(error),
                });
                self.settle_seek_host_unavailable();
                return Some(ClassicGroupFetchFront::Idle);
            }
        };
        match self.events.retain_terminal(topic, effect) {
            Ok(()) => {
                self.effects.pop_front();
                Some(ClassicGroupFetchFront::Interpreted)
            }
            Err((error, topic)) => {
                self.fault = Some(ClassicGroupFetchOwnerFault::Event {
                    effect,
                    error,
                    _topic: topic,
                });
                self.settle_seek_host_unavailable();
                Some(ClassicGroupFetchFront::Idle)
            }
        }
    }

    /// Transfers one exact retained Fetch failure at the delivery observation boundary.
    pub(in crate::consumer::group) fn take_fetch_failure(
        &mut self,
    ) -> Result<Option<GroupConsumerFetchFailureKind>, ClassicGroupFetchDeliveryError> {
        if self.is_faulted() {
            return Err(ClassicGroupFetchDeliveryError::Faulted);
        }
        for _attempt in 0..FIRST_GROUP_FETCH_PARTITIONS {
            let Some(event) = self.events.take_event() else {
                return Ok(None);
            };
            let Some(fence) = fetch_event_fence(&event) else {
                self.fault = Some(ClassicGroupFetchOwnerFault::DeliveryEvent {
                    error: None,
                    _event: event,
                });
                return Err(ClassicGroupFetchDeliveryError::Retained);
            };
            if self.activation.is_none() {
                continue;
            }
            match self.machine.delivery_ownership(fence) {
                Ok(DeliveryOwnership::Active) => match translate_event(event) {
                    Ok(kind) => return Ok(Some(kind)),
                    Err(event) => {
                        self.fault = Some(ClassicGroupFetchOwnerFault::DeliveryEvent {
                            error: None,
                            _event: event,
                        });
                        return Err(ClassicGroupFetchDeliveryError::Retained);
                    }
                },
                Ok(DeliveryOwnership::Superseded) => {}
                Err(error) => {
                    self.fault = Some(ClassicGroupFetchOwnerFault::DeliveryEvent {
                        error: Some(error),
                        _event: event,
                    });
                    return Err(ClassicGroupFetchDeliveryError::Retained);
                }
            }
        }
        Ok(None)
    }

    fn discard_terminal_effect(
        &mut self,
        effect: AssignedConsumerEffect,
    ) -> ClassicGroupFetchFront {
        match self.events.discard_terminal(effect) {
            Ok(()) => {
                self.effects.pop_front();
                ClassicGroupFetchFront::Interpreted
            }
            Err(error) => {
                self.fault = Some(ClassicGroupFetchOwnerFault::Effect {
                    effect,
                    failure: ClassicGroupFetchEffectFailure::Event(error),
                });
                self.settle_seek_host_unavailable();
                ClassicGroupFetchFront::Idle
            }
        }
    }

    fn discard_superseded_terminal_effect(
        &mut self,
        effect: AssignedConsumerEffect,
    ) -> ClassicGroupFetchFront {
        match self.events.discard_superseded_terminal(effect) {
            Ok(()) => {
                self.effects.pop_front();
                ClassicGroupFetchFront::Interpreted
            }
            Err(error) => {
                self.fault = Some(ClassicGroupFetchOwnerFault::Effect {
                    effect,
                    failure: ClassicGroupFetchEffectFailure::Event(error),
                });
                self.settle_seek_host_unavailable();
                ClassicGroupFetchFront::Idle
            }
        }
    }

    fn has_exact_retirement_control(&self, position: kafka_client_core::PositionFence) -> bool {
        self.effects.iter().skip(1).any(|queued| {
            matches!(
                *queued,
                AssignedConsumerEffect::Revoke {
                    assignment_epoch,
                    partition,
                } if assignment_epoch == position.assignment_epoch()
                    && partition == position.partition()
            )
        })
    }
}

const fn effect_fetch_fence(effect: AssignedConsumerEffect) -> kafka_client_core::FetchFence {
    match effect {
        AssignedConsumerEffect::FetchThrottleFailed { fence, .. }
        | AssignedConsumerEffect::FetchFailed { fence, .. } => fence,
        _ => unreachable!(),
    }
}

const fn fetch_event_fence(event: &AssignedConsumerEvent) -> Option<kafka_client_core::FetchFence> {
    match event {
        AssignedConsumerEvent::FetchThrottleFailed { fence, .. }
        | AssignedConsumerEvent::FetchFailed { fence, .. } => Some(*fence),
        AssignedConsumerEvent::PositionResolutionFailed { .. } => None,
    }
}

fn translate_event(
    event: AssignedConsumerEvent,
) -> Result<GroupConsumerFetchFailureKind, AssignedConsumerEvent> {
    match event {
        AssignedConsumerEvent::FetchThrottleFailed { failure, .. } => {
            Ok(translate_fetch_throttle_failure(failure))
        }
        AssignedConsumerEvent::FetchFailed { failure, .. } => Ok(translate_fetch_failure(failure)),
        event @ AssignedConsumerEvent::PositionResolutionFailed { .. } => Err(event),
    }
}

const fn translate_fetch_failure(failure: FetchFailure) -> GroupConsumerFetchFailureKind {
    match failure {
        FetchFailure::DeadlineElapsed => GroupConsumerFetchFailureKind::DeadlineElapsed,
        FetchFailure::DriverRejected => GroupConsumerFetchFailureKind::DriverRejected,
        FetchFailure::Transport => GroupConsumerFetchFailureKind::Transport,
        FetchFailure::Broker(code) => GroupConsumerFetchFailureKind::Broker(code.get()),
        FetchFailure::Compatibility => GroupConsumerFetchFailureKind::Compatibility,
        FetchFailure::InvalidResponse => GroupConsumerFetchFailureKind::InvalidResponse,
        FetchFailure::ResponseTooLarge => GroupConsumerFetchFailureKind::ResponseTooLarge,
    }
}

const fn translate_fetch_throttle_failure(
    failure: FetchThrottleFailure,
) -> GroupConsumerFetchFailureKind {
    match failure {
        FetchThrottleFailure::DeadlineOverflow => {
            GroupConsumerFetchFailureKind::ThrottleDeadlineOverflow
        }
    }
}
