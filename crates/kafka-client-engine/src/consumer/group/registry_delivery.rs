//! Group-selected transfer and reclamation of one bounded Fetch byte lease.

use kafka_client_core::{
    ClassicProcessingLeaseEffect, ClassicProcessingLeaseFence, ClassicProcessingLeaseInput, GroupId,
};

use super::{
    classic_group_fetch::{ClassicGroupFetchDelivery, ClassicGroupFetchReclaimError},
    registry::GroupConsumerRegistry,
    registry_delivery_error::GroupConsumerDeliveryError,
    registry_entry::GroupConsumerEntryState,
    registry_port::GroupConsumerPort,
    registry_shard::GroupConsumerShardLockError,
    registry_wake::GroupConsumerShardWakeError,
};
use crate::clock::MonotonicClock;

/// Failure to return a lease before or after exact group-owner transfer.
#[must_use = "a pre-transfer rejection still owns the exact group Fetch delivery"]
#[allow(
    clippy::large_enum_variant,
    reason = "pre-transfer rejection must return the exact linear delivery without hidden boxing"
)]
pub(in crate::consumer::group) enum GroupConsumerDeliveryReclaimFailure {
    Returned {
        reason: GroupConsumerDeliveryReclaimRejection,
        delivery: ClassicGroupFetchDelivery,
    },
    Retained(ClassicGroupFetchReclaimError),
}

/// Stable reason a reclaim never reached its exact Fetch owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum GroupConsumerDeliveryReclaimRejection {
    Lock(GroupConsumerShardLockError),
    UnknownGroup,
}

impl GroupConsumerDeliveryReclaimFailure {
    pub(in crate::consumer::group) const fn reason(
        &self,
    ) -> Option<GroupConsumerDeliveryReclaimRejection> {
        match self {
            Self::Returned { reason, .. } => Some(*reason),
            Self::Retained(_) => None,
        }
    }

    pub(in crate::consumer::group) fn into_delivery(self) -> Option<ClassicGroupFetchDelivery> {
        match self {
            Self::Returned { delivery, .. } => Some(delivery),
            Self::Retained(_) => None,
        }
    }
}

impl GroupConsumerRegistry {
    /// Transfers at most one already-authorized delivery from one exact group.
    pub(in crate::consumer::group) fn take_delivery(
        &mut self,
        group_id: GroupId,
        clock: &MonotonicClock,
    ) -> Result<Option<ClassicGroupFetchDelivery>, GroupConsumerDeliveryError> {
        let Some(entry) = self
            .entries
            .iter_mut()
            .find(|entry| entry.group_id() == group_id)
        else {
            return Err(GroupConsumerDeliveryError::UnknownGroup);
        };
        if entry.state == GroupConsumerEntryState::Closing {
            return Err(GroupConsumerDeliveryError::Closing);
        }
        if let Some(failure) = entry.take_position_failure_observation() {
            return Err(GroupConsumerDeliveryError::PositionFailure(failure));
        }
        if entry.fault.is_some() {
            return Err(GroupConsumerDeliveryError::EntryFault);
        }
        let delivery = entry
            .fetch
            .take_delivery(&entry.catalog)
            .map_err(GroupConsumerDeliveryError::Fetch)?;
        let Some(delivery) = delivery else {
            return Ok(None);
        };
        let now = match clock.now() {
            Ok(now) => now,
            Err(error) => {
                let delivery_retained = reclaim_rejected_delivery(entry, delivery);
                return Err(GroupConsumerDeliveryError::Clock {
                    error,
                    delivery_retained,
                });
            }
        };
        let position_fence = delivery.position_fence();
        let processing_fence = ClassicProcessingLeaseFence::new(
            delivery.group_id(),
            position_fence.membership_cycle(),
            position_fence.assignment_generation(),
        );
        let transition = match entry
            .processing_lease
            .apply(ClassicProcessingLeaseInput::Progress {
                fence: processing_fence,
                now,
            }) {
            Ok(transition) => transition,
            Err(error) => {
                let delivery_retained = reclaim_rejected_delivery(entry, delivery);
                return Err(GroupConsumerDeliveryError::Processing {
                    error,
                    delivery_retained,
                });
            }
        };
        let mut effects = transition.effects().copied();
        match (effects.next(), effects.next()) {
            (Some(ClassicProcessingLeaseEffect::Arm { schedule }), None)
                if schedule.fence() == processing_fence =>
            {
                Ok(Some(delivery))
            }
            (Some(ClassicProcessingLeaseEffect::AssignmentLost { expiration }), None)
                if expiration.schedule().fence() == processing_fence =>
            {
                let delivery_retained = reclaim_rejected_delivery(entry, delivery);
                Err(GroupConsumerDeliveryError::ProcessingExpired {
                    expiration,
                    delivery_retained,
                })
            }
            _ => {
                let delivery_retained = reclaim_rejected_delivery(entry, delivery);
                Err(GroupConsumerDeliveryError::ProcessingEffect { delivery_retained })
            }
        }
    }

    /// Returns one exact lease without consulting mutable membership state.
    #[expect(
        clippy::result_large_err,
        reason = "pre-transfer unknown-group rejection must return the exact linear delivery"
    )]
    pub(in crate::consumer::group) fn reclaim_delivery(
        &mut self,
        delivery: ClassicGroupFetchDelivery,
    ) -> Result<(), GroupConsumerDeliveryReclaimFailure> {
        let group_id = delivery.group_id();
        let Some(entry) = self
            .entries
            .iter_mut()
            .find(|entry| entry.group_id() == group_id)
        else {
            return Err(GroupConsumerDeliveryReclaimFailure::Returned {
                reason: GroupConsumerDeliveryReclaimRejection::UnknownGroup,
                delivery,
            });
        };
        entry
            .fetch
            .reclaim_delivery(delivery)
            .map_err(GroupConsumerDeliveryReclaimFailure::Retained)
    }
}

/// Immediate observation rejection before any byte lease transfers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) enum GroupConsumerDeliveryPortError {
    Closed,
    Lock(GroupConsumerShardLockError),
    Registry(GroupConsumerDeliveryError),
}

/// Successful reclaim plus an advisory reactor-wake failure.
#[must_use = "a reclaimed delivery can retain advisory host degradation"]
pub(in crate::consumer::group) struct GroupConsumerDeliveryReclaim {
    wake: Option<GroupConsumerShardWakeError>,
}

impl GroupConsumerDeliveryReclaim {
    pub(in crate::consumer::group) const fn wake_failed(&self) -> bool {
        self.wake.is_some()
    }
}

impl GroupConsumerPort {
    /// Probes one already-authorized delivery without starting Fetch or a timeout.
    pub(in crate::consumer) fn try_take_delivery(
        &self,
        group_id: GroupId,
    ) -> Result<Option<ClassicGroupFetchDelivery>, GroupConsumerDeliveryPortError> {
        if self.shared.admission_is_closed() {
            return Err(GroupConsumerDeliveryPortError::Closed);
        }
        let mut registry = self
            .shared
            .try_registry()
            .map_err(GroupConsumerDeliveryPortError::Lock)?;
        if self.shared.admission_is_closed() {
            return Err(GroupConsumerDeliveryPortError::Closed);
        }
        let result = registry.take_delivery(group_id, &self.clock);
        let requires_host_turn = matches!(
            result.as_ref(),
            Err(GroupConsumerDeliveryError::ProcessingExpired { .. })
        );
        drop(registry);
        if requires_host_turn {
            let _wake_result = self.shared.request_turn();
        }
        result.map_err(GroupConsumerDeliveryPortError::Registry)
    }

    /// Returns the byte lease even after observation admission has closed.
    #[expect(
        clippy::result_large_err,
        reason = "pre-transfer shard rejection must return the exact linear delivery"
    )]
    pub(in crate::consumer::group) fn reclaim_delivery(
        &self,
        delivery: ClassicGroupFetchDelivery,
    ) -> Result<GroupConsumerDeliveryReclaim, GroupConsumerDeliveryReclaimFailure> {
        let mut registry = match self.shared.try_registry() {
            Ok(registry) => registry,
            Err(error) => {
                return Err(GroupConsumerDeliveryReclaimFailure::Returned {
                    reason: GroupConsumerDeliveryReclaimRejection::Lock(error),
                    delivery,
                });
            }
        };
        registry.reclaim_delivery(delivery)?;
        drop(registry);
        Ok(GroupConsumerDeliveryReclaim {
            wake: self.shared.request_turn().err(),
        })
    }

    /// Blocks only the dropping application thread until the exact lease can
    /// return, then requests reactor progress after releasing the owner lock.
    pub(in crate::consumer) fn return_delivery_blocking(
        &self,
        delivery: ClassicGroupFetchDelivery,
    ) {
        self.shared.return_delivery_blocking(delivery);
    }
}

fn reclaim_rejected_delivery(
    entry: &mut super::registry_entry::GroupConsumerEntry,
    delivery: ClassicGroupFetchDelivery,
) -> bool {
    entry.fetch.reclaim_delivery(delivery).is_err()
}
