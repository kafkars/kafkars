//! Linear terminal-capacity reservations committed from exact core effects.

use kafka_client_core::{AssignedConsumerEffect, AssignedTopicPartition};

use super::{AssignedConsumerEventStore, AssignedConsumerEventStoreError, EventClaim};

#[derive(Clone, Copy)]
pub(super) enum PreparedKind {
    Replacement(usize),
    Partition(AssignedTopicPartition),
}

/// Exclusive proof that terminal capacity was reserved before core mutation.
#[must_use = "prepared event claims must be committed or rolled back"]
pub(crate) struct PreparedEventClaims<'store> {
    pub(super) store: &'store mut AssignedConsumerEventStore,
    pub(super) kind: PreparedKind,
}

impl AssignedConsumerEventStore {
    pub(crate) fn prepare_replacement(
        &mut self,
        partition_count: usize,
    ) -> Result<PreparedEventClaims<'_>, AssignedConsumerEventStoreError> {
        if self.ready.len().saturating_add(partition_count) > self.capacity {
            return Err(AssignedConsumerEventStoreError::Capacity);
        }
        Ok(PreparedEventClaims {
            store: self,
            kind: PreparedKind::Replacement(partition_count),
        })
    }

    pub(crate) fn prepare_partition(
        &mut self,
        partition: AssignedTopicPartition,
    ) -> Result<PreparedEventClaims<'_>, AssignedConsumerEventStoreError> {
        let extra = usize::from(
            !self
                .claims
                .iter()
                .any(|claim| claim.partition() == partition),
        );
        if self
            .claims
            .len()
            .saturating_add(self.ready.len())
            .saturating_add(extra)
            > self.capacity
        {
            return Err(AssignedConsumerEventStoreError::Capacity);
        }
        Ok(PreparedEventClaims {
            store: self,
            kind: PreparedKind::Partition(partition),
        })
    }
}

impl PreparedEventClaims<'_> {
    pub(crate) fn commit_event_claims(
        self,
        effects: &[AssignedConsumerEffect],
    ) -> Result<(), AssignedConsumerEventStoreError> {
        match self.kind {
            PreparedKind::Replacement(count) => self.commit_replacement(count, effects),
            PreparedKind::Partition(partition) => self.commit_partition(partition, effects),
        }
    }

    pub(crate) fn rollback_event_claims(self) {
        let Self { store: _, kind: _ } = self;
    }

    fn commit_replacement(
        self,
        count: usize,
        effects: &[AssignedConsumerEffect],
    ) -> Result<(), AssignedConsumerEventStoreError> {
        let Some(revoke_count) = effects.len().checked_sub(count) else {
            return Err(AssignedConsumerEventStoreError::TransitionMismatch);
        };
        let (revocations, starts) = effects.split_at(revoke_count);
        if !revocations
            .iter()
            .all(|effect| matches!(effect, AssignedConsumerEffect::Revoke { .. }))
            || starts.iter().any(|effect| effect_claim(*effect).is_none())
            || has_duplicate_claim(starts)
        {
            return Err(AssignedConsumerEventStoreError::TransitionMismatch);
        }
        self.store.install_replacement_claims(effects);
        Ok(())
    }

    fn commit_partition(
        self,
        partition: AssignedTopicPartition,
        effects: &[AssignedConsumerEffect],
    ) -> Result<(), AssignedConsumerEventStoreError> {
        let Some(last) = effects.last() else {
            return validate_no_claim_transition(partition, effects);
        };
        let Some(claim) = effect_claim(*last) else {
            return validate_no_claim_transition(partition, effects);
        };
        let prefix_valid = match effects.split_last() {
            Some((_claim, [])) => true,
            Some((_claim, [AssignedConsumerEffect::Suspend { fence }])) => {
                fence.partition() == partition
            }
            _ => false,
        };
        if claim.partition() != partition || !prefix_valid {
            return Err(AssignedConsumerEventStoreError::TransitionMismatch);
        }
        self.store.install_partition_claim(partition, Some(claim));
        Ok(())
    }
}

pub(super) const fn effect_claim(effect: AssignedConsumerEffect) -> Option<EventClaim> {
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

fn has_duplicate_claim(effects: &[AssignedConsumerEffect]) -> bool {
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

fn validate_no_claim_transition(
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
