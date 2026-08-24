//! Exact core-effect validation and installation for prepared event claims.

use kafka_client_core::{AssignedConsumerEffect, AssignedTopicPartition};

use super::{
    super::AssignedConsumerEventStoreError,
    claim::{effect_claim, has_duplicate_claim, validate_no_claim_transition},
    model::{PreparedEventClaims, PreparedKind},
};

impl PreparedEventClaims<'_, '_> {
    pub(crate) fn commit_event_claims(
        self,
        effects: &[AssignedConsumerEffect],
    ) -> Result<(), AssignedConsumerEventStoreError> {
        match self.kind {
            PreparedKind::Replacement(count) => self.commit_replacement(count, effects),
            PreparedKind::Reconciliation(count) => self.commit_reconciliation(count, effects),
            PreparedKind::Addition(count) => self.commit_addition(count, effects),
            PreparedKind::Removal(count) => self.commit_removal(count, effects),
            PreparedKind::Partition(partition) => self.commit_partition(partition, effects),
            PreparedKind::Pause(partitions) => self.commit_pause(partitions, effects),
            PreparedKind::Resume(partitions) => self.commit_resume(partitions, effects),
        }
    }

    fn commit_addition(
        self,
        count: usize,
        effects: &[AssignedConsumerEffect],
    ) -> Result<(), AssignedConsumerEventStoreError> {
        if effects.len() != count
            || effects.iter().any(|effect| effect_claim(*effect).is_none())
            || has_duplicate_claim(effects)
            || effects.iter().any(|effect| {
                effect_claim(*effect).is_some_and(|candidate| {
                    self.store
                        .claims
                        .iter()
                        .any(|present| present.partition() == candidate.partition())
                })
            })
        {
            return Err(AssignedConsumerEventStoreError::TransitionMismatch);
        }
        self.store.install_addition_claims(effects);
        Ok(())
    }

    fn commit_removal(
        self,
        count: usize,
        effects: &[AssignedConsumerEffect],
    ) -> Result<(), AssignedConsumerEventStoreError> {
        let Self { store: _, kind: _ } = self;
        if effects.len() == count
            && effects
                .iter()
                .all(|effect| matches!(effect, AssignedConsumerEffect::Revoke { .. }))
        {
            Ok(())
        } else {
            Err(AssignedConsumerEventStoreError::TransitionMismatch)
        }
    }

    fn commit_reconciliation(
        self,
        count: usize,
        effects: &[AssignedConsumerEffect],
    ) -> Result<(), AssignedConsumerEventStoreError> {
        let first_claim = effects
            .iter()
            .position(|effect| effect_claim(*effect).is_some())
            .unwrap_or(effects.len());
        let (controls, starts) = effects.split_at(first_claim);
        if !controls.iter().all(|effect| {
            matches!(
                effect,
                AssignedConsumerEffect::Revoke { .. } | AssignedConsumerEffect::Suspend { .. }
            )
        }) || starts.iter().any(|effect| effect_claim(*effect).is_none())
            || starts.len() > count
            || has_duplicate_claim(starts)
        {
            return Err(AssignedConsumerEventStoreError::TransitionMismatch);
        }
        self.store.install_replacement_claims(effects);
        Ok(())
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

    fn commit_pause(
        self,
        partitions: &[AssignedTopicPartition],
        effects: &[AssignedConsumerEffect],
    ) -> Result<(), AssignedConsumerEventStoreError> {
        let Self { store: _, kind: _ } = self;
        let mut targets = partitions.iter();
        for effect in effects {
            let AssignedConsumerEffect::Suspend { fence } = effect else {
                return Err(AssignedConsumerEventStoreError::TransitionMismatch);
            };
            if !targets.any(|target| *target == fence.partition()) {
                return Err(AssignedConsumerEventStoreError::TransitionMismatch);
            }
        }
        Ok(())
    }

    fn commit_resume(
        self,
        partitions: &[AssignedTopicPartition],
        effects: &[AssignedConsumerEffect],
    ) -> Result<(), AssignedConsumerEventStoreError> {
        let mut targets = partitions.iter();
        for effect in effects {
            let Some(claim) = effect_claim(*effect) else {
                return Err(AssignedConsumerEventStoreError::TransitionMismatch);
            };
            let partition = claim.partition();
            if !targets.any(|target| *target == partition) {
                return Err(AssignedConsumerEventStoreError::TransitionMismatch);
            }
        }
        for effect in effects {
            let Some(claim) = effect_claim(*effect) else {
                return Err(AssignedConsumerEventStoreError::TransitionMismatch);
            };
            let partition = claim.partition();
            self.store.install_partition_claim(partition, Some(claim));
        }
        Ok(())
    }
}
