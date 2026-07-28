//! Linear retention of exact core effects after broker-rejection policy advances.

use kafka_client_core::{
    ClassicGeneration, ClassicGroupEffect, ClassicProcessingLeaseError, LiveGroupAssignment,
};

use super::{
    classic_group_assignment::ClassicGroupAssignmentPreparationFailureKind,
    classic_group_fetch::ClassicGroupFetchRetirementError,
};

/// Exact core effects retained when the engine cannot complete their installation.
#[must_use = "post-core rejection effects remain owned until shutdown recovery"]
pub(super) struct ClassicRejectionPostCore {
    post_core_rejection_effects: [Option<ClassicGroupEffect>; 2],
    post_core_rejection_failure: ClassicRejectionInstallFailure,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClassicRejectionInstallFailure {
    EffectShape,
    MachineState,
    RejoinState,
    RediscoveryState,
    Assignment(ClassicGroupAssignmentPreparationFailureKind),
    ProcessingLeaseCycleUnavailable,
    ProcessingLease(ClassicProcessingLeaseError),
    FetchRetirement(ClassicGroupFetchRetirementError),
}

impl ClassicRejectionPostCore {
    pub(super) const fn new(
        effects: [Option<ClassicGroupEffect>; 2],
        failure: ClassicRejectionInstallFailure,
    ) -> Self {
        Self {
            post_core_rejection_effects: effects,
            post_core_rejection_failure: failure,
        }
    }

    pub(super) fn heartbeat(
        assignment: LiveGroupAssignment,
        generation: ClassicGeneration,
        followup: ClassicGroupEffect,
        failure: ClassicRejectionInstallFailure,
    ) -> Self {
        Self::new(
            [
                Some(ClassicGroupEffect::Revoke {
                    assignment,
                    classic_generation: generation,
                }),
                Some(followup),
            ],
            failure,
        )
    }

    pub(super) fn retained_owner_count(&self) -> usize {
        self.post_core_rejection_effects
            .iter()
            .filter(|effect| effect.is_some())
            .count()
            .max(1)
    }

    #[cfg(test)]
    pub(super) const fn failure(&self) -> ClassicRejectionInstallFailure {
        self.post_core_rejection_failure
    }
}
