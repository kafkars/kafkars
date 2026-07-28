//! Heartbeat rejection effect-shape dispatch into atomic recovery installation.

use kafka_client_core::{
    ClassicCoordinatorRecovery, ClassicGroupEffect, ClassicGroupTransition, Moment,
};

use super::{
    classic_group_heartbeat_rejection_install::{
        install_fatal, install_rediscovery, install_rejoin,
    },
    classic_group_rejection_fault::ClassicRejectionPostCore,
    registry_entry::GroupConsumerEntry,
};

#[expect(
    clippy::result_large_err,
    reason = "the error retains exact post-core effects without allocating or erasing recovery state"
)]
pub(super) fn install_heartbeat_rejection(
    entry: &mut GroupConsumerEntry,
    transition: ClassicGroupTransition,
    now: Moment,
) -> Result<(), ClassicRejectionPostCore> {
    let effects = into_effects(transition);
    match effects {
        [
            Some(ClassicGroupEffect::Revoke {
                assignment,
                classic_generation,
            }),
            Some(ClassicGroupEffect::ArmRejoin {
                schedule,
                coordinator: ClassicCoordinatorRecovery::Retain,
            }),
        ] => install_rejoin(entry, assignment, classic_generation, schedule, now),
        [
            Some(ClassicGroupEffect::Revoke {
                assignment,
                classic_generation,
            }),
            Some(ClassicGroupEffect::ArmRejoin {
                schedule,
                coordinator: ClassicCoordinatorRecovery::Rediscover,
            }),
        ] => install_rediscovery(entry, assignment, classic_generation, schedule, now),
        [
            Some(ClassicGroupEffect::Revoke {
                assignment,
                classic_generation,
            }),
            Some(ClassicGroupEffect::Fatal { fatal }),
        ] => install_fatal(entry, assignment, classic_generation, fatal),
        effects => Err(ClassicRejectionPostCore::new(effects, EffectShape)),
    }
}

fn into_effects(transition: ClassicGroupTransition) -> [Option<ClassicGroupEffect>; 2] {
    let mut effects = transition.into_effects();
    [effects.next(), effects.next()]
}

use super::classic_group_rejection_fault::ClassicRejectionInstallFailure::EffectShape;
