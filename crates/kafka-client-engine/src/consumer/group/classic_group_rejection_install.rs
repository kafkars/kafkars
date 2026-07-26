//! Atomic mechanism installation for core-classified classic broker rejections.

use kafka_client_core::{
    ClassicBrokerError, ClassicCoordinatorRecovery, ClassicGroupEffect, ClassicGroupFatal,
    ClassicGroupPhase, ClassicGroupTransition, ClassicRejoinSchedule,
};

use crate::protocol::consumer::ClassicBrokerRejection;

use super::{
    classic_group_rejection_fault::{ClassicRejectionInstallFailure, ClassicRejectionPostCore},
    registry_entry::GroupConsumerEntry,
};

pub(super) const fn exact_broker_error(
    rejection: ClassicBrokerRejection,
) -> Option<ClassicBrokerError> {
    ClassicBrokerError::try_from_code(rejection.error_code().get())
}

#[expect(
    clippy::result_large_err,
    reason = "the error retains exact post-core effects without allocating or erasing recovery state"
)]
pub(super) fn install_stage_rejection(
    entry: &mut GroupConsumerEntry,
    transition: ClassicGroupTransition,
) -> Result<(), ClassicRejectionPostCore> {
    let effects = into_effects(transition);
    match (&effects[0], &effects[1]) {
        (
            Some(ClassicGroupEffect::ArmRejoin {
                schedule,
                coordinator: ClassicCoordinatorRecovery::Retain,
            }),
            None,
        ) => {
            let schedule = *schedule;
            if !waiting_state_matches(entry, schedule) {
                return Err(ClassicRejectionPostCore::new(effects, MachineState));
            }
            match entry.rejoin.prepare_rejoin_install(schedule) {
                Ok(prepared) => {
                    prepared.commit();
                    Ok(())
                }
                Err(_error) => Err(ClassicRejectionPostCore::new(effects, RejoinState)),
            }
        }
        (
            Some(ClassicGroupEffect::ArmRejoin {
                coordinator: ClassicCoordinatorRecovery::Rediscover,
                ..
            }),
            None,
        ) => Err(ClassicRejectionPostCore::new(
            effects,
            CoordinatorRediscovery,
        )),
        (Some(ClassicGroupEffect::Fatal { fatal }), None) if fatal_state_matches(entry, *fatal) => {
            Ok(())
        }
        (Some(ClassicGroupEffect::Fatal { .. }), None) => {
            Err(ClassicRejectionPostCore::new(effects, MachineState))
        }
        _ => Err(ClassicRejectionPostCore::new(effects, EffectShape)),
    }
}

fn waiting_state_matches(entry: &GroupConsumerEntry, schedule: ClassicRejoinSchedule) -> bool {
    entry.classic.machine().phase() == ClassicGroupPhase::WaitingToRejoin
        && entry.classic.machine().pending_rejoin() == Some(schedule)
        && entry.classic.machine().fatal().is_none()
        && entry.rejoin.is_dormant()
}

fn fatal_state_matches(entry: &GroupConsumerEntry, fatal: ClassicGroupFatal) -> bool {
    entry.classic.machine().phase() == ClassicGroupPhase::Fatal
        && entry.classic.machine().fatal() == Some(fatal)
        && entry.classic.machine().pending_rejoin().is_none()
        && entry.rejoin.is_dormant()
}

fn into_effects(transition: ClassicGroupTransition) -> [Option<ClassicGroupEffect>; 2] {
    let mut effects = transition.into_effects();
    [effects.next(), effects.next()]
}

use ClassicRejectionInstallFailure::{
    CoordinatorRediscovery, EffectShape, MachineState, RejoinState,
};
