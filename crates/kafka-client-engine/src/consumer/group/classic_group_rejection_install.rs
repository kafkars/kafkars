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
    match effects {
        [
            Some(ClassicGroupEffect::ArmRejoin {
                schedule,
                coordinator,
            }),
            None,
        ] => install_rejoin(entry, schedule, coordinator).map_err(|failure| {
            ClassicRejectionPostCore::new(
                [
                    Some(ClassicGroupEffect::ArmRejoin {
                        schedule,
                        coordinator,
                    }),
                    None,
                ],
                failure,
            )
        }),
        [Some(ClassicGroupEffect::Fatal { fatal }), None] if fatal_state_matches(entry, fatal) => {
            Ok(())
        }
        [Some(ClassicGroupEffect::Fatal { fatal }), None] => Err(ClassicRejectionPostCore::new(
            [Some(ClassicGroupEffect::Fatal { fatal }), None],
            MachineState,
        )),
        other => Err(ClassicRejectionPostCore::new(other, EffectShape)),
    }
}

fn install_rejoin(
    entry: &mut GroupConsumerEntry,
    schedule: ClassicRejoinSchedule,
    coordinator: ClassicCoordinatorRecovery,
) -> Result<(), ClassicRejectionInstallFailure> {
    if !waiting_state_matches(entry, schedule) {
        return Err(MachineState);
    }
    let prepared_rediscovery = match coordinator {
        ClassicCoordinatorRecovery::Retain => None,
        ClassicCoordinatorRecovery::Rediscover => Some(
            entry
                .rediscovery
                .prepare_rediscovery_install()
                .map_err(|_error| RediscoveryState)?,
        ),
    };
    let prepared_rejoin = entry
        .rejoin
        .prepare_rejoin_install(schedule)
        .map_err(|_error| RejoinState)?;
    prepared_rejoin.commit();
    if let Some(prepared) = prepared_rediscovery {
        prepared.commit();
    }
    Ok(())
}

fn waiting_state_matches(entry: &GroupConsumerEntry, schedule: ClassicRejoinSchedule) -> bool {
    entry.classic.machine().phase() == ClassicGroupPhase::WaitingToRejoin
        && entry.classic.machine().pending_rejoin() == Some(schedule)
        && entry.classic.machine().fatal().is_none()
        && entry.rejoin.is_dormant()
        && !entry.rediscovery.blocks_join()
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

use ClassicRejectionInstallFailure::{EffectShape, MachineState, RediscoveryState, RejoinState};
