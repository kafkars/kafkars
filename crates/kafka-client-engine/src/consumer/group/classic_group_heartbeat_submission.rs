//! One bounded classic Heartbeat handoff into the existing driver reactor.

use std::sync::Arc;

use kafka_client_core::{ClassicGroupInput, Moment};

use crate::driver::{
    DriverOwner,
    classic_group::{ClassicHeartbeatAdmissionFailure, ClassicHeartbeatCallReservationError},
};

use super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_heartbeat::{
        ClassicHeartbeatAcceptanceFailure, ClassicHeartbeatDriverOwner,
        ClassicHeartbeatExecutionState, PreparedClassicHeartbeat,
    },
    classic_group_heartbeat_rejection::install_heartbeat_effects,
    registry::GroupConsumerRegistry,
    registry_entry::GroupConsumerEntry,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClassicHeartbeatSubmissionTurn {
    Idle,
    Progress,
    Blocked,
}

impl GroupConsumerRegistry {
    pub(super) fn submit_one_classic_heartbeat(
        &mut self,
        now: Moment,
        driver: &DriverOwner,
    ) -> Result<ClassicHeartbeatSubmissionTurn, ClassicGroupExecutionError> {
        let Some(index) = self
            .entries
            .iter()
            .position(|entry| entry.is_active() && entry.heartbeat.prepared().is_some())
        else {
            return Ok(ClassicHeartbeatSubmissionTurn::Idle);
        };
        let key = self.entries[index]
            .heartbeat
            .prepared()
            .ok_or(ClassicGroupExecutionError::HeartbeatState)?
            .key();
        let group = Arc::clone(self.entries[index].catalog.group());
        let calls = self
            .heartbeat_calls
            .as_mut()
            .ok_or(ClassicGroupExecutionError::CallRegistryUnavailable)?;
        let permit = match calls.try_reserve_classic_heartbeat(key, &group) {
            Ok(permit) => permit,
            Err(ClassicHeartbeatCallReservationError::Capacity { .. }) => {
                return Ok(ClassicHeartbeatSubmissionTurn::Blocked);
            }
            Err(ClassicHeartbeatCallReservationError::Duplicate { .. }) => {
                return Err(ClassicGroupExecutionError::CallIdentityMismatch);
            }
        };
        let entry = &mut self.entries[index];
        let prepared = begin_handoff(entry)?;
        let (prepared_key, request) = prepared.into_parts();
        match permit.submit(driver, request) {
            Ok(accepted) => {
                confirm_driver_owned(entry, prepared_key, accepted)?;
                Ok(ClassicHeartbeatSubmissionTurn::Progress)
            }
            Err(failure) => {
                settle_admission_failure(entry, prepared_key, failure, now)?;
                Ok(ClassicHeartbeatSubmissionTurn::Progress)
            }
        }
    }
}

pub(super) fn confirm_driver_owned(
    entry: &mut GroupConsumerEntry,
    expected: crate::driver::classic_group::ClassicHeartbeatCallKey,
    accepted: crate::driver::classic_group::AcceptedClassicHeartbeatCall,
) -> Result<(), ClassicGroupExecutionError> {
    let handoff_matches = matches!(
        entry.heartbeat.state(),
        ClassicHeartbeatExecutionState::Handoff(key) if *key == expected
    );
    if !handoff_matches {
        entry.fault = Some(ClassicGroupEntryFault::HeartbeatAcceptance(
            ClassicHeartbeatAcceptanceFailure::new(expected, accepted),
        ));
        return Err(ClassicGroupExecutionError::HeartbeatState);
    }
    if accepted.key() != expected {
        entry.heartbeat.set(ClassicHeartbeatExecutionState::Dormant);
        entry.fault = Some(ClassicGroupEntryFault::HeartbeatAcceptance(
            ClassicHeartbeatAcceptanceFailure::new(expected, accepted),
        ));
        return Err(ClassicGroupExecutionError::CallIdentityMismatch);
    }
    entry
        .heartbeat
        .set(ClassicHeartbeatExecutionState::DriverOwned(
            ClassicHeartbeatDriverOwner::new(accepted),
        ));
    Ok(())
}

fn begin_handoff(
    entry: &mut GroupConsumerEntry,
) -> Result<PreparedClassicHeartbeat, ClassicGroupExecutionError> {
    let state = entry
        .heartbeat
        .replace(ClassicHeartbeatExecutionState::Dormant);
    let ClassicHeartbeatExecutionState::Prepared(prepared) = state else {
        entry.heartbeat.set(state);
        return Err(ClassicGroupExecutionError::HeartbeatState);
    };
    let key = prepared.key();
    entry
        .heartbeat
        .set(ClassicHeartbeatExecutionState::Handoff(key));
    Ok(prepared)
}

fn settle_admission_failure(
    entry: &mut GroupConsumerEntry,
    key: crate::driver::classic_group::ClassicHeartbeatCallKey,
    failure: ClassicHeartbeatAdmissionFailure,
    now: Moment,
) -> Result<(), ClassicGroupExecutionError> {
    if !matches!(
        entry.heartbeat.state(),
        ClassicHeartbeatExecutionState::Handoff(expected) if *expected == key
    ) {
        entry.fault = Some(ClassicGroupEntryFault::HeartbeatAdmission(failure));
        return Err(ClassicGroupExecutionError::HeartbeatState);
    }
    let transition = match entry.classic.apply(ClassicGroupInput::HeartbeatFailed {
        attempt: key.attempt(),
        now,
    }) {
        Ok(transition) => transition,
        Err(error) => {
            entry.fault = Some(ClassicGroupEntryFault::HeartbeatAdmission(failure));
            return Err(ClassicGroupExecutionError::Core(error.kind()));
        }
    };
    let mut effects = transition.into_effects();
    if let Err(rejection) = install_heartbeat_effects(entry, [effects.next(), effects.next()], now)
    {
        entry.fault = Some(ClassicGroupEntryFault::HeartbeatAdmissionPostCore(
            rejection, failure,
        ));
        return Err(ClassicGroupExecutionError::RejoinPostCore);
    }
    entry.heartbeat.set(ClassicHeartbeatExecutionState::Dormant);
    drop(failure);
    Ok(())
}
