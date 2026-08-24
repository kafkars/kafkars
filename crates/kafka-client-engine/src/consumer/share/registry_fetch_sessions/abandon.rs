//! Exact release of obsolete or faulted broker-local share-fetch sessions.

use super::super::{
    entry::ShareConsumerEntry, fetch_session_set::ShareFetchSessionSetTurn,
    registry_fetch_routing::current_generation, registry_membership::ShareMembershipHostError,
};
use super::ShareFetchSessionsHostTurn;

pub(super) fn abandon_sessions(
    entry: &mut ShareConsumerEntry,
    clear_fault: bool,
) -> Result<ShareFetchSessionsHostTurn, ShareMembershipHostError> {
    let turn = entry
        .fetch_mut()
        .sessions_mut()
        .ok_or(ShareMembershipHostError::EffectShape)?
        .abandon_turn()
        .map_err(|_error| ShareMembershipHostError::EffectShape)?;
    match turn {
        ShareFetchSessionSetTurn::Progress => Ok(ShareFetchSessionsHostTurn::Progress),
        ShareFetchSessionSetTurn::Blocked => Ok(ShareFetchSessionsHostTurn::Blocked),
        ShareFetchSessionSetTurn::Idle | ShareFetchSessionSetTurn::NeedsPreparation(_) => {
            Err(ShareMembershipHostError::EffectShape)
        }
        ShareFetchSessionSetTurn::RecoveryReady => Err(ShareMembershipHostError::EffectShape),
        ShareFetchSessionSetTurn::Released => {
            let sessions = entry
                .fetch_mut()
                .take_sessions()
                .ok_or(ShareMembershipHostError::EffectShape)?;
            sessions
                .release_unsubmitted()
                .map_err(|_error| ShareMembershipHostError::EffectShape)?;
            if clear_fault {
                entry.fetch_mut().clear_session_fault();
            }
            Ok(ShareFetchSessionsHostTurn::Progress)
        }
    }
}

pub(super) fn fetch_sessions_have_work(entry: &ShareConsumerEntry) -> bool {
    let generation = current_generation(entry);
    if entry.fetch().sessions().is_some() {
        return true;
    }
    if let Some(fault) = entry.fetch().session_fault() {
        return entry.has_close() || generation != Some(fault.generation());
    }
    entry.fetch().routed().is_some()
}
