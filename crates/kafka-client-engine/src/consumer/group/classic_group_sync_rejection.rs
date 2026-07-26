//! Exact Sync broker fact application before raw-terminal disposition.

use kafka_client_core::{ClassicGroupInput, MembershipCycle, Moment};

use crate::protocol::consumer::ClassicBrokerRejection;

use super::{
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_rejection_fault::ClassicRejectionPostCore,
    classic_group_rejection_install::{exact_broker_error, install_stage_rejection},
    registry_entry::GroupConsumerEntry,
};

pub(super) enum ClassicSyncRejectionFailure {
    Restorable(ClassicGroupExecutionError),
    PostCore(ClassicRejectionPostCore),
}

#[expect(
    clippy::result_large_err,
    reason = "the error retains exact post-core effects without allocating or erasing recovery state"
)]
pub(super) fn apply_sync_rejection(
    entry: &mut GroupConsumerEntry,
    cycle: MembershipCycle,
    now: Moment,
    rejection: ClassicBrokerRejection,
) -> Result<(), ClassicSyncRejectionFailure> {
    let error = exact_broker_error(rejection).ok_or(ClassicSyncRejectionFailure::Restorable(
        ClassicGroupExecutionError::SyncTerminal,
    ))?;
    let transition = entry
        .classic
        .apply(ClassicGroupInput::SyncRejected { cycle, now, error })
        .map_err(|error| {
            ClassicSyncRejectionFailure::Restorable(ClassicGroupExecutionError::Core(error.kind()))
        })?;
    install_stage_rejection(entry, transition).map_err(ClassicSyncRejectionFailure::PostCore)
}
