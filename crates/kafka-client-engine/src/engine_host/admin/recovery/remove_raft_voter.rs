//! Post-driver recovery for the explicit voter-removal owner.

use super::super::super::{EngineHostError, EngineHostResources};

pub(super) fn recover(
    resources: &EngineHostResources,
    mut failure: EngineHostError,
) -> EngineHostError {
    let mut remove_raft_voter = resources.remove_raft_voter.terminal_host();
    if let Some(cleanup) = remove_raft_voter
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::RemoveRaftVoter)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(remove_raft_voter);
    failure
}
