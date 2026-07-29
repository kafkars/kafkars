//! Post-driver recovery for the explicit voter-addition owner.

use super::super::super::{EngineHostError, EngineHostResources};

pub(super) fn recover(
    resources: &EngineHostResources,
    mut failure: EngineHostError,
) -> EngineHostError {
    let mut add_raft_voter = resources.add_raft_voter.terminal_host();
    if let Some(cleanup) = add_raft_voter
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::AddRaftVoter)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(add_raft_voter);
    failure
}
