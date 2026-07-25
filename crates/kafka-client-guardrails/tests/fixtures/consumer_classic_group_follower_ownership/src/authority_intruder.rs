//! Foreign construction and mutation of follower authorities forbidden by this fixture.

use crate::authority_owner::{
    ClassicGroupJoinAcceptanceFailure, ClassicGroupJoinCallOwner,
    ClassicGroupSyncAcceptanceFailure, ClassicGroupSyncDriverOwner, PreparedClassicGroupSync,
    SyncInterpretationFailure,
};

fn intrude() {
    let mut call = ClassicGroupJoinCallOwner {
        integration_for_join_call: 1,
        tracking_for_join_call: 2,
        accepted_join_call_receipt: 3,
    };
    call.accepted_join_call_receipt = 4;
    let mut join_failure = ClassicGroupJoinAcceptanceFailure {
        rejected_join_acceptance: 5,
        unrestored_join_receipt: 6,
    };
    join_failure.rejected_join_acceptance = 7;
    let mut prepared = PreparedClassicGroupSync {
        prepared_sync_identity: 8,
        pending_sync_request: 9,
    };
    prepared.pending_sync_request = 10;
    let mut sync = ClassicGroupSyncDriverOwner {
        driver_sync_identity: 11,
        accepted_sync_receipt: 12,
    };
    sync.accepted_sync_receipt = 13;
    let mut sync_failure = ClassicGroupSyncAcceptanceFailure {
        rejected_sync_identity: 14,
        unrestored_sync_receipt: 15,
    };
    sync_failure.rejected_sync_identity = 16;
    let mut interpretation = SyncInterpretationFailure {
        sync_failure_kind: 17,
        restorable_sync_terminal: 18,
    };
    interpretation.restorable_sync_terminal = 19;
}
