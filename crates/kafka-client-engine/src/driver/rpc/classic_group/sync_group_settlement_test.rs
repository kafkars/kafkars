//! Sync settlement owner transfer scenarios.

use kafka_driver::ApiVersion;
use kafka_wire::SyncGroupResponse;

use super::{
    sync_group_settlement::SettledSyncGroupCall, sync_group_terminal::retain_sync_group_terminal,
    sync_group_terminal_test::key,
};

#[test]
fn settled_owner_splits_and_restores_the_exact_terminal() {
    let key = key(1);
    let terminal = retain_sync_group_terminal(
        key,
        Some(ApiVersion::new(2)),
        Ok(SyncGroupResponse::default()),
    );
    let settled = SettledSyncGroupCall::new(terminal, None);

    let (terminal, pending) = settled.into_parts();

    assert_eq!(terminal.key(), key);
    assert_eq!(pending.key(), key);
    let restored = pending.into_settled(terminal);
    assert_eq!(restored.key(), key);
}
