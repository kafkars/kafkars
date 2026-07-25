//! Classic Heartbeat settlement owner transfer scenarios.

use kafka_driver::ApiVersion;
use kafka_wire::HeartbeatResponse;

use super::{
    heartbeat_settlement::SettledClassicHeartbeatCall,
    heartbeat_terminal::retain_classic_heartbeat_terminal, heartbeat_terminal_test::key,
};

#[test]
fn settled_owner_splits_and_restores_the_exact_terminal() {
    let key = key(1);
    let terminal = retain_classic_heartbeat_terminal(
        key,
        Some(ApiVersion::new(2)),
        Ok(HeartbeatResponse::default()),
    );
    let settled = SettledClassicHeartbeatCall::new(terminal, None);

    let (terminal, pending) = settled.into_parts();

    assert_eq!(terminal.key(), key);
    assert_eq!(pending.key(), key);
    let restored = pending.into_settled(terminal);
    assert_eq!(restored.key(), key);
}
