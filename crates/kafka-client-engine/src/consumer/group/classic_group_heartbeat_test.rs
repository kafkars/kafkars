//! Linear classic Heartbeat execution-owner state scenarios.

use std::sync::Arc;

use kafka_client_core::GroupId;

use super::{
    classic_group_heartbeat::{ClassicHeartbeatExecution, ClassicHeartbeatExecutionState},
    classic_group_owner::ClassicGroupOwner,
    classic_group_test_support,
    session_catalog::GroupSessionCatalog,
};

#[test]
fn dormant_heartbeat_owner_has_no_deadline_or_obligation() {
    let heartbeat = ClassicHeartbeatExecution::new();

    assert!(heartbeat.is_dormant());
    assert_eq!(heartbeat.next_deadline(), None);
    assert_eq!(heartbeat.unsettled(), 0);
}

#[test]
fn waiting_owner_arms_the_earliest_cadence_or_liveness_deadline() {
    let group_id =
        GroupId::try_from_raw(1).unwrap_or_else(|| panic!("nonzero group identity expected"));
    let mut catalog =
        GroupSessionCatalog::try_new(group_id, Arc::from("workers"), &[Arc::from("orders")])
            .unwrap_or_else(|error| panic!("catalog setup failed: {error:?}"));
    let mut owner = ClassicGroupOwner::new(
        group_id,
        classic_group_test_support::timing(),
        classic_group_test_support::heartbeat_policy(),
    );
    let schedule =
        classic_group_test_support::install_follower(&mut catalog, &mut owner, "member", 7, vec![]);
    let mut heartbeat = ClassicHeartbeatExecution::new();
    heartbeat.set(ClassicHeartbeatExecutionState::Waiting(schedule));

    assert_eq!(heartbeat.next_deadline(), Some(schedule.next_deadline()));
    assert_eq!(heartbeat.unsettled(), 1);
}
