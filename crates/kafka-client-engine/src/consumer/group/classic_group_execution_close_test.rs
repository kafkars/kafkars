//! Local prepared-cycle close and mechanism-release scenarios.

use std::{sync::Arc, time::Duration};

use kafka_client_core::{ClassicGroupPhase, ClassicGroupTiming, ClassicHeartbeatPolicy, GroupId};

use crate::clock::MonotonicClock;

use super::{
    classic_group_execution::new_classic_group_execution,
    classic_group_execution_close::ClassicGroupCloseProgress,
    classic_group_owner::ClassicGroupOwner, session_catalog::GroupSessionCatalog,
};

#[test]
fn local_prepared_join_can_close_without_transport_or_lost_effects() {
    let group_id = GroupId::try_from_raw(1).unwrap_or_else(|| panic!("nonzero group identity"));
    let timing = ClassicGroupTiming::try_new(12_345, 54_321)
        .unwrap_or_else(|error| panic!("valid classic group timing: {error}"));
    let heartbeat = ClassicHeartbeatPolicy::try_new(1_000_000_000, 2_000_000_000)
        .unwrap_or_else(|error| panic!("valid heartbeat policy: {error}"));
    let mut owner = ClassicGroupOwner::new(group_id, timing, heartbeat);
    let mut catalog =
        GroupSessionCatalog::try_new(group_id, Arc::from("workers"), &[Arc::from("orders")])
            .unwrap_or_else(|error| panic!("catalog failed: {error:?}"));
    let mut execution = new_classic_group_execution();
    let capture = MonotonicClock::new()
        .capture_deadline_after(Duration::from_secs(2))
        .unwrap_or_else(|error| panic!("deadline capture failed: {error}"));
    execution
        .begin(&mut owner, capture)
        .unwrap_or_else(|error| panic!("begin failed: {error:?}"));

    assert_eq!(
        execution.close_if_local(&mut owner, &mut catalog),
        Ok(ClassicGroupCloseProgress::Progress)
    );
    assert_eq!(owner.machine().phase(), ClassicGroupPhase::Closed);
    assert_eq!(execution.unsettled(), 0);
    assert!(catalog.live_assignment().is_none());
}
