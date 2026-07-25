//! Dormant classic membership ownership evidence.

use crate::GroupId;

use super::{ClassicGroupMachine, ClassicGroupPhase, ClassicGroupTiming, ClassicHeartbeatPolicy};

#[test]
fn construction_is_dormant_timeless_and_assignment_free() {
    let group_id = GroupId::try_from_raw(7).unwrap_or_else(|| panic!("nonzero group"));
    let timing = ClassicGroupTiming::try_new(10_000, 30_000)
        .unwrap_or_else(|error| panic!("valid timing: {error}"));
    let heartbeat = ClassicHeartbeatPolicy::try_new(10, 20)
        .unwrap_or_else(|error| panic!("valid heartbeat policy: {error}"));
    let machine = ClassicGroupMachine::new(group_id, timing, heartbeat);

    assert_eq!(machine.group_id(), group_id);
    assert_eq!(machine.timing(), timing);
    assert_eq!(machine.phase(), ClassicGroupPhase::Dormant);
    assert_eq!(machine.active_cycle(), None);
    assert_eq!(machine.deadline(), None);
    assert_eq!(machine.live_assignment(), None);
    assert_eq!(machine.live_generation(), None);
}
